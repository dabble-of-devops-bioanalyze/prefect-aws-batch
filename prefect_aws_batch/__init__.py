"""Main module.

Functions grabbed from here: https://github.com/awslabs/aws-batch-helpers/blob/master/gpu-example/submit-job.py
"""

import copy
import sys
import time
from datetime import datetime
from typing import List, Dict, Any, Optional

from mypy_boto3_batch.client import BatchClient
from mypy_boto3_batch.literals import JobStatusType
from mypy_boto3_batch.type_defs import DescribeJobDefinitionsResponseTypeDef
from mypy_boto3_batch.type_defs import DescribeJobsResponseTypeDef
from mypy_boto3_batch.type_defs import JobDefinitionTypeDef
from mypy_boto3_batch.type_defs import RegisterJobDefinitionRequestRequestTypeDef
from mypy_boto3_batch.type_defs import RegisterJobDefinitionResponseTypeDef
from mypy_boto3_batch.type_defs import SubmitJobRequestRequestTypeDef
from mypy_boto3_batch.type_defs import SubmitJobResponseTypeDef
from mypy_boto3_logs.client import CloudWatchLogsClient
from mypy_boto3_logs.type_defs import GetLogEventsResponseTypeDef
from mypy_boto3_logs.type_defs import OutputLogEventTypeDef
from prefect import flow, task, unmapped


def get_human_readable_time(last_time_stamp):
    return datetime.fromtimestamp(
        last_time_stamp / 1000.0).isoformat()


def find_or_create_job_definition(
    client: BatchClient,
    job_definition_name: str,
    job_definition: RegisterJobDefinitionRequestRequestTypeDef
) -> JobDefinitionTypeDef:
    assert job_definition_name, 'job_definition_name required'

    job_definitions_response: DescribeJobDefinitionsResponseTypeDef = client.describe_job_definitions(
        jobDefinitionName=job_definition_name
    )
    # job_definitions_response
    job_definitions: List[JobDefinitionTypeDef] = job_definitions_response.get(
        'jobDefinitions')

    if job_definitions[0]:
        job_definition_copy = copy.deepcopy(job_definitions[0])
        del job_definition_copy['jobDefinitionArn']
        del job_definition_copy['revision']
        del job_definition_copy['status']
        if not job_definition_copy == job_definition:
            register_job_definition_response: RegisterJobDefinitionResponseTypeDef = client.register_job_definition(
                **job_definition)
        else:
            return job_definitions[0]
    else:
        register_job_definition_response: RegisterJobDefinitionResponseTypeDef = client.register_job_definition(
            **job_definition
        )

    job_definitions_response: DescribeJobDefinitionsResponseTypeDef = client.describe_job_definitions(
        jobDefinitionName=register_job_definition_response['jobDefinitionName']
    )
    job_definition = job_definitions_response['jobDefinitions'][0]

    return job_definition


@task
def find_or_create_job_definition_task(
    client: BatchClient,
    job_definition_name: str,
    job_definition: RegisterJobDefinitionRequestRequestTypeDef
) -> JobDefinitionTypeDef:
    return find_or_create_job_definition(client, job_definition_name, job_definition)


def get_log_stream_name(
    job_response: DescribeJobsResponseTypeDef
) -> str:
    try:
        log_stream_name = job_response['jobs'][0]['container']['logStreamName']
    except Exception as e:
        log_stream_name = None
        print("Exception occurred")
        print(job_response['jobs'])

    return log_stream_name


def get_log_events(
    client: CloudWatchLogsClient,
    log_group,
    log_stream_name,
    start_time=0,
    skip=0,
    start_from_head=True
) -> List[OutputLogEventTypeDef]:
    """
    A generator for log items in a single stream. This will yield all the
    items that are available at the current moment.

    Completely stole this from here
    https://airflow.apache.org/docs/apache-airflow/1.10.5/_modules/airflow/contrib/hooks/aws_logs_hook.html

    :param client: boto3.client('cloudwatch')
    :param log_group: The name of the log group.
    :type log_group: str
    :param log_stream_name: The name of the specific stream.
    :type log_stream_name: str
    :param start_time: The time stamp value to start reading the logs from (default: 0).
    :type start_time: int
    :param skip: The number of log entries to skip at the start (default: 0).
        This is for when there are multiple entries at the same timestamp.
    :type skip: int
    :param start_from_head: whether to start from the beginning (True) of the log or
        at the end of the log (False).
    :type start_from_head: bool
    :rtype: dict
    :return: | A CloudWatch log event with the following key-value pairs:
             |   'timestamp' (int): The time in milliseconds of the event.
             |   'message' (str): The log event data.
             |   'ingestionTime' (int): The time in milliseconds the event was ingested.
    """

    next_token = None

    event_count = 1
    while event_count > 0:
        if next_token is not None:
            token_arg = {'nextToken': next_token}
        else:
            token_arg = {}

        response: GetLogEventsResponseTypeDef = client.get_log_events(
            logGroupName=log_group,
            logStreamName=log_stream_name,
            startTime=start_time,
            startFromHead=start_from_head,
            **token_arg
        )

        events = response['events']
        event_count = len(events)

        if event_count > skip:
            events = events[skip:]
            skip = 0
        else:
            skip = skip - event_count
            events = []

        for ev in events:
            yield ev

        if 'nextForwardToken' in response:
            next_token = response['nextForwardToken']
        else:
            return


def print_logs(
    client: CloudWatchLogsClient,
    log_stream_name: str,
    start_time: int = 0
) -> int:
    log_events = get_log_events(
        client, log_group='/aws/batch/job',
        log_stream_name=log_stream_name,
        start_time=start_time
    )

    last_time_stamp = start_time
    for log_event in log_events:
        last_time_stamp = log_event['timestamp']
        human_timestamp = get_human_readable_time(last_time_stamp)
        message = log_event['message']
        print(f'[{human_timestamp}] {message}')

    if last_time_stamp > 0:
        last_time_stamp = last_time_stamp + 1

    return last_time_stamp


def watch_batch_job(
    batch_client: BatchClient,
    log_client: CloudWatchLogsClient,
    job_response: DescribeJobsResponseTypeDef
) -> JobStatusType:
    """Watch an AWS Batch job and print out the logs

    Shoutout to aws labs:
    https://github.com/awslabs/aws-batch-helpers/blob/master/gpu-example/submit-job.py

    Args:
        batch_client (BatchClient): boto3.client('batch')
        log_client (CloudWatchLogsClient): boto3.client('logs')
        job_response (DescribeJobsResponseTypeDef): batch_client.describe_jobs(jobs=[jobId])

    Returns:
        JobStatusType: AWS Batch Job Status
    """
    spinner = 0
    running = False
    start_time = 0
    wait = True
    spin = ['-', '/', '|', '\\', '-', '/', '|', '\\']
    job_id = job_response['jobs'][0]['jobId']
    job_name = job_response['jobs'][0]['jobName']
    log_stream_name: Any = None
    line = '=' * 80
    status = 'RUNNABLE'

    while wait:
        time.sleep(1)
        describe_jobs_response: DescribeJobsResponseTypeDef = batch_client.describe_jobs(
            jobs=[job_id]
        )
        status: JobStatusType = describe_jobs_response['jobs'][0]['status']

        if status == 'SUCCEEDED' or status == 'FAILED':
            log_stream_name = get_log_stream_name(
                job_response=describe_jobs_response
            )

            if not running and log_stream_name:
                running = False
                print(f'Job [{job_name} - {job_id}] is COMPLETE with status: {status}')
                print(f'Logs for log stream: {log_stream_name}:')

            if log_stream_name:
                start_time = print_logs(
                    client=log_client,
                    log_stream_name=log_stream_name,
                    start_time=start_time
                )

            print(f'{line}\nJob [{job_name} - {job_id}] {status}')

            break

        elif status == 'RUNNING':

            log_stream_name = get_log_stream_name(
                job_response=describe_jobs_response)

            if not running and log_stream_name:
                running = True
                print(f'Job [{job_name} - {job_id}] is RUNNING')
                print(f'Polling cloudwatch logs...')
                print(f'Output for logstream: {log_stream_name}:\n{line}')
            if log_stream_name:
                start_time = print_logs(
                    client=log_client,
                    log_stream_name=log_stream_name,
                    start_time=start_time
                )

        else:
            this_spin = spin[spinner % len(spin)]
            print(f'Job [{job_name} - {job_id}] is: {status}... {this_spin}')
            sys.stdout.flush()
            time.sleep(30)
            spinner += 1

    return status


@task(retries=5, retry_delay_seconds=1)
def watch_batch_job_task(
    batch_client: BatchClient,
    log_client: CloudWatchLogsClient,
    job_response: DescribeJobsResponseTypeDef
) -> JobStatusType:
    """Watch an AWS Batch job and print out the logs

    Shoutout to aws labs:
    https://github.com/awslabs/aws-batch-helpers/blob/master/gpu-example/submit-job.py

    Args:
        batch_client (BatchClient): boto3.client('batch')
        log_client (CloudWatchLogsClient): boto3.client('logs')
        job_response (DescribeJobsResponseTypeDef): batch_client.describe_jobs(jobs=[jobId])

    Returns:
        JobStatusType: AWS Batch Job Status
    """
    return watch_batch_job(batch_client=batch_client, log_client=log_client, job_response=job_response)


def submit_batch_job(
    batch_client: BatchClient,
    job_data: SubmitJobRequestRequestTypeDef
) -> Dict[str, Any]:
    """Submit job to AWS Batch and wait for it

    Args:
        batch_client (BatchClient): boto3.client('batch')
        job_data (SubmitJobRequestRequestTypeDef): Submit object to AWS Batch
    """
    try:
        response: SubmitJobResponseTypeDef = batch_client.submit_job(**job_data)
    except Exception as e:
        print("Exception occurred submitting job")
        raise Exception(e)

    jobId = response['jobId']
    job_response: DescribeJobsResponseTypeDef = batch_client.describe_jobs(
        jobs=[
            jobId
        ]
    )
    return dict(job_id=jobId, job_response=job_response)


@task
def submit_batch_job_task(
    batch_client: BatchClient,
    job_data: SubmitJobRequestRequestTypeDef
) -> Dict[str, Any]:
    """Submit job to AWS Batch and wait for it

    Args:
        batch_client (BatchClient): boto3.client('batch')
        job_data (SubmitJobRequestRequestTypeDef): Submit object to AWS Batch
    """
    return submit_batch_job(batch_client=batch_client, job_data=job_data)


@flow
def submit_batch_jobs(
    batch_client: Any,
    batch_jobs: List[Dict[str, Any]],
    log_client: Optional[Any],
    watch=True,
) -> List[SubmitJobResponseTypeDef]:
    if watch and not log_client:
        raise Exception(f"If watch=True you must specify the log_client")

    submit_job_responses = submit_batch_job_task.map(
        batch_client=unmapped(batch_client), job_data=batch_jobs
    )
    # submit_job_responses = submit_job_responses.result()

    # for job_data, submit_job_response in zip(batch_jobs, submit_job_responses):
    #     job_queue = job_data['jobQueue']
    #     list_job_response = batch_client.list_jobs(
    #         jobQueue=job_queue,
    #         filters=[
    #             {
    #                 'name': 'JOB_NAME',
    #                 'values': [
    #                     job_data['jobName'],
    #                 ]
    #             },
    #         ]
    #     )
    #     list_job_responses.append(list_job_response)

    # watch_jobs_data = list(map(lambda x: x[1], submit_job_responses))
    # if watch:
    #     statuses = watch_batch_job_task.map(
    #         batch_client=unmapped(batch_client),
    #         log_client=unmapped(log_client),
    #         job_response=watch_jobs_data
    #     )

    return list_job_responses
