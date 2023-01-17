"""Console script for prefect_aws_batch."""
import sys
import click


@click.command()
def main(args=None):
    """Console script for prefect_aws_batch."""
    click.echo("Replace this message by putting your code into "
               "prefect_aws_batch.cli.main")
    click.echo("See click documentation at https://click.palletsprojects.com/")
    return 0


if __name__ == "__main__":
    sys.exit(main())  # pragma: no cover
