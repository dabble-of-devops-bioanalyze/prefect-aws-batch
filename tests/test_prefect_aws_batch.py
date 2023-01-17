#!/usr/bin/env python

"""Tests for `prefect_aws_batch` package."""


import unittest
from click.testing import CliRunner

from prefect_aws_batch import prefect_aws_batch
from prefect_aws_batch import cli


class TestPrefect_aws_batch(unittest.TestCase):
    """Tests for `prefect_aws_batch` package."""

    def setUp(self):
        """Set up test fixtures, if any."""

    def tearDown(self):
        """Tear down test fixtures, if any."""

    def test_000_something(self):
        """Test something."""

    def test_command_line_interface(self):
        """Test the CLI."""
        runner = CliRunner()
        result = runner.invoke(cli.main)
        assert result.exit_code == 0
        assert 'prefect_aws_batch.cli.main' in result.output
        help_result = runner.invoke(cli.main, ['--help'])
        assert help_result.exit_code == 0
        assert '--help  Show this message and exit.' in help_result.output
