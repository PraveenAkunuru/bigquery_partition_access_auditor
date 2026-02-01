import pytest
from unittest.mock import MagicMock, patch
from bq_partition_audit import main

def test_cli_parsing_basic():
    test_args = ["prog", "--project", "p", "--table", "p.d.t"]
    with patch("sys.argv", test_args), \
         patch("bq_partition_audit.bigquery.Client"), \
         patch("bq_partition_audit.run_audit") as mock_run:
        main()
        mock_run.assert_called_once()
        config = mock_run.call_args[0][0]
        assert config.audit_project == "p"
        assert config.target_table_ref == "p.d.t"

def test_cli_verbose_flag():
    test_args = ["prog", "--project", "p", "--table", "p.d.t", "--verbose"]
    with patch("sys.argv", test_args), \
         patch("bq_partition_audit.bigquery.Client"), \
         patch("bq_partition_audit.setup_logging") as mock_setup, \
         patch("bq_partition_audit.run_audit"):
        main()
        mock_setup.assert_called_with("DEBUG")

def test_cli_missing_args():
    test_args = ["prog", "--project", "p"]
    with patch("sys.argv", test_args), pytest.raises(SystemExit):
        main()
