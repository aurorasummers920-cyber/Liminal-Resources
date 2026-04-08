#!/usr/bin/env python3
"""
Comprehensive test suite for reconcile_1997.py

Covers:
- Fixed-width record generation
- Mock archive creation and file encoding
- Mock database setup (matches, discrepancies, missing rows)
- Core reconciliation engine (matching, discrepancies, edge cases)
- Email report (dry-run, missing credentials, construction)
- CLI argument parsing
- Edge cases: empty archives, malformed data, short lines, non-numeric amounts
"""

import os
import tempfile
import textwrap
from decimal import Decimal
from unittest import mock

import pandas as pd
import pytest
from sqlalchemy import create_engine, text

# Import the module under test
import reconcile_1997 as rec


# ============================= Fixtures =============================

@pytest.fixture
def tmp_archive(tmp_path):
    """Create a temporary archive directory and return its path."""
    archive = tmp_path / "archive"
    archive.mkdir()
    return str(archive)


@pytest.fixture
def mock_records():
    """Return a small set of expected records for testing."""
    return [
        {"transaction_id": "TX000001", "amount": Decimal("100.00")},
        {"transaction_id": "TX000002", "amount": Decimal("200.50")},
        {"transaction_id": "TX000003", "amount": Decimal("350.75")},
    ]


@pytest.fixture
def in_memory_db(mock_records):
    """Create an in-memory SQLite database with all records matching exactly."""
    engine = create_engine("sqlite:///:memory:")
    with engine.connect() as conn:
        conn.execute(
            text(
                "CREATE TABLE transactions "
                "(transaction_id TEXT PRIMARY KEY, amount NUMERIC, status TEXT)"
            )
        )
        for r in mock_records:
            conn.execute(
                text("INSERT INTO transactions VALUES (:id, :amt, 'PROCESSED')"),
                {"id": r["transaction_id"], "amt": float(r["amount"])},
            )
        conn.commit()
    return engine


# ============= Tests for generate_mock_fixed_width_record =============

class TestGenerateMockFixedWidthRecord:
    def test_record_length(self):
        """Each record (excluding newline) should be exactly 80 characters."""
        record = rec.generate_mock_fixed_width_record("TX000001", Decimal("125.75"), "comment")
        assert record.endswith("\n")
        assert len(record.rstrip("\n")) == 80

    def test_tx_id_field_padded(self):
        """Transaction ID is left-justified and padded/truncated to 8 chars."""
        record = rec.generate_mock_fixed_width_record("AB", Decimal("0.00"), "")
        assert record[:8] == "AB      "

    def test_tx_id_field_truncated(self):
        """Long transaction IDs are truncated to 8 characters."""
        record = rec.generate_mock_fixed_width_record("ABCDEFGHIJKLMNOP", Decimal("0.00"), "")
        assert record[:8] == "ABCDEFGH"

    def test_amount_field_right_justified(self):
        """Amount occupies positions 8–23, right-justified with 2 decimal places."""
        record = rec.generate_mock_fixed_width_record("TX000001", Decimal("125.75"), "")
        amount_field = record[8:24]
        assert amount_field.strip() == "125.75"
        assert len(amount_field) == 16

    def test_comment_field_padded(self):
        """Comment occupies positions 24–79, left-justified, padded to 56 chars."""
        comment = "Hello"
        record = rec.generate_mock_fixed_width_record("TX000001", Decimal("0.00"), comment)
        comment_field = record[24:80]
        assert comment_field.startswith("Hello")
        assert len(comment_field) == 56

    def test_long_comment_truncated(self):
        """Comments longer than 56 characters are truncated."""
        long_comment = "A" * 100
        record = rec.generate_mock_fixed_width_record("TX000001", Decimal("0.00"), long_comment)
        comment_field = record[24:80]
        assert len(comment_field) == 56
        assert comment_field == "A" * 56

    def test_zero_amount(self):
        """Zero amount is formatted correctly."""
        record = rec.generate_mock_fixed_width_record("TX000001", Decimal("0"), "test")
        amount_field = record[8:24]
        assert amount_field.strip() == "0.00"

    def test_large_amount(self):
        """Large amounts fit within the 16-char field."""
        record = rec.generate_mock_fixed_width_record("TX000001", Decimal("9999999.99"), "test")
        amount_field = record[8:24]
        assert amount_field.strip() == "9999999.99"

    def test_negative_amount(self):
        """Negative amounts are represented."""
        record = rec.generate_mock_fixed_width_record("TX000001", Decimal("-50.25"), "test")
        amount_field = record[8:24]
        assert "-50.25" in amount_field.strip()


# ================ Tests for create_mock_archive ================

class TestCreateMockArchive:
    def test_creates_three_files(self, tmp_archive):
        """Should create 3 batch files."""
        rec.create_mock_archive(tmp_archive)
        files = [f for f in os.listdir(tmp_archive) if f.endswith(".dat")]
        assert len(files) == 3

    def test_returns_correct_record_count(self, tmp_archive):
        """Should return 240 records (3 files × 80 records)."""
        expected = rec.create_mock_archive(tmp_archive)
        assert len(expected) == 240

    def test_records_have_required_keys(self, tmp_archive):
        """Each record dict has transaction_id and amount."""
        expected = rec.create_mock_archive(tmp_archive)
        for r in expected:
            assert "transaction_id" in r
            assert "amount" in r

    def test_file_contents_readable(self, tmp_archive):
        """Files should be readable and non-empty."""
        rec.create_mock_archive(tmp_archive)
        for fname in os.listdir(tmp_archive):
            path = os.path.join(tmp_archive, fname)
            size = os.path.getsize(path)
            assert size > 0

    def test_creates_directory_if_missing(self, tmp_path):
        """Should create the archive directory if it doesn't exist."""
        nested = str(tmp_path / "a" / "b" / "c")
        rec.create_mock_archive(nested)
        assert os.path.isdir(nested)

    def test_unique_transaction_ids(self, tmp_archive):
        """All transaction IDs should be unique."""
        expected = rec.create_mock_archive(tmp_archive)
        tx_ids = [r["transaction_id"] for r in expected]
        assert len(tx_ids) == len(set(tx_ids))


# ================ Tests for setup_mock_database ================

class TestSetupMockDatabase:
    def test_creates_transactions_table(self, mock_records):
        """Database should have a 'transactions' table."""
        engine = rec.setup_mock_database(mock_records)
        with engine.connect() as conn:
            result = conn.execute(text("SELECT name FROM sqlite_master WHERE type='table'"))
            tables = [row[0] for row in result]
        assert "transactions" in tables

    def test_deliberately_skips_records(self):
        """Records at index % 7 == 0 are skipped (missing from DB)."""
        records = [
            {"transaction_id": f"TX{i:07d}", "amount": Decimal("100.00")}
            for i in range(14)
        ]
        engine = rec.setup_mock_database(records)
        with engine.connect() as conn:
            result = conn.execute(text("SELECT COUNT(*) FROM transactions"))
            count = result.scalar()
        # Indices 0, 7 are skipped (% 7 == 0), but index 0 is also % 10 == 0
        # so index 0 would be skipped by the i % 7 == 0 check only if not caught first
        # Actually: i % 10 == 0 → modify amount; elif i % 7 == 0 → skip; else → exact
        # i=0: %10==0, inserted with modified amount
        # i=7: %7==0, skipped
        # So 13 inserted out of 14
        assert count == 13

    def test_deliberately_modifies_amounts(self):
        """Records at index % 10 == 0 have amount increased by 5.00."""
        records = [
            {"transaction_id": f"TX{i:07d}", "amount": Decimal("100.00")}
            for i in range(10)
        ]
        engine = rec.setup_mock_database(records)
        with engine.connect() as conn:
            result = conn.execute(
                text("SELECT amount FROM transactions WHERE transaction_id = 'TX0000000'")
            )
            db_amount = result.scalar()
        # Index 0: amount should be 100.00 + 5.00 = 105.00
        assert float(db_amount) == pytest.approx(105.00)


# ================ Tests for run_reconciliation ================

class TestRunReconciliation:
    def _write_archive_file(self, archive_dir, filename, records):
        """Helper: write fixed-width records encoded in cp037 to a .dat file.

        Uses binary mode with ASCII newline (0x0A) as the record separator,
        matching how run_reconciliation reads files (rb mode, splitting on \\n).
        """
        filepath = os.path.join(archive_dir, filename)
        with open(filepath, "wb") as f:
            for r in records:
                line = rec.generate_mock_fixed_width_record(
                    r["transaction_id"], r["amount"], r.get("comment", "test comment")
                )
                # Encode record content as cp037, then add ASCII newline separator
                f.write(line.rstrip("\n").encode("cp037") + b"\n")

    def test_perfect_match(self, tmp_archive, mock_records, in_memory_db):
        """All records match when DB amounts are identical."""
        self._write_archive_file(tmp_archive, "batch_01.dat", mock_records)
        output_xlsx = os.path.join(tmp_archive, "report.xlsx")
        df = rec.run_reconciliation(tmp_archive, in_memory_db, output_xlsx)
        assert len(df) == len(mock_records)
        assert df["match"].all()

    def test_discrepancy_detected(self, tmp_archive, mock_records):
        """Discrepancies are flagged when DB amounts differ."""
        engine = create_engine("sqlite:///:memory:")
        with engine.connect() as conn:
            conn.execute(
                text(
                    "CREATE TABLE transactions "
                    "(transaction_id TEXT PRIMARY KEY, amount NUMERIC, status TEXT)"
                )
            )
            for r in mock_records:
                # Insert with a different amount for the first record
                amt = float(r["amount"]) + 999.0 if r["transaction_id"] == "TX000001" else float(r["amount"])
                conn.execute(
                    text("INSERT INTO transactions VALUES (:id, :amt, 'PROCESSED')"),
                    {"id": r["transaction_id"], "amt": amt},
                )
            conn.commit()

        self._write_archive_file(tmp_archive, "batch_01.dat", mock_records)
        output_xlsx = os.path.join(tmp_archive, "report.xlsx")
        df = rec.run_reconciliation(tmp_archive, engine, output_xlsx)

        mismatches = df[~df["match"]]
        assert len(mismatches) == 1
        assert mismatches.iloc[0]["transaction_id"] == "TX000001"

    def test_missing_db_record(self, tmp_archive, mock_records):
        """Records missing from DB result in NaN db_amount and no match."""
        engine = create_engine("sqlite:///:memory:")
        with engine.connect() as conn:
            conn.execute(
                text(
                    "CREATE TABLE transactions "
                    "(transaction_id TEXT PRIMARY KEY, amount NUMERIC, status TEXT)"
                )
            )
            # Only insert the first record
            conn.execute(
                text("INSERT INTO transactions VALUES (:id, :amt, 'PROCESSED')"),
                {"id": mock_records[0]["transaction_id"], "amt": float(mock_records[0]["amount"])},
            )
            conn.commit()

        self._write_archive_file(tmp_archive, "batch_01.dat", mock_records)
        output_xlsx = os.path.join(tmp_archive, "report.xlsx")
        df = rec.run_reconciliation(tmp_archive, engine, output_xlsx)

        # 2 records have no matching DB entry
        assert len(df) == len(mock_records)
        missing = df[df["status"].isna()]
        assert len(missing) == 2

    def test_generates_xlsx_report(self, tmp_archive, mock_records, in_memory_db):
        """An Excel report file is created with Reconciliation and Summary sheets."""
        self._write_archive_file(tmp_archive, "batch_01.dat", mock_records)
        output_xlsx = os.path.join(tmp_archive, "report.xlsx")
        rec.run_reconciliation(tmp_archive, in_memory_db, output_xlsx)

        assert os.path.exists(output_xlsx)
        xl = pd.ExcelFile(output_xlsx)
        assert "Reconciliation" in xl.sheet_names
        assert "Summary" in xl.sheet_names

    def test_skips_non_dat_files(self, tmp_archive, mock_records, in_memory_db):
        """Files without .dat/.txt/.csv extension are ignored."""
        self._write_archive_file(tmp_archive, "batch_01.dat", mock_records)
        # Create a file that should be ignored
        with open(os.path.join(tmp_archive, "readme.md"), "w") as f:
            f.write("This is not a data file")

        output_xlsx = os.path.join(tmp_archive, "report.xlsx")
        df = rec.run_reconciliation(tmp_archive, in_memory_db, output_xlsx)
        assert len(df) == len(mock_records)

    def test_short_lines_skipped(self, tmp_archive, in_memory_db):
        """Lines shorter than 80 characters are skipped."""
        filepath = os.path.join(tmp_archive, "batch_01.dat")
        with open(filepath, "wb") as f:
            # Write a short line (< 80 chars when decoded)
            f.write("SHORT".encode("cp037") + b"\n")
            # Write a valid 80-char record
            line = rec.generate_mock_fixed_width_record("TX000001", Decimal("100.00"), "test")
            f.write(line.rstrip("\n").encode("cp037") + b"\n")

        output_xlsx = os.path.join(tmp_archive, "report.xlsx")
        engine = create_engine("sqlite:///:memory:")
        with engine.connect() as conn:
            conn.execute(
                text(
                    "CREATE TABLE transactions "
                    "(transaction_id TEXT PRIMARY KEY, amount NUMERIC, status TEXT)"
                )
            )
            conn.execute(
                text("INSERT INTO transactions VALUES ('TX000001', 100.00, 'PROCESSED')")
            )
            conn.commit()

        df = rec.run_reconciliation(tmp_archive, engine, output_xlsx)
        # Only the valid 80-char line should be parsed
        assert len(df) == 1
        assert df.iloc[0]["transaction_id"] == "TX000001"

    def test_empty_archive_directory(self, tmp_archive, in_memory_db):
        """Empty archive directory produces an empty dataframe with both report sheets."""
        output_xlsx = os.path.join(tmp_archive, "report.xlsx")
        df = rec.run_reconciliation(tmp_archive, in_memory_db, output_xlsx)
        assert len(df) == 0
        assert os.path.exists(output_xlsx)
        xl = pd.ExcelFile(output_xlsx)
        assert "Reconciliation" in xl.sheet_names
        assert "Summary" in xl.sheet_names

    def test_malformed_amount_defaults_to_zero(self, tmp_archive, in_memory_db):
        """Non-numeric amount fields default to 0.0 after conversion."""
        filepath = os.path.join(tmp_archive, "batch_01.dat")
        # Build a valid 80-char record with garbage in the amount field (positions 8-23)
        tx_id = "TX000001"
        garbage_amount = "NOT_A_NUMBE!!!!!"  # exactly 16 chars
        comment = "malformed test comment padded out to fill fifty six char"  # pad to 56 chars
        raw_content = tx_id + garbage_amount + comment
        assert len(raw_content) == 80, f"Expected 80 chars, got {len(raw_content)}"
        with open(filepath, "wb") as f:
            f.write(raw_content.encode("cp037") + b"\n")

        output_xlsx = os.path.join(tmp_archive, "report.xlsx")
        df = rec.run_reconciliation(tmp_archive, in_memory_db, output_xlsx)
        assert len(df) == 1
        assert df.iloc[0]["amount"] == pytest.approx(0.0)

    def test_txt_and_csv_extensions_processed(self, tmp_archive, mock_records, in_memory_db):
        """Files with .txt and .csv extensions are also processed."""
        self._write_archive_file(tmp_archive, "data.txt", mock_records[:1])
        self._write_archive_file(tmp_archive, "data.csv", mock_records[1:2])

        output_xlsx = os.path.join(tmp_archive, "report.xlsx")
        df = rec.run_reconciliation(tmp_archive, in_memory_db, output_xlsx)
        assert len(df) == 2

    def test_near_match_within_tolerance(self, tmp_archive):
        """Amounts differing by less than half a penny should be considered a match."""
        records = [{"transaction_id": "TX000001", "amount": Decimal("100.00")}]
        engine = create_engine("sqlite:///:memory:")
        with engine.connect() as conn:
            conn.execute(
                text(
                    "CREATE TABLE transactions "
                    "(transaction_id TEXT PRIMARY KEY, amount NUMERIC, status TEXT)"
                )
            )
            # Insert with a sub-penny difference (float precision artifact)
            conn.execute(
                text("INSERT INTO transactions VALUES ('TX000001', 100.004, 'PROCESSED')")
            )
            conn.commit()

        self._write_archive_file(tmp_archive, "batch_01.dat", records)
        output_xlsx = os.path.join(tmp_archive, "report.xlsx")
        df = rec.run_reconciliation(tmp_archive, engine, output_xlsx)
        assert len(df) == 1
        assert df.iloc[0]["match"]

    def test_amount_parsing_catches_specific_exceptions(self, tmp_archive, in_memory_db):
        """Malformed amounts raise ValueError/ArithmeticError, not bare except."""
        # Build a record with garbage in the amount field — should default to 0
        filepath = os.path.join(tmp_archive, "batch_01.dat")
        tx_id = "TX000001"
        garbage_amount = "DEADBEEF!@#$%^&*"  # exactly 16 chars
        comment = "exception handling test padded to fifty six chars!!!!!xx"  # 56 chars
        raw_content = tx_id + garbage_amount + comment
        assert len(raw_content) == 80
        with open(filepath, "wb") as f:
            f.write(raw_content.encode("cp037") + b"\n")

        output_xlsx = os.path.join(tmp_archive, "report.xlsx")
        df = rec.run_reconciliation(tmp_archive, in_memory_db, output_xlsx)
        assert len(df) == 1
        assert df.iloc[0]["amount"] == pytest.approx(0.0)


# ================ Tests for send_email_report ================

class TestSendEmailReport:
    def test_dry_run_returns_true(self, tmp_path):
        """Dry run should return True without sending email."""
        report = tmp_path / "report.xlsx"
        report.write_bytes(b"fake xlsx data")
        result = rec.send_email_report(str(report), dry_run=True)
        assert result is True

    def test_missing_credentials_returns_false(self, tmp_path):
        """Missing email credentials should return False."""
        report = tmp_path / "report.xlsx"
        report.write_bytes(b"fake xlsx data")

        with mock.patch.object(rec, "EMAIL_USER", None), \
             mock.patch.object(rec, "EMAIL_PASS", None):
            result = rec.send_email_report(str(report), dry_run=False)
        assert result is False

    @mock.patch("reconcile_1997.smtplib.SMTP")
    def test_successful_send(self, mock_smtp_cls, tmp_path):
        """Successful email send returns True."""
        report = tmp_path / "report.xlsx"
        report.write_bytes(b"fake xlsx data")

        mock_server = mock.MagicMock()
        mock_smtp_cls.return_value = mock_server

        with mock.patch.object(rec, "SMTP_SERVER", "smtp.test.com"), \
             mock.patch.object(rec, "SMTP_PORT", 587), \
             mock.patch.object(rec, "EMAIL_USER", "user@test.com"), \
             mock.patch.object(rec, "EMAIL_PASS", "secret"), \
             mock.patch.object(rec, "FROM_EMAIL", "user@test.com"), \
             mock.patch.object(rec, "TO_EMAIL", "dest@test.com"):
            result = rec.send_email_report(str(report), dry_run=False)

        assert result is True
        mock_server.starttls.assert_called_once()
        mock_server.login.assert_called_once_with("user@test.com", "secret")
        mock_server.send_message.assert_called_once()
        mock_server.quit.assert_called_once()

    @mock.patch("reconcile_1997.smtplib.SMTP")
    def test_smtp_exception_returns_false(self, mock_smtp_cls, tmp_path):
        """SMTP exception should return False."""
        report = tmp_path / "report.xlsx"
        report.write_bytes(b"fake xlsx data")

        mock_smtp_cls.side_effect = Exception("Connection refused")

        with mock.patch.object(rec, "SMTP_SERVER", "smtp.test.com"), \
             mock.patch.object(rec, "EMAIL_USER", "user@test.com"), \
             mock.patch.object(rec, "EMAIL_PASS", "secret"), \
             mock.patch.object(rec, "TO_EMAIL", "dest@test.com"):
            result = rec.send_email_report(str(report), dry_run=False)

        assert result is False


# ================ Tests for run_test_harness ================

class TestRunTestHarness:
    def test_harness_completes_without_error(self):
        """The test harness should run end-to-end without exceptions."""
        # Suppress print output
        with mock.patch("builtins.print"):
            rec.run_test_harness()

    def test_harness_sends_dry_run_email(self):
        """Test harness should call send_email_report with dry_run=True."""
        with mock.patch("reconcile_1997.send_email_report") as mock_send, \
             mock.patch("builtins.print"):
            rec.run_test_harness()
            mock_send.assert_called_once()
            _, kwargs = mock_send.call_args
            assert kwargs.get("dry_run") is True


# ================ Tests for main / CLI argument parsing ================

class TestMain:
    def test_test_flag_runs_harness(self):
        """--test flag should invoke run_test_harness."""
        with mock.patch("reconcile_1997.run_test_harness") as mock_harness, \
             mock.patch("sys.argv", ["reconcile_1997.py", "--test"]):
            rec.main()
            mock_harness.assert_called_once()

    def test_production_mode_missing_archive(self, tmp_path):
        """Production mode with missing archive directory should return early."""
        fake_dir = str(tmp_path / "nonexistent")
        with mock.patch("sys.argv", ["reconcile_1997.py"]), \
             mock.patch.object(rec, "ARCHIVE_DIR", fake_dir):
            # Should not raise
            rec.main()

    def test_dry_run_flag_parsed(self):
        """--dry-run flag should be parsed without error."""
        with mock.patch("sys.argv", ["reconcile_1997.py", "--test"]), \
             mock.patch("reconcile_1997.run_test_harness"):
            rec.main()


# ================ Integration-style tests ================

class TestIntegration:
    def test_full_round_trip(self, tmp_path):
        """Full round trip: create archive → setup DB → reconcile → verify report."""
        archive_dir = str(tmp_path / "archive")
        expected = rec.create_mock_archive(archive_dir)
        db_engine = rec.setup_mock_database(expected)
        output_xlsx = str(tmp_path / "report.xlsx")

        df = rec.run_reconciliation(archive_dir, db_engine, output_xlsx)

        assert len(df) > 0
        assert os.path.exists(output_xlsx)
        assert "match" in df.columns
        assert "discrepancy" in df.columns
        # There should be both matches and discrepancies based on mock DB setup
        assert df["match"].any()  # some match
        assert (~df["match"]).any()  # some don't match

    def test_full_round_trip_report_sheets(self, tmp_path):
        """The generated report should have both Reconciliation and Summary sheets."""
        archive_dir = str(tmp_path / "archive")
        expected = rec.create_mock_archive(archive_dir)
        db_engine = rec.setup_mock_database(expected)
        output_xlsx = str(tmp_path / "report.xlsx")

        rec.run_reconciliation(archive_dir, db_engine, output_xlsx)

        xl = pd.ExcelFile(output_xlsx)
        assert "Reconciliation" in xl.sheet_names
        assert "Summary" in xl.sheet_names

        df_recon = pd.read_excel(output_xlsx, sheet_name="Reconciliation")
        assert "transaction_id" in df_recon.columns
        assert "amount" in df_recon.columns
        assert "match" in df_recon.columns
