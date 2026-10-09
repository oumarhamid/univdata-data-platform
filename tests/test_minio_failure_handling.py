
import sys
import unittest
from unittest.mock import MagicMock, patch

# Permet d'importer les modules du projet dans le conteneur Airflow.
sys.path.insert(0, "/opt/airflow/dags")

from minio.error import S3Error
from utils import bfs_scraper_universal, minio_client


class TestMinioFailureHandling(unittest.TestCase):

    def test_successful_upload(self):
        """Verify that a successful upload calls MinIO."""
        client = MagicMock()
        client.bucket_exists.return_value = True

        with patch.object(
            minio_client,
            "get_client",
            return_value=client,
        ):
            minio_client.upload_bytes(
                "raw-json",
                "tests/success.json",
                b'{"ok": true}',
            )

        client.put_object.assert_called_once()

    def test_s3_error_is_propagated(self):
        """Verify that an S3 error is propagated to the caller."""
        client = MagicMock()
        client.bucket_exists.return_value = True

        client.put_object.side_effect = S3Error(
            "AccessDenied",
            "Simulated MinIO failure",
            "/raw-json/tests/failure.json",
            "test-request-id",
            "test-host-id",
            None,
        )

        with patch.object(
            minio_client,
            "get_client",
            return_value=client,
        ):
            with self.assertRaises(S3Error):
                minio_client.upload_bytes(
                    "raw-json",
                    "tests/failure.json",
                    b'{"ok": false}',
                )

    def test_scraping_log_failure_does_not_propagate(self):
        """Verify that a logging failure does not stop the scraper."""
        with patch.object(
            bfs_scraper_universal,
            "upload_bytes",
            side_effect=RuntimeError("MinIO unavailable"),
        ):
            with patch("builtins.print") as mocked_print:
                bfs_scraper_universal.save_scraping_error(
                    "raw-logs",
                    "https://example.org/page",
                    0,
                    "Simulated scraping error",
                )

        # Check the warning without depending on accented characters.
        warning_found = any(
            "[WARN] Journal MinIO" in str(call)
            for call in mocked_print.call_args_list
        )

        self.assertTrue(
            warning_found,
            "Expected a warning when saving the log fails.",
        )

    def test_scraper_data_upload_failure_is_propagated(self):
        """Verify that a failed HTML upload raises an exception."""
        config = {
            "domain": "example.org",
            "start_url": "https://example.org/",
            "max_pages_per_domain": 1,
            "max_depth": 0,
            "min_html_size": 0,
            "rate_limit_seconds": 0,
            "use_playwright": False,
        }

        with patch.object(
            bfs_scraper_universal,
            "load_config",
            return_value=config,
        ):
            with patch.object(
                bfs_scraper_universal,
                "fetch_with_requests",
                return_value=(
                    b"<html>test</html>",
                    "text/html",
                    200,
                ),
            ):
                with patch.object(
                    bfs_scraper_universal,
                    "upload_bytes",
                    side_effect=RuntimeError(
                        "Simulated storage failure"
                    ),
                ):
                    with self.assertRaisesRegex(
                        RuntimeError,
                        "Simulated storage failure",
                    ):
                        bfs_scraper_universal.bfs_scrape_universal(
                            "unused-config.json"
                        )


if __name__ == "__main__":
    unittest.main(verbosity=2)