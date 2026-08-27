import unittest

from app.tasks import progress


class ProgressTests(unittest.TestCase):
    def setUp(self):
        progress.clear()

    def tearDown(self):
        progress.clear()

    def test_running_completed_and_failed_lifecycle(self):
        progress.start("provider-a")
        progress.update(
            "provider-a",
            phase="database",
            message="Saving entries",
            current=250,
            total=1000,
            series_cached=25,
        )

        running = progress.snapshot("provider-a")
        self.assertEqual(running["status"], "running")
        self.assertEqual(running["current"], 250)
        self.assertEqual(running["total"], 1000)
        self.assertEqual(running["series_cached"], 25)

        progress.finish("provider-a")
        completed = progress.snapshot("provider-a")
        self.assertEqual(completed["status"], "completed")
        self.assertIsNotNone(completed["completed_at"])

        progress.start("provider-b")
        progress.fail("provider-b")
        self.assertEqual(progress.snapshot("provider-b")["status"], "failed")

    def test_snapshot_is_a_copy(self):
        progress.start("provider-a")
        state = progress.snapshot("provider-a")
        state["status"] = "changed"

        self.assertEqual(progress.snapshot("provider-a")["status"], "running")


if __name__ == "__main__":
    unittest.main()