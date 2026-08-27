import os
import unittest
from unittest.mock import patch

from app import config


class ConfigTests(unittest.TestCase):
    def test_folder_name_rejects_paths(self):
        with patch.dict(os.environ, {"TEST_FOLDER": "../movies"}):
            with self.assertRaisesRegex(RuntimeError, "single folder name"):
                config._folder_name("TEST_FOLDER")

if __name__ == "__main__":
    unittest.main()