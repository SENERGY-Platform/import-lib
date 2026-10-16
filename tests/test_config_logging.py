"""
   Copyright 2026 InfAI (CC SES)

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
"""

import json
import os
import unittest
from unittest import mock

from import_lib import import_lib as import_lib_module

SECRET = "s3cr3t-api-key-0123456789"


class TestConfigLogging(unittest.TestCase):
    def test_config_values_are_not_logged(self):
        # The CONFIG variable is the user's import configuration and may hold
        # credentials such as API keys. Only the key names may reach the log.
        env = {"CONFIG": json.dumps({"api_key": SECRET, "site": "site-value-xyz"}), "KAFKA_BOOTSTRAP": "localhost"}
        with mock.patch.dict(os.environ, env), \
                mock.patch.object(import_lib_module, "Producer"), \
                mock.patch.object(import_lib_module, "AdminClient"):
            with self.assertLogs("import", level="INFO") as captured:
                import_lib_module.ImportLib("test-import")

        output = "\n".join(captured.output)
        self.assertIn("api_key", output)
        self.assertIn("site", output)
        self.assertNotIn(SECRET, output)
        self.assertNotIn("site-value-xyz", output)


if __name__ == "__main__":
    unittest.main()
