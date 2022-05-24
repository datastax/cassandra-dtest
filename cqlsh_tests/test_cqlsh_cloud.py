# coding=utf-8

# Copyright DataStax, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import logging
import pytest
from ccmlib.node import ToolError

from dtest import Tester

logger = logging.getLogger(__name__)
since = pytest.mark.since


@since("4.0")
class TestSecureBundleConnection(Tester):
    """
    Tests related to cqlsh behavior for cloud e.g. secure bundle connection.
    We only test cqlshrc behavior.
    Testing if the connection using secure bundle is really working
    requires a true cluster with generated secure bundle to run.
    And this is not possible without testing infrastructure/tooling changes.

    We can assume that it is correctly tested by the python driver or
    will be tested in the next stage of testing (cloud native).

    Validation is done using --debug information or error msgs.

    Inspired by STAR-765.
    """

    CQLSHRC_PATH = 'cqlsh_tests/cqlshrc.sample.cloud'
    BUNDLE_PATH = 'cqlsh_tests/secure-connect-test.zip'

    def prepare(self, start=False):
        if not self.cluster.nodelist():
            self.cluster.populate(1)
            if start:
                self.cluster.start()
        return self.cluster.nodelist()[0]

    def _expect_tool_error(self, cmds, options, msg):
        node = self.cluster.nodelist()[0]
        with pytest.raises(ToolError, match=msg):
            out, err, _ = node.run_cqlsh(cmds=cmds, cqlsh_options=options)
            return out, err

    def test_start_fails_on_non_existing_file(self):
        self.prepare()
        self._expect_tool_error(cmds='HELP',
                                options=['--secure-connect-bundle', 'not-existing-file.zip'],
                                msg='No such file or directory')

    def test_start_fails_when_file_not_a_bundle(self):
        self.prepare()
        self._expect_tool_error(cmds='HELP',
                                options=['--secure-connect-bundle', self.CQLSHRC_PATH],
                                msg='Unable to open the zip file for the cloud config')

    def test_read_bundle_path_from_cqlshrc(self):
        self.prepare()
        self._expect_tool_error(cmds='HELP',
                                options=['--cqlshrc', self.CQLSHRC_PATH],
                                msg="No such file or directory: '/path/to/creds.zip'")

    def test_host_and_port_are_ignored_with_secure_bundle(self):
        # it should connect with provided host and port to the started ccm node
        node = self.prepare(start=True)
        node.run_cqlsh("HELP", [])
        # but fail with secure bundle even if port and host are set
        expected_msg = "https://1263dd11-0aa5-41ef-8e56-17fa5fc7036e-europe-west1.db.astra.datastax.com:31669"
        self._expect_tool_error(cmds='HELP',
                                options=['--secure-connect-bundle', self.BUNDLE_PATH, node.ip_addr, '9042'],
                                msg=expected_msg)

    def test_default_consistency_level_for_secure_connect_bundle_param(self):
        self.prepare()
        self._expect_tool_error(cmds='HELP',
                                options=['--secure-connect-bundle', 'not-existing-file.zip', '--debug'],
                                msg='Using consistency level:.*LOCAL_QUORUM')

    def test_default_consistency_level_for_secure_connect_bundle_in_clqshrc(self):
        self.prepare()
        self._expect_tool_error(cmds='HELP',
                                options=['--cqlshrc', self.CQLSHRC_PATH, '--debug'],
                                msg='Using consistency level:.*LOCAL_QUORUM')

    def test_set_consistency_level_for_secure_connect_bundle_in_clqshrc(self):
        self.prepare()
        self._expect_tool_error(cmds='HELP',
                                options=['--cqlshrc', self.CQLSHRC_PATH, '--debug', '--consistency-level', 'TWO'],
                                msg='Using consistency level:.*TWO')

    def test_debug_should_include_cloud_details(self):
        self.prepare()
        self._expect_tool_error(cmds='HELP',
                                options=['--secure-connect-bundle', 'not-existing-file.zip', '--debug'],
                                msg='Using secure connect bundle.*not-existing-file.zip')

    @pytest.mark.skip("we cannot test it without ccm secure conn bundle support in ccm")
    def test_endpoint_load_balancing_policy_is_used(self):
        # to test this we would need a 3 nodes cloud cluster
        assert False, "TODO: implement"

    @pytest.mark.skip("we cannot test it without ccm secure conn bundle support in ccm")
    def test_connects_correctly(self):
        assert False, "TODO: implement"

    @pytest.mark.skip("we cannot test it without ccm secure conn bundle support in ccm")
    def test_login_command_keeps_cloud_connection_using_bundle(self):
        # cqlsh.py -b some-bundle.zip -u user -p password
        # LOGIN user(password)
        assert False
