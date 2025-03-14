import os
import re
import tempfile
from distutils.version import LooseVersion

import pytest
import logging
import time

from flaky import flaky

from itertools import chain
from shutil import rmtree

from cassandra import ConsistencyLevel, ReadTimeout, Unavailable
from cassandra.query import SimpleStatement
from ccmlib.node import Node

from bootstrap_test import BootstrapTester
from dtest import Tester, mk_bman_path
from tools.assertions import assert_bootstrap_state, assert_all, assert_not_running
from tools.data import rows_to_list

since = pytest.mark.since
logger = logging.getLogger(__name__)


class NodeUnavailable(Exception):
    pass


class BaseReplaceAddressTest(Tester):

    @pytest.fixture(autouse=True)
    def fixture_add_additional_log_patterns(self, fixture_dtest_setup):
        fixture_dtest_setup.ignore_log_patterns = (
            # This one occurs when trying to send the migration to a
            # node that hasn't started yet, and when it does, it gets
            # replayed and everything is fine.
            r'Can\'t send migration request: node.*is down',
            r'Migration task failed to complete',  # 10978
            # ignore streaming error during bootstrap
            r'Streaming error occurred',
            r'failed stream session',
            r'Failed to properly handshake with peer',
            # ignore streaming error during resumable tests
            r'peer 127.0.0.1:7000 is probably down',
            r'Attempting gossip state mutation from illegal thread'
        )

    def _setup(self, n=3, opts=None, enable_byteman=False, mixed_versions=False):
        logger.debug("Starting cluster with {} nodes.".format(n))
        self.cluster.populate(n)
        if opts is not None:
            logger.debug("Setting cluster options: {}".format(opts))
            self.cluster.set_configuration_options(opts)

        self.cluster.set_batch_commitlog(enabled=True, use_batch_window = self.cluster.version() < '5.0')
        self.query_node = self.cluster.nodelist()[0]
        self.replaced_node = self.cluster.nodelist()[-1]

        self.cluster.seeds.remove(self.replaced_node)

        if enable_byteman:
            # set up byteman
            self.query_node.byteman_port = '8100'
            self.query_node.import_config_files()

        if mixed_versions:
            logger.debug("Starting nodes on version 2.2.4")
            self.cluster.set_install_dir(version="2.2.4")
            self.install_nodetool_legacy_parsing()

        jvm_args = []
        if self.cluster.cassandra_version() >= '4.0':
            jvm_args.append("-Dcassandra.failed_bootstrap_timeout_ms=30000")

        self.cluster.start(jvm_args=jvm_args)

        if self.cluster.cassandra_version() >= '2.2.0':
            session = self.patient_cql_connection(self.query_node)
            # Change system_auth keyspace replication factor to 2, otherwise replace will fail
            session.execute("""ALTER KEYSPACE system_auth
                            WITH replication = {'class':'SimpleStrategy',
                            'replication_factor':2};""")

    def _do_replace(self, same_address=False, jvm_option='replace_address',
                    wait_other_notice=False, wait_for_binary_proto=True,
                    replace_address=None, opts=None, data_center=None,
                    extra_jvm_args=None):
        """
        This explicitly starts the replacement node with reset_bootstrap_progress=false as the tests were originally
        authored in a pre 5.0 world where bootstrap resume was enabled by default. As tests that exercise this functionality
        explicitly set it or assume a world with this config, we should be in good shape.
        """
        if replace_address is None:
            replace_address = self.replaced_node.address()
        if data_center is None:
            data_center = "dc1"

        # only create node if it's not yet created
        if self.replacement_node is None:
            replacement_address = '127.0.0.4'
            if same_address:
                replacement_address = self.replaced_node.address()
                self.cluster.remove(self.replaced_node)

            logger.debug("Starting replacement node {} with jvm_option '{}={}'".format(replacement_address, jvm_option, replace_address))
            self.replacement_node = Node('replacement', cluster=self.cluster, auto_bootstrap=True,
                                         thrift_interface=None, storage_interface=(replacement_address, 7000),
                                         jmx_port='7400', remote_debug_port='0', initial_token=None, binary_interface=(replacement_address, 9042))
            if opts is not None:
                logger.debug("Setting options on replacement node: {}".format(opts))
                self.replacement_node.set_configuration_options(opts)
            self.cluster.add(self.replacement_node, False, data_center=data_center)

        if extra_jvm_args is None:
            extra_jvm_args = []
        extra_jvm_args.extend(["-Dcassandra.{}={}".format(jvm_option, replace_address),
                               "-Dcassandra.ring_delay_ms=10000",
                               "-Dcassandra.broadcast_interval_ms=10000",
                               "-Dcassandra.reset_bootstrap_progress=false"])

        self.replacement_node.start(jvm_args=extra_jvm_args,
                                    wait_for_binary_proto=wait_for_binary_proto, wait_other_notice=wait_other_notice)

        if self.cluster.cassandra_version() >= '2.2.8' and same_address:
            self.replacement_node.watch_log_for("Writes will not be forwarded to this node during replacement",
                                                timeout=60)

    def _stop_node_to_replace(self, gently=False, table='keyspace1.standard1', cl=ConsistencyLevel.THREE):
        if self.replaced_node.is_running():
            logger.debug("Stopping {}".format(self.replaced_node.name))
            self.replaced_node.stop(gently=gently, wait_other_notice=True)

        logger.debug("Testing node stoppage (query should fail).")
        with pytest.raises((Unavailable, ReadTimeout)):
            session = self.patient_cql_connection(self.query_node)
            query = SimpleStatement('select * from {}'.format(table), consistency_level=cl)
            session.execute(query)

    def _insert_data(self, n='1k', rf=3, whitelist=False):
        logger.debug("Inserting {} entries with rf={} with stress...".format(n, rf))
        self.query_node.stress(['write', 'n={}'.format(n), 'no-warmup', '-schema', 'replication(factor={})'.format(rf),
                                '-rate', 'threads=10'],
                               whitelist=whitelist)
        self.cluster.flush()
        time.sleep(20)

    def _fetch_initial_data(self, table='keyspace1.standard1', cl=ConsistencyLevel.THREE, limit=10000):
        logger.debug("Fetching initial data from {} on {} with CL={} and LIMIT={}".format(table, self.query_node.name, cl, limit))
        session = self.patient_cql_connection(self.query_node)
        query = SimpleStatement('select * from {} LIMIT {}'.format(table, limit), consistency_level=cl)
        return rows_to_list(session.execute(query, timeout=20))

    def _verify_data(self, initial_data, table='keyspace1.standard1', cl=ConsistencyLevel.ONE, limit=10000,
                     restart_nodes=False):
        assert len(initial_data) > 0, "Initial data must be greater than 0"

        # query should work again
        logger.debug("Stopping old nodes")
        for node in self.cluster.nodelist():
            if node.is_running() and node != self.replacement_node:
                logger.debug("Stopping {}".format(node.name))
                node.stop(gently=False, wait_other_notice=True)

        logger.debug("Verifying {} on {} with CL={} and LIMIT={}".format(table, self.replacement_node.address(), cl, limit))
        session = self.patient_exclusive_cql_connection(self.replacement_node)
        assert_all(session, 'select * from {} LIMIT {}'.format(table, limit),
                   expected=initial_data,
                   cl=cl)

    def _verify_replacement(self, node, same_address):
        if not same_address:
            if self.cluster.cassandra_version() >= '2.2.7':
                node.watch_log_for("Node {} is replacing {}"
                                   .format(self.replacement_node.address_for_current_version_slashy(),
                                           self.replaced_node.address_for_current_version_slashy()),
                                   timeout=60, filename='debug.log')
                node.watch_log_for("Node {} will complete replacement of {} for tokens"
                                   .format(self.replacement_node.address_for_current_version_slashy(),
                                           self.replaced_node.address_for_current_version_slashy()), timeout=10)
                gossip_action = "removing endpoint" if self.cluster.cassandra_version() < '5.1' else "evicting"
                node.watch_log_for("{} {}".format(gossip_action, self.replaced_node.address_for_current_version_slashy()),
                                   timeout=60, filename='debug.log')
            else:
                node.watch_log_for("between /{} and /{}; /{} is the new owner"
                                   .format(self.replaced_node.address(),
                                           self.replacement_node.address(),
                                           self.replacement_node.address()),
                                   timeout=60)

    def _verify_tokens_migrated_successfully(self, expected_match_count=None):
        if not self.dtest_config.use_vnodes:
            num_tokens = 1
        else:
            # a little hacky but grep_log returns the whole line...
            num_tokens = int(self.replacement_node.get_conf_option('num_tokens'))

        logger.debug("Verifying {} tokens migrated successfully".format(num_tokens))
        replmnt_address = self.replacement_node.address_for_current_version_slashy()
        repled_address = self.replaced_node.address_for_current_version_slashy()
        token_ownership_log = r"Token (.*?) changing ownership from {} to {}".format(repled_address,
                                                                                     replmnt_address)
        # watch log until we see a matching log line
        logger.debug("Waiting for any matching log message")
        self.replacement_node.watch_log_for(token_ownership_log)
        # now grep until we find the expected number of lines
        logger.debug("Found log message, now getting others")
        moved_tokens = {}
        timeout_seconds = 10
        start = time.time()
        while len(moved_tokens) < num_tokens:
            logs = self.replacement_node.grep_log(token_ownership_log)
            moved_tokens = set([l[1].group(1) for l in logs])
            logger.debug("Found {} matching messages in log".format(len(moved_tokens)))
            if len(moved_tokens) < num_tokens:
                assert start + timeout_seconds >= time.time(), (
                    "Found only {} of expected {} log messages before timeout".format(len(moved_tokens), num_tokens))
                time.sleep(1)

        logger.debug("number of moved tokens: {}".format(len(moved_tokens)))

        # pre 5.1 we expect notifications to only be logged once, so in that case check
        # that there are no additional matching lines in the replacement node log
        total_match_count = len(self.replacement_node.grep_log(token_ownership_log))
        if expected_match_count is not None:
            assert total_match_count == expected_match_count

        assert len(moved_tokens) == num_tokens
        return total_match_count

    def _test_insert_data_during_replace(self, same_address, mixed_versions=False):
        """
        @jira_ticket CASSANDRA-8523
        """
        default_install_dir = self.cluster.get_install_dir()
        self._setup(opts={'hinted_handoff_enabled': False}, mixed_versions=mixed_versions)

        self._insert_data(n='1k')
        initial_data = self._fetch_initial_data()
        self._stop_node_to_replace()

        if mixed_versions:
            logger.debug("Upgrading all except {} to current version".format(self.query_node.address()))
            self.cluster.set_install_dir(install_dir=default_install_dir)
            for node in self.cluster.nodelist():
                if node.is_running() and node != self.query_node:
                    logger.debug("Upgrading {} to current version".format(node.address()))
                    node.stop(gently=True, wait_other_notice=True)
                    node.start(wait_for_binary_proto=True)

        # start node in current version on write survey mode
        self._do_replace(same_address=same_address, extra_jvm_args=["-Dcassandra.write_survey=true"])

        # Insert additional keys on query node
        self._insert_data(n='2k', whitelist=True)

        # If not same address or mixed versions, query node should forward writes to replacement node
        # so we update initial data to reflect additional keys inserted during replace
        if not same_address and not mixed_versions:
            initial_data = self._fetch_initial_data(cl=ConsistencyLevel.TWO)

        logger.debug("Joining replaced node")
        self.replacement_node.nodetool("join")

        if not same_address:
            for node in self.cluster.nodelist():
                # if mixed version, query node is not upgraded so it will not print replacement log
                if node.is_running() and (not mixed_versions or node != self.query_node):
                    self._verify_replacement(node, same_address)

        self._verify_data(initial_data)


class TestReplaceAddress(BaseReplaceAddressTest):

    @pytest.mark.resource_intensive
    def test_replace_stopped_node(self):
        """
        Test that we can replace a node that is not shutdown gracefully.
        """
        self._test_replace_node(gently=False)

    @pytest.mark.resource_intensive
    def test_replace_shutdown_node(self):
        """
        @jira_ticket CASSANDRA-9871
        Test that we can replace a node that is shutdown gracefully.
        """
        self._test_replace_node(gently=True)

    @pytest.mark.resource_intensive
    def test_replace_stopped_node_same_address(self):
        """
        @jira_ticket CASSANDRA-8523
        Test that we can replace a node with the same address correctly
        """
        self._test_replace_node(gently=False, same_address=True)

    @pytest.mark.resource_intensive
    def test_replace_first_boot(self):
        self._test_replace_node(jvm_option='replace_address_first_boot')

    def _test_replace_node(self, gently=False, jvm_option='replace_address', same_address=False):
        """
        Check that the replace address function correctly replaces a node that has failed in a cluster.
        Create a cluster, cause a node to fail, and bring up a new node with the replace_address parameter.
        Check that tokens are migrated and that data is replicated properly.
        """
        self._setup(n=3)
        self._insert_data()
        initial_data = self._fetch_initial_data()
        self._stop_node_to_replace(gently=gently)
        self._do_replace(same_address=same_address)

        for node in self.cluster.nodelist():
            if node.is_running() and node != self.replacement_node:
                self._verify_replacement(node, same_address)

        if not same_address:
            previous_log_size = self._verify_tokens_migrated_successfully()

        # restart replacement node
        self.replacement_node.stop(gently=False)
        self.replacement_node.start(wait_for_binary_proto=True, wait_other_notice=False)

        # Before CEP-21, we redo this check because restarting node should not result
        # in tokens being moved again, ie number should be same. Post CEP-21, however,
        # the metadata log is replayed at startup, so the message will be logged
        # repeatedly (although the movement only actually happens once). We can address
        # this with smarter logging in Cassandra.
        if not same_address and self.cluster.version() < LooseVersion('5.1'):
            self._verify_tokens_migrated_successfully(previous_log_size)

        self._verify_data(initial_data)

    @pytest.mark.resource_intensive
    def test_replace_active_node(self):
        self.fixture_dtest_setup.ignore_log_patterns = list(self.fixture_dtest_setup.ignore_log_patterns) + [
            r'Exception encountered during startup', r'Unable to replace node for token', r'Unable to replace live node /127.0.0.3:7000']

        self._setup(n=3)
        self._do_replace(wait_for_binary_proto=False)

        logger.debug("Waiting for replace to fail")
        self.replacement_node.watch_log_for("java.lang.UnsupportedOperationException: Cannot replace a live node...")
        assert_not_running(self.replacement_node)

    @pytest.mark.resource_intensive
    def test_replace_nonexistent_node(self):
        self.fixture_dtest_setup.ignore_log_patterns = list(self.fixture_dtest_setup.ignore_log_patterns) + [
            # This is caused by starting a node improperly (replacing active/nonexistent)
            r'Exception encountered during startup',
            # This is caused by trying to replace a nonexistent node
            r'Exception in thread Thread']

        self._setup(n=3)
        self._do_replace(replace_address='127.0.0.5', wait_for_binary_proto=False)

        logger.debug("Waiting for replace to fail")
        node_log_str = "/127.0.0.5" if self.cluster.version() < '4.0' else "/127.0.0.5:7000"
        log_message = "java.lang.RuntimeException: Cannot replace_address {} because it doesn't exist in gossip" \
            if self.cluster.version() < LooseVersion('5.1') \
            else "Cannot replace node {} which is not currently joined"
        self.replacement_node.watch_log_for(log_message.format(node_log_str))
        assert_not_running(self.replacement_node)

    @since('3.6')
    def test_fail_without_replace(self):
        """
        When starting a node from a clean slate with the same address as
        an existing down node, the node should error out even when
        auto_bootstrap = false (or the node is a seed) and tell the user
        to use replace_address.
        @jira_ticket CASSANDRA-10134
        """
        self.fixture_dtest_setup.ignore_log_patterns = list(self.fixture_dtest_setup.ignore_log_patterns) + [
            r'Exception encountered during startup']

        self._setup(n=3)
        self._insert_data()
        node1, node2, node3 = self.cluster.nodelist()

        mark = None
        for auto_bootstrap in (True, False):
            logger.debug("Stopping node 3.")
            node3.stop(gently=False)

            # completely delete the data, commitlog, and saved caches
            for d in chain([os.path.join(node3.get_path(), "commitlogs")],
                           [os.path.join(node3.get_path(), "saved_caches")],
                           [os.path.join(node3.get_path(), "metadata")],
                           node3.data_directories()):
                if os.path.exists(d):
                    rmtree(d)

            node3.set_configuration_options(values={'auto_bootstrap': auto_bootstrap})
            logger.debug("Starting node 3 with auto_bootstrap = {val}".format(val=auto_bootstrap))
            node3.start(wait_other_notice=False)
            node3.watch_log_for('Use cassandra.replace_address if you want to replace this node', from_mark=mark, timeout=20)
            mark = node3.mark_log()

    @since('3.0')
    def test_fail_when_seed(self):
        """
        When a node is a seed replace should fail
        @jira_ticket CASSANDRA-14463
        """
        self.fixture_dtest_setup.ignore_log_patterns = list(self.fixture_dtest_setup.ignore_log_patterns) + [
            r'Couldn\'t find table with id 5bc52802-de25-35ed-aeab-188eecebb090', # system_auth
            r'Exception encountered during startup',
            r'Unknown endpoint'
        ]

        # see CASSANDRA-17611
        if self.cluster.version() < '4.0':
            self.fixture_dtest_setup.ignore_log_patterns = list(self.fixture_dtest_setup.ignore_log_patterns) + [
                r'RejectedExecutionException']

        self._setup(n=3)
        node1, node2, node3 = self.cluster.nodelist()
        self.cluster.seeds.append(node3.address())

        node3.stop(gently=False)
        mark = node3.mark_log()

        for d in chain([os.path.join(node3.get_path(), "commitlogs")],
                       [os.path.join(node3.get_path(), "saved_caches")],
                       [os.path.join(node3.get_path(), "metadata")],
                       node3.data_directories()):
            if os.path.exists(d):
                rmtree(d)

        node3.start(jvm_args=["-Dcassandra.replace_address=" + node3.address()], wait_other_notice=False)
        node3.watch_log_for('Replacing a node without bootstrapping risks invalidating consistency guarantees', from_mark=mark, timeout=20)

    @since('3.6')
    def test_unsafe_replace(self):
        """
        To handle situations such as failed disk in a JBOD, it may be desirable to
        replace a node without bootstrapping. In such scenarios best practice
        advice has been to wipe the node's system keyspace data, set the initial
        tokens via cassandra.yaml, startup without bootstrap and then repair.
        Starting the node as a replacement allows the tokens to be learned from
        gossip, but previously required auto_bootstrap=true. Since CASSANDRA-10134
        replacement is allowed without bootstrapping, but it requires the operator
        to acknowledge the risk in doing so by setting the cassandra.allow_unsafe_replace
        system property at startup.

        @jira_ticket CASSANDRA-10134
        """
        self.fixture_dtest_setup.ignore_log_patterns = list(self.fixture_dtest_setup.ignore_log_patterns) + [
            r'Exception encountered during startup']

        # see CASSANDRA-17611
        if self.cluster.version() < '4.0':
            self.fixture_dtest_setup.ignore_log_patterns = list(self.fixture_dtest_setup.ignore_log_patterns) + [
                r'RejectedExecutionException']

        self._setup(n=3)
        self._insert_data()
        initial_data = self._fetch_initial_data()
        self.replacement_node = self.replaced_node

        for set_allow_unsafe_flag in [False, True]:
            logger.debug("Stopping {}".format(self.replaced_node.name))
            self.replaced_node.stop(gently=False)

            # completely delete the system keyspace data plus commitlog and saved caches
            for d in self.replacement_node.data_directories():
                system_data = os.path.join(d, 'system')
                if os.path.exists(system_data):
                    rmtree(system_data)

            for d in ['commitlogs', 'saved_caches', 'metadata']:
                p = os.path.join(self.replacement_node.get_path(), d)
                if os.path.exists(p):
                    rmtree(p)

            self.replacement_node.set_configuration_options(values={'auto_bootstrap': False})
            mark = self.replacement_node.mark_log()

            if set_allow_unsafe_flag:
                logger.debug('Starting replacement node with auto_bootstrap = false and replace_address = {} and allow_unsafe_replace = true'.format(self.replaced_node.address()))
                self._do_replace(extra_jvm_args=['-Dcassandra.allow_unsafe_replace=true'])
                self._verify_data(initial_data)
            else:
                logger.debug('Starting replacement node with auto_bootstrap = false and replace_address = {}'.format(self.replaced_node.address()))
                self._do_replace(wait_for_binary_proto=False)
                self.replacement_node.watch_log_for('To perform this operation, please restart with -Dcassandra.allow_unsafe_replace=true',
                                                    from_mark=mark, timeout=20)

    @pytest.mark.skip_version('3.9')
    @since('2.2')
    def test_insert_data_during_replace_same_address(self):
        """
        Test that replacement node with same address DOES NOT receive writes during replacement
        @jira_ticket CASSANDRA-8523
        """
        self._test_insert_data_during_replace(same_address=True)

    @pytest.mark.skip_version('3.9')
    @since('2.2')
    def test_insert_data_during_replace_different_address(self):
        """
        Test that replacement node with different address DOES receive writes during replacement
        @jira_ticket CASSANDRA-8523
        """
        self._test_insert_data_during_replace(same_address=False)

    @since('2.2')
    @pytest.mark.resource_intensive
    def test_resume_failed_replace(self):
        """
        Test resumable bootstrap while replacing node. Feature introduced in
        2.2 with ticket https://issues.apache.org/jira/browse/CASSANDRA-8838

        @jira_ticket https://issues.apache.org/jira/browse/CASSANDRA-8838
        """
        self._test_restart_failed_replace(mode='resume')

    @since('2.2')
    @pytest.mark.resource_intensive
    def test_restart_failed_replace_with_reset_resume_state(self):
        """Test replace with resetting bootstrap progress"""
        self._test_restart_failed_replace(mode='reset_resume_state')

    @since('2.2')
    @pytest.mark.resource_intensive
    def test_restart_failed_replace(self):
        """
        Test that if a node fails to replace, it can join the cluster even if the data is wiped.
        """
        self._test_restart_failed_replace(mode='wipe')

    def _test_restart_failed_replace(self, mode):
        self.fixture_dtest_setup.ignore_log_patterns = list(self.fixture_dtest_setup.ignore_log_patterns) + [
            r'Error while waiting on bootstrap to complete',
            r'Cannot send the message .* as messaging service is shutting down']

        self._setup(n=3, enable_byteman=True)
        self._insert_data(n="1k")

        initial_data = self._fetch_initial_data()

        self._stop_node_to_replace()

        logger.debug("Submitting byteman script to make stream fail")
        btmmark = self.query_node.mark_log()

        if self.cluster.version() < '4.0':
            self.query_node.byteman_submit([mk_bman_path('pre4.0/stream_failure.btm')])
            self._do_replace(jvm_option='replace_address_first_boot',
                             opts={'streaming_socket_timeout_in_ms': 1000},
                             wait_for_binary_proto=False,
                             wait_other_notice=True)
        else:
            self.query_node.byteman_submit([mk_bman_path('4.0/stream_failure.btm')])
            self._do_replace(jvm_option='replace_address_first_boot', wait_for_binary_proto=False, wait_other_notice=True)

        # Make sure bootstrap did not complete successfully
        self.query_node.watch_log_for("Triggering network failure", from_mark=btmmark)
        self.query_node.watch_log_for("Stream failed", from_mark=btmmark)
        self.replacement_node.watch_log_for("Stream failed")
        self.replacement_node.watch_log_for("Some data streaming failed.*IN_PROGRESS$")

        if mode == 'reset_resume_state':
            mark = self.replacement_node.mark_log()
            logger.debug("Restarting replacement node with -Dcassandra.reset_bootstrap_progress=true")
            # restart replacement node with resetting bootstrap state (with 180s timeout)
            self.replacement_node.stop()
            self.replacement_node.start(jvm_args=[
                                        "-Dcassandra.replace_address_first_boot={}".format(self.replaced_node.address()),
                                        "-Dcassandra.reset_bootstrap_progress=true"
                                        ],
                                        wait_for_binary_proto=180)
            # check if we reset bootstrap state
            self.replacement_node.watch_log_for("Resetting bootstrap progress to start fresh", from_mark=mark)
        elif mode == 'resume':
            logger.debug("Resuming failed bootstrap")
            self.replacement_node.nodetool(BootstrapTester.nodetool_resume_command(self.cluster))
            # check if we skipped already retrieved ranges
            self.replacement_node.watch_log_for("already available. Skipping streaming.")
            self.replacement_node.watch_log_for("Resume complete")
        elif mode == 'wipe':
            self.replacement_node.stop()

            logger.debug("Waiting other nodes to detect node stopped")
            node_log_str = self.replacement_node.address_for_current_version_slashy()
            self.query_node.watch_log_for("FatClient {} has been silent for 30000ms, removing from gossip".format(node_log_str), timeout=120)
            if self.cluster.version() < LooseVersion('5.1'):
                self.query_node.watch_log_for("Node {} failed during replace.".format(node_log_str), timeout=120, filename='debug.log')
            else:
                logger.debug("Calling nodetool abortbootstrap --ip {}".format(self.replacement_node.address()))
                self.query_node.nodetool("abortbootstrap --ip {}".format(self.replacement_node.address()))

            logger.debug("Restarting node after wiping data")
            self._cleanup(self.replacement_node)
            self.replacement_node.start(jvm_args=["-Dcassandra.replace_address_first_boot={}"
                                        .format(self.replaced_node.address())],
                                        wait_for_binary_proto=120)
        else:
            raise RuntimeError('invalid mode value {mode}'.format(mode=mode))

        # check if bootstrap succeeded
        assert_bootstrap_state(self, self.replacement_node, 'COMPLETED')

        logger.debug("Bootstrap finished successfully, verifying data.")

        self._verify_data(initial_data)

    def test_replace_with_insufficient_replicas(self):
        """
        Test that replace fails when there are insufficient replicas
        @jira_ticket CASSANDRA-11848
        """
        self.fixture_dtest_setup.ignore_log_patterns = list(self.fixture_dtest_setup.ignore_log_patterns) + [
            r'Unable to find sufficient sources for streaming range']

        self._setup(n=3)
        self._insert_data(rf=2)

        self._stop_node_to_replace()
        # stop other replica. For CEP-21 purposes, we need to ensure this is not
        # the cluster's only CMS member
        logger.debug("Stopping other replica")
        self.cluster.nodelist()[1].stop(wait_other_notice=True)

        options = {}
        # CEP-21: availability is intentionally degraded, so we lower the required number of
        # acks required for the replacement node to progress to a point where it attempts to
        # perform streaming
        if self.cluster.version() >= LooseVersion('5.1'):
            options = {'progress_barrier_min_consistency_level': 'ONE',
                       'progress_barrier_default_consistency_level': 'ONE'}

        self._do_replace(wait_for_binary_proto=False,
                         wait_other_notice=False,
                         opts=options)

        # replace should fail due to insufficient replicas
        self.replacement_node.watch_log_for("Unable to find sufficient sources for streaming range")
        assert_not_running(self.replacement_node)

    def test_multi_dc_replace_with_rf1_stcs(self):
        self._test_multi_dc_replace_with_rf1('SizeTieredCompactionStrategy')

    @since("4.0")
    def test_multi_dc_replace_with_rf1_ucs(self):
        self._test_multi_dc_replace_with_rf1('UnifiedCompactionStrategy')

    def _test_multi_dc_replace_with_rf1(self, compaction_strategy):
        """
        Test that multi-dc replace works when rf=1 on each dc
        """
        self._setup(n=[1, 1])

        yaml_config = """
        # Create the keyspace and table
        keyspace: keyspace1
        keyspace_definition: |
          CREATE KEYSPACE keyspace1 WITH replication = {{'class': 'NetworkTopologyStrategy', 'dc1': 1, 'dc2': 1}};
        table: users
        table_definition:
          CREATE TABLE users (
            username text,
            first_name text,
            last_name text,
            email text,
            PRIMARY KEY(username)
          ) WITH compaction = {{'class':'{compaction_strategy}'}};
        insert:
          partitions: fixed(1)
          batchtype: UNLOGGED
        queries:
          read:
            cql: select * from users where username = ?
            fields: samerow
        """.format(compaction_strategy=compaction_strategy)

        with tempfile.NamedTemporaryFile(mode='w+') as stress_config:
            stress_config.write(yaml_config)
            stress_config.flush()
            self.query_node.stress(['user', 'profile=' + stress_config.name, 'n=10k', 'no-warmup',
                                    'ops(insert=1)', '-rate', 'threads=5'])
            # need to sleep for a bit to try and let things catch up as we frequently do a lot of
            # GC after the stress invocation above causing the next step of the test to timeout.
            # and then flush to make sure we really are fully caught up
            time.sleep(30)

        # Save initial data
        table_name = 'keyspace1.users'
        initial_data = self._fetch_initial_data(table=table_name, cl=ConsistencyLevel.TWO)

        self._stop_node_to_replace(table=table_name)

        self._do_replace(data_center='dc2')

        assert_bootstrap_state(self, self.replacement_node, 'COMPLETED')

        # Check that keyspace was replicated from dc1 to dc2
        assert not self.replacement_node.grep_log("Unable to find sufficient sources for streaming range")

        self._verify_data(initial_data, table=table_name, cl=ConsistencyLevel.LOCAL_ONE)

    def _cleanup(self, node):
        commitlog_dir = os.path.join(node.get_path(), 'commitlogs')
        for data_dir in node.data_directories():
            logger.debug("Deleting {}".format(data_dir))
            rmtree(data_dir)
        rmtree(commitlog_dir)
        metadata_dir = os.path.join(node.get_path(), 'metadata')
        if os.path.exists(metadata_dir):
            rmtree(metadata_dir)

    @since('4.0')
    def test_revive_endpoint(self):
        """
        Rudimentary test for GossiperMBean.reviveEndpoint/assassinateEndpoint/unsafeSetEndpointState.

        @jira_ticket HCD-73
        """
        self._setup(n=2)

        node1, node2 = self.cluster.nodelist()
        node2.stop(wait_other_notice=False)

        gen = self._get_endpoint_generation(node1, node2)
        logger.debug("node2 generation:{}".format(gen))

        # 1st test - assassinate the endpoint
        logger.debug("assassinate node2")
        (out, err, _) = node1.nodetool("assassinate {}".format(node2.address()))
        logger.debug("  stdout:{}".format(out))
        logger.debug("  stderr:{}".format(err))
        status = self._get_endpoint_status(node1, node2)
        assert "LEFT" == status, "Expecting node2 to have Gossip status LEFT, but is {}".format(status)

        gen_assassinate = self._get_endpoint_generation(node1, node2)
        logger.debug("node2 generation:{}".format(gen_assassinate))

        assert gen_assassinate > gen, "assassinate should have increased the Gossip generation"

        # Now revive the endpoint
        logger.debug("revive node2")
        (out, err, _) = node1.nodetool("sjk mx -b org.apache.cassandra.net:type=Gossiper -mc -op reviveEndpoint -a {}".format(node2.address()))
        logger.debug("  stdout:{}".format(out))
        logger.debug("  stderr:{}".format(err))
        status = self._get_endpoint_status(node1, node2)
        assert "NORMAL" == status, "Expecting node2 to have Gossip status NORMAL, but is {}".format(status)

        gen_revive = self._get_endpoint_generation(node1, node2)
        logger.debug("node2 generation:{}".format(gen_revive))
        assert gen_revive > gen_assassinate, "revive should have increased the Gossip generation"

        # 2nd test - bring the endpoint into hibernate
        logger.debug("hibernate node2")
        (out, err, _) = node1.nodetool("sjk mx -b org.apache.cassandra.net:type=Gossiper -mc -op unsafeSetEndpointState -a {} -a hibernate".format(node2.address()))
        logger.debug("  stdout:{}".format(out))
        logger.debug("  stderr:{}".format(err))
        status = self._get_endpoint_status(node1, node2)
        assert "hibernate" == status, "Expecting node2 to have Gossip status hibernate, but is {}".format(status)

        gen_hiberate = self._get_endpoint_generation(node1, node2)
        logger.debug("node2 generation:{}".format(gen_hiberate))
        assert gen_hiberate > gen_revive, "assassinate should have increased the Gossip generation"

        # Now revive the endpoint
        logger.debug("revive node2")
        (out, err, _) = node1.nodetool("sjk mx -b org.apache.cassandra.net:type=Gossiper -mc -op reviveEndpoint -a {}".format(node2.address()))
        logger.debug("  stdout:{}".format(out))
        logger.debug("  stderr:{}".format(err))
        status = self._get_endpoint_status(node1, node2)
        assert "NORMAL" == status, "Expecting node2 to have Gossip status NORMAL, but is {}".format(status)

        gen_revive2 = self._get_endpoint_generation(node1, node2)
        logger.debug("node2 generation:{}".format(gen_revive2))
        assert gen_revive2 > gen_hiberate, "revive should have increased the Gossip generation"

        nodetool_stderr = node1.nodetool("sjk mx -b org.apache.cassandra.net:type=Gossiper -mc -op reviveEndpoint -a {}".format(node2.address())).stderr
        assert "Cannot revive endpoint /127.0.0.2:7000: not in a (silent) shutdown state: NORMAL" in nodetool_stderr

    def _get_endpoint_generation(self, node, target):
        (out, _, _) = node.nodetool("sjk mx -b org.apache.cassandra.net:type=FailureDetector -mc -op getEndpointState -a {}".format(target.address()))
        return int([re.sub(r"  generation:([0-9]+)", r"\1", l) for l in out.splitlines() if re.match(r"  generation.*", l)][0])

    def _get_endpoint_status(self, node, target):
        (out, _, _) = node.nodetool("sjk mx -b org.apache.cassandra.net:type=FailureDetector -mc -op getEndpointState -a {}".format(target.address()))
        return [re.sub(r"  STATUS:\d+:([A-Za-z]+).*", r"\1", l) for l in out.splitlines() if re.match(r"  STATUS.*", l)][0]
