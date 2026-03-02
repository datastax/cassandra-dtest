import logging
import operator
import os
import pprint
import pytest
import psutil
import random
import shutil
import time
import uuid

from collections import defaultdict
from multiprocessing import Process, Queue

from cassandra import ConsistencyLevel, WriteTimeout
from cassandra.query import SimpleStatement

from dtest import Tester
from tools.misc import generate_ssl_stores
from .upgrade_manifest import (build_downgrade_pairs,
                               CC4, CC5, HCD_1, HCD_2,
                               get_cluster_class)
from .upgrade_through_versions_test import (data_writer, data_checker,
                                            counter_incrementer, counter_checker)

logger = logging.getLogger(__name__)

CC_FAMILIES = (CC4, CC5, HCD_1, HCD_2)

# CC5-specific config options that CC4 does not understand.
CC5_CONFIG_OPTIONS_TO_PRUNE = [
    'user_defined_functions_threads_enabled',
    'scripted_user_defined_functions_enabled',
    'storage_compatibility_mode',
]


@pytest.mark.upgrade_test
@pytest.mark.resource_intensive
class TestDowngrade(Tester):
    """
    Downgrades a 3-node Murmur3Partitioner cluster from CC5 to CC4.
    Verifies data written on CC5 is preserved after downgrade.
    """
    test_version_metas = None  # set on init to know which versions to use
    subprocs = None
    extra_config = None

    @pytest.fixture(autouse=True)
    def fixture_add_additional_log_patterns(self, fixture_dtest_setup):
        fixture_dtest_setup.ignore_log_patterns = (
            r'Can\'t send migration request: node.*is down',
            r'RejectedExecutionException.*ThreadPoolExecutor has shut down',
            r'Cannot update data center or rack from.*for live host',
            r'Invalid memtable configuration.*Falling back to default',
            r'unexpected exception caught while deserializing.*GOSSIP_SHUTDOWN',
            r'Snitch definitions.*do not define a location',
        )

    def reinit_cluster_for_family(self, meta):
        meta_family_class = get_cluster_class(meta.family)
        if self.cluster.__class__ != meta_family_class:
            logger.info("changing cluster type to {} (from {} bc {})".format(meta_family_class, self.cluster.__class__, meta))
            self.cluster.__class__ = meta_family_class
            self.cluster._cassandra_version = None
            return True
        return False

    def prepare(self):
        if type(self).__name__ == "TestDowngrade":
            pytest.skip("Skip base class, only generated classes run the tests")

        if len(self.test_version_metas) < 2:
            pytest.skip("Skipping downgrade test, not enough versions: {}".format(self.test_version_metas))

        starting_meta = self.test_version_metas[0]
        logger.debug("Downgrade test beginning, setting CASSANDRA_VERSION to {}, and jdk to {}.".format(
            starting_meta.version, starting_meta.java_version))
        cluster = self.cluster
        self.reinit_cluster_for_family(starting_meta)
        cluster.set_install_dir(version=starting_meta.version)
        self.install_nodetool_legacy_parsing()
        self.fixture_dtest_setup.reinitialize_cluster_for_different_version()
        logger.debug("Versions to test (%s): %s" % (type(self), str([v.version for v in self.test_version_metas])))

    def init_config(self):
        Tester.init_config(self)

        if self.extra_config is not None:
            logger.debug("Setting extra configuration options:\n{}".format(
                pprint.pformat(dict(self.extra_config), indent=4))
            )
            self.cluster.set_configuration_options(
                values=dict(self.extra_config)
            )

    @pytest.mark.timeout(3000)
    def test_parallel_downgrade(self):
        """
        Test downgrading cluster all at once (requires cluster downtime).
        """
        self.downgrade_scenario()

    @pytest.mark.timeout(3000)
    def test_rolling_downgrade(self):
        """
        Test rolling downgrade of the cluster, so we have mixed versions part way through.
        """
        self.downgrade_scenario(rolling=True)

    @pytest.mark.timeout(3000)
    def test_parallel_downgrade_with_internode_ssl(self):
        """
        Test downgrading cluster all at once, with internode ssl.
        """
        self.downgrade_scenario(internode_ssl=True)

    @pytest.mark.timeout(3000)
    def test_rolling_downgrade_with_internode_ssl(self):
        """
        Rolling downgrade test using internode ssl.
        """
        self.downgrade_scenario(rolling=True, internode_ssl=True)

    def downgrade_scenario(self, rolling=False, internode_ssl=False):
        self.prepare()
        self.row_values = set()
        cluster = self.cluster

        if internode_ssl:
            logger.debug("***using internode ssl***")
            generate_ssl_stores(self.fixture_dtest_setup.test_path)
            self.cluster.enable_internode_ssl(self.fixture_dtest_setup.test_path)

        logger.debug('Creating cluster (%s)' % self.test_version_metas[0].version)
        cluster.populate(3)

        # Start CC5 in CC_4 compatibility mode. FIXME: this shouldn't be needed after CNDB-16865
        cluster.set_configuration_options(values={'storage_compatibility_mode': 'CC_4'})

        [node.start(use_jna=True, wait_for_binary_proto=True,
                    jvm_args=['-Dcassandra.test.storage_compatibility_mode=CC_4'])
         for node in cluster.nodelist()]

        for i, node in enumerate(cluster.nodelist(), 1):
            setattr(self, 'node' + str(i), node)

        if rolling:
            self._create_schema_for_rolling()
        else:
            self._create_schema()

        self._log_current_ver(self.test_version_metas[0])

        if rolling:
            write_proc, verify_proc, verification_queue = self._start_continuous_write_and_verify(wait_for_rowcount=5000)

            # downgrade one node at a time
            target_meta = self.test_version_metas[1]
            for num, node in enumerate(self.cluster.nodelist()):
                time.sleep(60)

                self.downgrade_to_version(target_meta, partial=True, nodes=(node,), internode_ssl=internode_ssl)

                logger.debug(str(self.fixture_dtest_setup.subprocs))
                self._check_on_subprocs(self.fixture_dtest_setup.subprocs)
                logger.debug('Successfully downgraded %d of %d nodes to %s' %
                      (num + 1, len(self.cluster.nodelist()), target_meta.version))

            self.cluster.set_install_dir(version=target_meta.version)
            self.install_nodetool_legacy_parsing()
            self.fixture_dtest_setup.reinitialize_cluster_for_different_version()

            # Stop write processes
            write_proc.terminate()
            self._check_on_subprocs([verify_proc])
            self._wait_until_queue_condition('writes pending verification', verification_queue, operator.le, 0, max_wait_s=1200)
            self._check_on_subprocs([verify_proc])

            self._terminate_subprocs()
        else:
            # parallel downgrade
            self._write_values()
            self._increment_counters()

            target_meta = self.test_version_metas[1]
            self.downgrade_to_version(target_meta, internode_ssl=internode_ssl)
            self.cluster.set_install_dir(version=target_meta.version)
            self.install_nodetool_legacy_parsing()
            self.fixture_dtest_setup.reinitialize_cluster_for_different_version()

            self._check_values()
            self._check_counters()
            self._check_select_count()

            # write more data on the downgraded version and verify
            self._write_values()
            self._check_values()

        cluster.stop()

    def downgrade_to_version(self, version_meta, partial=False, nodes=None, internode_ssl=False):
        """
        Downgrade nodes. If *partial* is True, only downgrade those nodes
        specified by *nodes*, otherwise downgrade all nodes.
        """
        logger.debug('Downgrading {nodes} to {version}'.format(
            nodes=[n.name for n in nodes] if nodes is not None else 'all nodes',
            version=version_meta.version))
        logger.debug("JAVA_HOME: " + os.environ.get('JAVA_HOME', 'not set'))

        if not partial:
            nodes = self.cluster.nodelist()

        self.install_nodetool_legacy_parsing()
        for node in nodes:
            logger.debug('Flushing node: ' + node.name)
            node.nodetool('flush')
            logger.debug('Shutting down node: ' + node.name)
            node.drain()
            node.watch_log_for("DRAINED")
            node.stop(wait_other_notice=False)

        family_changed = self.reinit_cluster_for_family(version_meta)
        for node in nodes:
            if family_changed:
                old_conf = node.get_conf_dir()
                node.__class__ = self.cluster.getNodeClass()
                new_conf = node.get_conf_dir()
                if old_conf != new_conf and os.path.exists(old_conf) and not os.path.exists(new_conf):
                    shutil.copytree(old_conf, new_conf)
            node.set_install_dir(version=version_meta.version)
            self.install_legacy_parsing(node)
            logger.debug("Set new cassandra dir for %s: %s" % (node.name, node.get_install_dir()))

            # Prune CC5-specific config options before starting on CC4
            self._prune_config_for_downgrade(node, version_meta)

            if internode_ssl:
                node.set_configuration_options({'server_encryption_options': {'enabled': True, 'enable_legacy_ssl_storage_port': True}})

        self.cluster._Cluster__update_topology_files()

        for node in nodes:
            logger.debug('Starting %s on new version (%s)' % (node.name, version_meta.version))
            node.set_log_level("INFO")
            node.start(wait_other_notice=400, wait_for_binary_proto=True,
                       jvm_args=['-Dcassandra.disable_max_protocol_auto_override=true'])

    def _prune_config_for_downgrade(self, node, target_meta):
        """
        Remove config options from node's cassandra.yaml that the target version doesn't understand.
        """
        for opt in CC5_CONFIG_OPTIONS_TO_PRUNE:
            node.set_configuration_options({opt: None})

    def tearDown(self):
        self._terminate_subprocs()
        super(TestDowngrade, self).tearDown()

    def _check_on_subprocs(self, subprocs):
        subproc_statuses = [s.is_alive() for s in subprocs]
        if not all(subproc_statuses):
            message = "A subprocess has terminated early. Subprocess statuses: "
            for s in subprocs:
                message += "{name} (is_alive: {aliveness}, exitCode: {exitCode}), ".format(name=s.name, aliveness=s.is_alive(), exitCode=s.exitcode)
            message += "attempting to terminate remaining subprocesses now."
            self._terminate_subprocs()
            raise RuntimeError(message)

    def _terminate_subprocs(self):
        for s in self.fixture_dtest_setup.subprocs:
            if s.is_alive():
                try:
                    psutil.Process(s.pid).kill()
                except Exception:
                    logger.debug("Error terminating subprocess. There could be a lingering process.")
                    pass

    def _log_current_ver(self, current_version_meta):
        vers = [m.version for m in self.test_version_metas]
        curr_index = vers.index(current_version_meta.version)
        logger.debug(
            "Current downgrade path: {}".format(
                vers[:curr_index] + ['***' + current_version_meta.version + '***'] + vers[curr_index + 1:]))

    def _create_schema_for_rolling(self):
        session = self.patient_cql_connection(self.node2, protocol_version=self.protocol_version)

        session.execute("CREATE KEYSPACE upgrade WITH replication = {'class':'SimpleStrategy', 'replication_factor':3};")

        session.execute('use upgrade')
        session.execute('CREATE TABLE cf ( k uuid PRIMARY KEY, v uuid )')
        session.execute('CREATE INDEX vals ON cf (v)')

        session.execute("""
            CREATE TABLE countertable (
                k1 uuid,
                c counter,
                PRIMARY KEY (k1)
                );""")
        session.cluster.control_connection.wait_for_schema_agreement()

    def _create_schema(self):
        session = self.patient_cql_connection(self.node2, protocol_version=self.protocol_version)

        session.execute("CREATE KEYSPACE upgrade WITH replication = {'class':'SimpleStrategy', 'replication_factor':2};")

        session.execute('use upgrade')
        session.execute('CREATE TABLE cf ( k int PRIMARY KEY, v text )')
        session.execute('CREATE INDEX vals ON cf (v)')

        session.execute("""
            CREATE TABLE countertable (
                k1 text,
                k2 int,
                c counter,
                PRIMARY KEY (k1, k2)
                );""")
        session.cluster.control_connection.wait_for_schema_agreement()

    def _write_values(self, num=100):
        session = self.patient_cql_connection(self.node2, protocol_version=self.protocol_version)
        session.execute("use upgrade")
        for i in range(num):
            x = len(self.row_values) + 1
            session.execute("UPDATE cf SET v='%d' WHERE k=%d" % (x, x))
            self.row_values.add(x)

    def _check_values(self, consistency_level=ConsistencyLevel.ALL):
        for node in self.cluster.nodelist():
            session = self.patient_cql_connection(node, protocol_version=self.protocol_version)
            session.execute("use upgrade")
            for x in self.row_values:
                query = SimpleStatement("SELECT k,v FROM cf WHERE k=%d" % x, consistency_level=consistency_level)
                result = session.execute(query)
                k, v = result[0]
                assert x == k
                assert str(x) == v

    def _wait_until_queue_condition(self, label, queue, opfunc, required_len, max_wait_s=600):
        wait_end_time = time.time() + max_wait_s

        while time.time() < wait_end_time:
            try:
                qsize = queue.qsize()
            except NotImplementedError:
                logger.debug("Queue size may not be checkable on Mac OS X. Test will continue without waiting.")
                break
            if opfunc(qsize, required_len):
                logger.debug("{} queue size ({}) is '{}' to {}. Continuing.".format(label, qsize, opfunc.__name__, required_len))
                break

            if divmod(round(time.time()), 30)[1] == 0:
                logger.debug("{} queue size is at {}, target is to reach '{}' {}".format(label, qsize, opfunc.__name__, required_len))

            time.sleep(0.1)
            continue
        else:
            raise RuntimeError("Ran out of time waiting for queue size ({}) to be '{}' to {}. Aborting.".format(qsize, opfunc.__name__, required_len))

    def _start_continuous_write_and_verify(self, wait_for_rowcount=0, max_wait_s=600):
        to_verify_queue = Queue(10000)
        verification_done_queue = Queue(maxsize=500)

        writer = Process(name="data_writer", target=data_writer, args=(self, to_verify_queue, verification_done_queue, 25))
        writer.daemon = True
        self.fixture_dtest_setup.subprocs.append(writer)
        writer.start()

        if wait_for_rowcount > 0:
            self._wait_until_queue_condition('rows written (but not verified)', to_verify_queue, operator.ge, wait_for_rowcount, max_wait_s=max_wait_s)

        verifier = Process(name="data_checker", target=data_checker, args=(self, to_verify_queue, verification_done_queue))
        verifier.daemon = True
        self.fixture_dtest_setup.subprocs.append(verifier)
        verifier.start()

        return writer, verifier, to_verify_queue

    def _increment_counters(self, opcount=25000):
        logger.debug("performing {opcount} counter increments".format(opcount=opcount))
        session = self.patient_cql_connection(self.node2, protocol_version=self.protocol_version)
        session.execute("use upgrade;")

        update_counter_query = ("UPDATE countertable SET c = c + 1 WHERE k1='{key1}' and k2={key2}")

        self.expected_counts = {}
        for i in range(10):
            self.expected_counts[uuid.uuid4()] = defaultdict(int)

        fail_count = 0

        for i in range(opcount):
            key1 = random.choice(list(self.expected_counts.keys()))
            key2 = random.randint(1, 10)
            try:
                query = SimpleStatement(update_counter_query.format(key1=key1, key2=key2), consistency_level=ConsistencyLevel.ALL)
                session.execute(query)
            except WriteTimeout:
                fail_count += 1
            else:
                self.expected_counts[key1][key2] += 1
            if fail_count > 100:
                break

        assert fail_count < 100, "Too many counter increment failures"

    def _check_counters(self):
        logger.debug("Checking counter values...")
        session = self.patient_cql_connection(self.node2, protocol_version=self.protocol_version)
        session.execute("use upgrade;")

        for key1 in list(self.expected_counts.keys()):
            for key2 in list(self.expected_counts[key1].keys()):
                expected_value = self.expected_counts[key1][key2]

                query = SimpleStatement("SELECT c from countertable where k1='{key1}' and k2={key2};".format(key1=key1, key2=key2),
                                        consistency_level=ConsistencyLevel.ONE)
                results = session.execute(query)

                if results is not None:
                    actual_value = results[0][0]
                else:
                    actual_value = None

                assert actual_value == expected_value

    def _check_select_count(self, consistency_level=ConsistencyLevel.ALL):
        logger.debug("Checking SELECT COUNT(*)")
        session = self.patient_cql_connection(self.node2, protocol_version=self.protocol_version)
        session.execute("use upgrade;")

        expected_num_rows = len(self.row_values)

        countquery = SimpleStatement("SELECT COUNT(*) FROM cf;", consistency_level=consistency_level)
        result = session.execute(countquery)

        if result is not None:
            actual_num_rows = result[0][0]
            assert actual_num_rows == expected_num_rows, "SELECT COUNT(*) returned %s when expecting %s" % (actual_num_rows, expected_num_rows)
        else:
            pytest.fail("Count query did not return")


def create_downgrade_class(clsname, version_metas, protocol_version, extra_config=None):
    if extra_config is None:
        extra_config = (('partitioner', 'org.apache.cassandra.dht.Murmur3Partitioner'),)

    parent_classes = (TestDowngrade,)

    print("Creating downgrade test class {} ".format(clsname))
    print("  for C* versions:\n{} ".format(pprint.pformat(version_metas)))
    print("  using protocol: v{}, and parent classes: {}".format(protocol_version, [cls.__name__ for cls in parent_classes]))

    newcls = type(
            clsname,
            parent_classes,
            {'test_version_metas': version_metas, '__test__': True, 'protocol_version': protocol_version, 'extra_config': extra_config}
        )

    if clsname in globals():
        raise RuntimeError("Class by name already exists!")

    globals()[clsname] = newcls
    return newcls


logger.info("Building downgrade pairs")
for pair in build_downgrade_pairs():
    create_downgrade_class(
        'Test' + pair.name,
        [pair.starting_meta, pair.upgrade_meta],
        protocol_version=min(pair.starting_meta.max_proto_v, pair.upgrade_meta.max_proto_v),
    )
    logger.info("Created downgrade test {} for versions {}, protocol {}".format(
        pair.name,
        [m.name for m in [pair.starting_meta, pair.upgrade_meta]],
        min(pair.starting_meta.max_proto_v, pair.upgrade_meta.max_proto_v)))
