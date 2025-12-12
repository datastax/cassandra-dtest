import shutil

import pytest
import logging

from cassandra import ConsistencyLevel
from cassandra.query import (SimpleStatement, dict_factory)

from dtest import Tester

from tools.data import rows_to_list
from tools.misc import generate_ssl_stores
from upgrade_tests.upgrade_manifest import get_cluster_class, indev_dse_6_9, CC4

since = pytest.mark.since
logger = logging.getLogger(__name__)


@pytest.mark.upgrade_test
@since('4.0', max_version='4.1.x')
class TestUpgradeFromDSEWithConcurrentWritesAndRestarts(Tester):

    @pytest.fixture(autouse=True)
    def fixture_add_additional_log_patterns(self, fixture_dtest_setup):
        fixture_dtest_setup.ignore_log_patterns = fixture_dtest_setup.ignore_log_patterns + [
            r'Collectd is only supported on Linux',
            r'that is incompatible with the version this node can handle',
            r'Unable to merge schema from',
            # such an error happens once in a while for upgrade, it does not seem to have a functional impact though
            r'InvalidCrc',
        ]

    def test_upgrade_from_6_9_with_internode_encryption(self):
        self.upgrade_from_6_9(internode_encryption=True)

    def test_upgrade_from_6_9(self):
        self.upgrade_from_6_9(internode_encryption=False)

    def upgrade_from_6_9(self, internode_encryption=False):
        cluster = self.setup_nodes(indev_dse_6_9.version, internode_encryption=internode_encryption)
        cluster.start()

        for node in cluster.nodelist():
            node.nodetool('nodesyncservice disable')

        nodes = cluster.nodelist()

        out, err, _ = nodes[0].run_cqlsh("CREATE KEYSPACE ks WITH replication = {'class':'SimpleStrategy', 'replication_factor':3}")
        out, err, _ = nodes[0].run_cqlsh("CREATE TABLE ks.cf (id int primary key)")

        for i in range(0, 100):
            out, err, _ = cluster.nodelist()[0].run_cqlsh("INSERT INTO ks.cf (id) VALUES ({})".format(i))

        # upgrade nodes ensuring writes with CL=ALL work
        upgraded_node_1 = self._upgrade_node(cluster, nodes[0], internode_encryption=internode_encryption)
        self._assert_insert_select_works(upgraded_node_1, 100, 200)

        upgraded_node_2 = self._upgrade_node(cluster, nodes[1], internode_encryption=internode_encryption)
        self._assert_insert_select_works(upgraded_node_2, 200, 300)

        upgraded_node_3 = self._upgrade_node(cluster, nodes[2], internode_encryption=internode_encryption)
        self._assert_insert_select_works(upgraded_node_3, 300, 400)

        # rolling restart post upgrade
        upgraded_node_1.stop(wait_other_notice=True)
        self._assert_insert_select_works(upgraded_node_2, 400, 500, cl=ConsistencyLevel.QUORUM)
        upgraded_node_1.start(jvm_args=["-Dcassandra.skip_schema_check=true"], wait_other_notice=160,
                              wait_for_binary_proto=True)
        self._assert_insert_select_works(upgraded_node_1, 400, 500)

        upgraded_node_2.stop(wait_other_notice=True)
        self._assert_insert_select_works(upgraded_node_3, 500, 600, cl=ConsistencyLevel.QUORUM)
        upgraded_node_2.start(jvm_args=["-Dcassandra.skip_schema_check=true"], wait_other_notice=160,
                              wait_for_binary_proto=True)
        self._assert_insert_select_works(upgraded_node_2, 500, 600)

        upgraded_node_3.stop(wait_other_notice=True)
        self._assert_insert_select_works(upgraded_node_1, 600, 700, cl=ConsistencyLevel.QUORUM)
        upgraded_node_3.start(jvm_args=["-Dcassandra.skip_schema_check=true"], wait_other_notice=160,
                              wait_for_binary_proto=True)
        self._assert_insert_select_works(upgraded_node_3, 600, 700)

    def setup_nodes(self, version, internode_encryption=False):
        cluster = self.cluster

        meta_family_class = get_cluster_class(indev_dse_6_9.family)
        self.cluster.__class__ = meta_family_class
        self.cluster._cassandra_version = self.cluster.dse_username = self.cluster.dse_password = self.cluster.opscenter = None

        cluster.set_install_dir(version=version)

        cluster.set_configuration_options(values={'endpoint_snitch': 'org.apache.cassandra.locator.SimpleSnitch', 'nodesync': {'enabled': False}})

        if internode_encryption:
            generate_ssl_stores(self.fixture_dtest_setup.test_path)
            self.cluster.enable_internode_ssl(self.fixture_dtest_setup.test_path, enable_legacy_ssl_storage_port=True)

        self.fixture_dtest_setup.reinitialize_cluster_for_different_version()
        self.install_nodetool_legacy_parsing()

        cluster = cluster.populate(3)
        return cluster

    def _upgrade_node(self, cluster, node, internode_encryption=False):
        logger.info('stopping dse node ' + str(node.address()))
        node.stop(wait_other_notice=True)
        logger.info('stopped dse node ' + str(node.address()))

        meta_family_class = get_cluster_class(CC4)
        cluster.__class__ = meta_family_class
        cluster._cassandra_version = self.cluster.dse_username = self.cluster.dse_password = self.cluster.opscenter = None

        node.set_configuration_options(values={'nodesync': None}, delete_empty=True, delete_always=True)
        cluster.set_configuration_options(values={'nodesync': None}, delete_empty=True, delete_always=True)

        old_conf = node.get_conf_dir()
        node.__class__ = cluster.getNodeClass()
        shutil.copytree(old_conf, node.get_conf_dir())

        node.set_install_dir(install_dir=self.dtest_config.cassandra_dir)

        self.install_legacy_parsing(node)

        if internode_encryption:
            node.set_configuration_options(values={'server_encryption_options': {'enabled': True}})

        logger.info('starting hcd node' + str(node.address()))
        node.start(jvm_args=["-Dcassandra.skip_schema_check=true"], wait_other_notice=160, wait_for_binary_proto=True)
        logger.info('started hcd node' + str(node.address()))
        return node

    def _assert_insert_select_works(self, node, start, end, cl=ConsistencyLevel.ALL):
        session = self.patient_exclusive_cql_connection(node)
        for i in range(start, end):
            session.execute("INSERT INTO ks.cf (id) VALUES ({})".format(i))
        rows = rows_to_list(
            session.execute(SimpleStatement("SELECT * FROM ks.cf", consistency_level=cl)))
        assert len(rows) == end, "Expected to read {} rows from upgraded node but got {}".format(end, len(rows))
