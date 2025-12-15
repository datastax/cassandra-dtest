import os
import shutil
import pytest
import logging

from cassandra.query import dict_factory
from tools import sslkeygen

from dtest import Tester
from cassandra.protocol import SyntaxException, ConfigurationException

from tools.misc import generate_ssl_stores
from upgrade_tests.upgrade_manifest import get_cluster_class, indev_dse_6_9, indev_cc4

since = pytest.mark.since
logger = logging.getLogger(__name__)


# @pytest.mark.upgrade_test
@since('4.0')
class TestUpgradeWithInternodeEncryption(Tester):

    @pytest.fixture(autouse=True)
    def fixture_add_additional_log_patterns(self, fixture_dtest_setup):
        fixture_dtest_setup.ignore_log_patterns = fixture_dtest_setup.ignore_log_patterns + [
            r'Collectd is only supported on Linux',
        ]

    def upgrade_to_version(self, tag, start_rpc=True, wait=True, nodes=None):
        logger.debug('Upgrading to ' + tag)
        if nodes is None:
            nodes = self.cluster.nodelist()

        for node in nodes:
            logger.debug('Shutting down node: ' + node.name)
            node.drain()
            node.watch_log_for("DRAINED")
            node.stop(wait_other_notice=False)

        # Update Cassandra Directory
        for node in nodes:
            node.set_install_dir(version=tag)
            logger.debug("Set new cassandra dir for %s: %s" % (node.name, node.get_install_dir()))
        self.cluster.set_install_dir(version=tag)
        self.fixture_dtest_setup.reinitialize_cluster_for_different_version()

        # Restart nodes on new version
        for node in nodes:
            logger.debug('Starting %s on new version (%s)' % (node.name, tag))
            node.start(wait_other_notice=wait, wait_for_binary_proto=wait)

    def test_upgrade_from_6_9(self):
        credNode1 = sslkeygen.generate_credentials("127.0.0.1")
        credNode2 = sslkeygen.generate_credentials("127.0.0.2", credNode1.cakeystore, credNode1.cacert)
        credNode3 = sslkeygen.generate_credentials("127.0.0.3", credNode1.cakeystore, credNode1.cacert)

        cluster = self.setup_nodes(indev_dse_6_9.version, credNode1, credNode2, credNode3, encryption_optional=False)
        cluster.start()

        node1 = cluster.nodelist()[0]
        node1.stop(wait_other_notice=True)

        meta_family_class = get_cluster_class(indev_cc4.family)
        cluster.__class__ = meta_family_class
        cluster._cassandra_version = self.cluster.dse_username = self.cluster.dse_password = self.cluster.opscenter = None

        old_conf = node1.get_conf_dir()
        node1.__class__ = cluster.getNodeClass()
        shutil.copytree(old_conf, node1.get_conf_dir())

        node1.set_install_dir(version=indev_cc4.version)

        self.install_legacy_parsing(node1)

        node1.start(wait_for_binary_proto=True)


    def setup_nodes(self, version):
        cluster = self.cluster

        meta_family_class = get_cluster_class(indev_dse_6_9.family)
        self.cluster.__class__ = meta_family_class
        self.cluster._cassandra_version = self.cluster.dse_username = self.cluster.dse_password = self.cluster.opscenter = None

        cluster.set_install_dir(version=version)

        cluster.set_configuration_options(values={'endpoint_snitch': 'org.apache.cassandra.locator.SimpleSnitch'})

        generate_ssl_stores(self.fixture_dtest_setup.test_path)
        self.cluster.enable_internode_ssl(self.fixture_dtest_setup.test_path, enable_legacy_ssl_storage_port=True)

        self.fixture_dtest_setup.reinitialize_cluster_for_different_version()
        self.install_nodetool_legacy_parsing()

        cluster = cluster.populate(3)
        return cluster
