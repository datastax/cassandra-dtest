import os
import shutil
import pytest
import logging

from cassandra.query import dict_factory
from tools import sslkeygen

from dtest import Tester
from cassandra.protocol import SyntaxException, ConfigurationException

from upgrade_tests.upgrade_manifest import current_dse_6_9

since = pytest.mark.since
logger = logging.getLogger(__name__)


# @pytest.mark.upgrade_test
@since('4.0')
class TestUpgradeWithInternodeEncryption(Tester):
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

        self.setup_nodes(current_dse_6_9.version, credNode1, credNode2, credNode3, encryption_optional=False)

        self.cluster.start()

    def setup_nodes(self, version, credentials1, credentials2, credentials3, endpoint_verification=False,
                    client_auth=False, internode_encryption='all', encryption_optional=None):
        cluster = self.cluster

        cluster.set_install_dir(version=version)
        self.fixture_dtest_setup.reinitialize_cluster_for_different_version()

        cluster = cluster.populate(3)
        self.node1 = cluster.nodelist()[0]
        self.copy_cred(credentials1, self.node1, internode_encryption, encryption_optional, endpoint_verification,
                       client_auth)

        self.node2 = cluster.nodelist()[1]
        self.copy_cred(credentials2, self.node2, internode_encryption, encryption_optional, endpoint_verification,
                       client_auth)

        self.node3 = cluster.nodelist()[2]
        self.copy_cred(credentials3, self.node3, internode_encryption, encryption_optional, endpoint_verification,
                       client_auth)

    def copy_cred(self, credentials, node, internode_encryption, encryption_optional, endpoint_verification=False,
                  client_auth=False):
        dir = node.get_conf_dir()
        kspath = os.path.join(dir, 'keystore.jks')
        tspath = os.path.join(dir, 'truststore.jks')
        shutil.copyfile(credentials.keystore, kspath)
        shutil.copyfile(credentials.cakeystore, tspath)

        server_enc_options = {
            'internode_encryption': internode_encryption,
            'keystore': kspath,
            'keystore_password': 'cassandra',
            'truststore': tspath,
            'truststore_password': 'cassandra',
            'require_endpoint_verification': endpoint_verification,
            'require_client_auth': client_auth,
        }

        if self.cluster.version() >= '4.0' and encryption_optional is not None:
            server_enc_options['optional'] = encryption_optional

        node.set_configuration_options(values={
            'server_encryption_options': server_enc_options
        })
