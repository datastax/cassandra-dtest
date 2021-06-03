import logging
import time
import pytest

from cassandra import InvalidRequest

from dtest import Tester, create_ks
from tools.assertions import assert_one

since = pytest.mark.since
logger = logging.getLogger(__name__)

class BaseGuardrailsTester(Tester):

    def prepare(self, rf=1, options=None, nodes=3, install_byteman=False, extra_jvm_args=None, **kwargs):
        if options is None:
            options = {}

        if extra_jvm_args is None:
            extra_jvm_args = []

        cluster = self.cluster
        cluster.populate([nodes, 0], install_byteman=install_byteman)
        if options:
            cluster.set_configuration_options(values=options)

        cluster.start(jvm_args=extra_jvm_args)
        node1 = cluster.nodelist()[0]

        session = self.patient_cql_connection(node1, **kwargs)
        create_ks(session, 'ks', rf)

        return session


@since('4.0')
class TestGuardrails(BaseGuardrailsTester):

    def test_disk_usage_guardrail(self):
        """
        Test disk usage guardrail will warn if exceeds warn threshold and reject writes if exceeds failure threshold
        """

        self.fixture_dtest_setup.ignore_log_patterns = ["Write request failed because disk usage exceeds failure threshold"]
        guardrails_config = {'guardrails': {'disk_usage_percentage_warn_threshold': 98,
                                            'disk_usage_percentage_failure_threshold': 99}}

        logger.debug("prepare 2-node cluster with rf=1 and guardrails enabled")
        session = self.prepare(rf=1, nodes=2, options=guardrails_config, extra_jvm_args=['-Dcassandra.disk_usage.monitor_interval_ms=100'], install_byteman=True)
        node1, node2 = self.cluster.nodelist()
        session.execute("CREATE TABLE t (id int PRIMARY KEY, v int)")

        logger.debug("Inject FULL to node1, expect log on node1 and node2 rejects writes")
        self.disk_usage_injection(node1, "full", False)
        time.sleep(2)  # wait for disk state broadcast

        # verify node2 will reject writes if node1 is the replica
        session2 = self.patient_exclusive_cql_connection(node2, keyspace="ks")
        rows = 100
        failed = 0
        for x in range(rows):
            try:
                session2.execute("INSERT INTO t(id, v) VALUES({v}, {v})".format(v=x))
            except InvalidRequest:
                failed = failed + 1

        assert rows != failed, "Expect node2 rejects some writes, but rejected all"
        assert 0 != failed, "Expect node2 rejects some writes, but rejected nothing"
        assert_one(session2, "SELECT COUNT(*) FROM t", [rows - failed])

        logger.debug("Inject STUFFED to node1, node2 should warn client")
        session2.execute("TRUNCATE t")
        self.disk_usage_injection(node1, "stuffed")
        time.sleep(2)  # wait for disk state broadcast

        warnings = 0
        for x in range(rows):
            fut = session2.execute_async("INSERT INTO t(id, v) VALUES({v}, {v})".format(v=x))
            fut.result()
            if fut.warnings:
                assert ["Replica disk usage exceeds warn threshold"] == fut.warnings
                warnings = warnings + 1

        assert rows != warnings,"Expect node2 emits some warnings, but got all warnings"
        assert 0 != warnings,"Expect node2 emits some warnings, but got no warnings"
        assert_one(session2, "SELECT COUNT(*) FROM t", [rows])

        session.cluster.shutdown()
        session2.cluster.shutdown()

    def disk_usage_injection(self, node, state, clear_byteman=True):
        if clear_byteman:
            node.byteman_submit(['-u'])
        node.byteman_submit(["./byteman/guardrails/disk_usage_{}.btm".format(state)])
