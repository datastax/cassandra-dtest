import logging
import time
import pytest
import re

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
        cluster.set_log_level('TRACE')
        cluster.populate(nodes, install_byteman=install_byteman)
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
        if self.supports_cc4_guardrails():
            guardrails_config = {'guardrails': {'disk_usage_percentage_warn_threshold': 98,
                                                'disk_usage_percentage_failure_threshold': 99}}
        else:
            guardrails_config = {'data_disk_usage_percentage_warn_threshold': 98, 'data_disk_usage_percentage_fail_threshold': 99}

        logger.debug("prepare 2-node cluster with rf=1 and guardrails enabled")
        session = self.prepare(rf=1, nodes=2, options=guardrails_config, extra_jvm_args=['-Dcassandra.disk_usage.monitor_interval_ms=100'], install_byteman=True)
        node1, node2 = self.cluster.nodelist()
        session.execute("CREATE TABLE t (id int PRIMARY KEY, v int)")

        logger.debug("Inject FULL to node1, expect log on node1 and node2 rejects writes")
        mark = node1.mark_log()
        self.disk_usage_injection(node1, "full", False)
        node1.watch_log_for("Adding state DISK_USAGE: FULL", filename='debug.log', from_mark=mark, timeout=10)

        # verify node2 will reject writes if node1 is the replica
        session2 = self.patient_exclusive_cql_connection(node2, keyspace="ks")
        rows = 100
        failed = 0
        for x in range(rows):
            try:
                session2.execute("INSERT INTO t(id, v) VALUES({v}, {v})".format(v=x))
            except InvalidRequest as e:
                assert re.search("Write request failed because disk usage exceeds failure threshold", str(e))
                failed = failed + 1

        assert rows != failed, "Expect node2 rejects some writes, but rejected all"
        assert 0 != failed, "Expect node2 rejects some writes, but rejected nothing"
        assert_one(session2, "SELECT COUNT(*) FROM t", [rows - failed])

        logger.debug("Inject STUFFED to node1, node2 should warn client")
        session2.execute("TRUNCATE t")
        mark = node1.mark_log()
        self.disk_usage_injection(node1, "stuffed")
        node1.watch_log_for("Adding state DISK_USAGE: STUFFED", filename='debug.log', from_mark=mark, timeout=10)

        warnings = 0
        for x in range(rows):
            fut = session2.execute_async("INSERT INTO t(id, v) VALUES({v}, {v})".format(v=x))
            fut.result()
            if fut.warnings:
                if self.supports_cc4_guardrails():
                    assert ["Replica disk usage exceeds warn threshold"] == fut.warnings
                else:
                    assert ["Guardrail replica_disk_usage violated: Replica disk usage exceeds warning threshold"] == fut.warnings
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
