import logging
import pytest

from dtest import Tester, create_ks

logger = logging.getLogger(__name__)
since = pytest.mark.since


@since("4.0")
class TestCqlshTypes(Tester):

    def prepare(self, workload=None):
        if not self.cluster.nodelist():
            self.allow_log_errors = True
            self.cluster.populate(1)
            if workload is not None:
                for node in self.cluster.nodelist():
                    node.set_workload(workload)
            logger.debug('About to start cluster')
            self.cluster.start()
            logger.debug('Cluster started')
            for node in self.cluster.nodelist():
                node.watch_log_for('Starting listening for CQL clients', timeout=60)
            self.cluster.nodelist()[0].watch_log_for('Created default superuser')
            self.node = self.cluster.nodelist()[0]

            conn = self.patient_cql_connection(self.node)
            create_ks(conn, 'ks', 1)

        logger.debug('prepare completed')

    def test_point(self):
        self.prepare()

        expected = 'POINT (1.2 2.3)'
        self.node.run_cqlsh("CREATE TABLE ks.point_tbl (k INT PRIMARY KEY, point 'PointType');")
        self.node.run_cqlsh("INSERT INTO ks.point_tbl (k, point) VALUES (1, '{}')".format(expected))
        result = self.node.run_cqlsh("SELECT * FROM ks.point_tbl;")
        assert expected in result[0], result

    def test_linestring(self):
        self.prepare()

        expected = 'LINESTRING (30.0 10.0, 10.0 30.0, 40.0 40.0)'
        self.node.run_cqlsh("CREATE TABLE ks.line_tbl (k INT PRIMARY KEY, linestring 'LineStringType');")
        self.node.run_cqlsh("INSERT INTO ks.line_tbl (k, linestring) VALUES (1, '{}')".format(expected))
        result = self.node.run_cqlsh("SELECT * FROM ks.line_tbl;")
        assert expected in result[0], result

    def test_polygon(self):
        self.prepare()

        expected = 'POLYGON ((30.0 10.0, 40.0 40.0, 20.0 40.0, 10.0 20.0, 30.0 10.0))'
        self.node.run_cqlsh("CREATE TABLE ks.polygon_tbl (k INT PRIMARY KEY, polygon 'PolygonType');")
        self.node.run_cqlsh("INSERT INTO ks.polygon_tbl (k, polygon) VALUES (1, '{}')".format(expected))
        result = self.node.run_cqlsh("SELECT * FROM ks.polygon_tbl;")
        assert expected in result[0], result

    def test_date_range(self):
        self.prepare()

        expected = '[2015-01 TO *]'
        self.node.run_cqlsh("CREATE TABLE ks.date_range_tbl (k INT PRIMARY KEY, date_range_tbl 'DateRangeType');")
        self.node.run_cqlsh("INSERT INTO ks.date_range_tbl (k, date_range_tbl) VALUES (1, '{}')".format(expected))
        result = self.node.run_cqlsh("SELECT * FROM ks.date_range_tbl;")
        assert expected in result[0], result
