import pytest
import logging

from distutils.version import LooseVersion

from dtest import Tester, create_ks

since = pytest.mark.since
logger = logging.getLogger(__name__)


class TestCqlTracing(Tester):
    """
    Smoke test that the default implementation for tracing works. Also test
    that Cassandra falls back to the default tracing implementation when the
    user specifies an invalid implementation.

    # TODO write a mock Tracing implementation and assert, at least, it can be
    #      instantiated when specified as a custom tracing implementation.
    """

    def prepare(self, create_keyspace=True, nodes=3, rf=3, protocol_version=3, jvm_args=None, random_partitioner=False, **kwargs):
        if jvm_args is None:
            jvm_args = []

        jvm_args.append('-Dcassandra.wait_for_tracing_events_timeout_secs=15')

        cluster = self.cluster
        opts = {'write_request_timeout_in_ms': 30000, 'read_request_timeout_in_ms': 30000}
        if str(cluster.cassandra_version()) >= '4.1.6' and not str(cluster.cassandra_version()).startswith(('5.0-alpha', '5.0-beta')):
            opts['native_transport_timeout'] = '30s'
        cluster.set_configuration_options(values=opts);

        if random_partitioner:
            cluster.set_partitioner("org.apache.cassandra.dht.RandomPartitioner")
        else:
            cluster.set_partitioner("org.apache.cassandra.dht.Murmur3Partitioner")

        cluster.populate(nodes)
        node1 = cluster.nodelist()[0]
        cluster.start(jvm_args=jvm_args)

        session = self.patient_cql_connection(node1, protocol_version=protocol_version)
        if create_keyspace:
            create_ks(session, 'ks', rf)
        return session

    def trace(self, session):
        """
        * CREATE a table
        * enable TRACING
        * SELECT on a known system table and assert it ran with tracing by checking the output
        * INSERT a row into the created system table and assert it ran with tracing
        * SELECT from the table and assert it ran with tracing

        @param session The Session object to use to create a table.
        @jira_ticket CASSANDRA-10392
        """

        node1 = self.cluster.nodelist()[0]

        # Create
        session.execute("""
            CREATE TABLE ks.users (
                userid uuid PRIMARY KEY,
                firstname text,
                lastname text,
                age int
            );
        """)

        out, err, _ = node1.run_cqlsh('TRACING ON')
        if self.cluster.version() >= LooseVersion('5.0'):
            # See CASSANDRA-18547
            assert ('Tracing set to FULL.\n' if node1.is_converged_core() else 'Tracing set to ON.\n') in out
        else:
            assert 'Tracing is enabled' in out

        out, err, _ = node1.run_cqlsh('TRACING ON; SELECT * from system.peers')
        assert 'Tracing session: ' in out
        assert 'Request complete ' in out

        # Inserts
        out, err, _ = node1.run_cqlsh(
            "CONSISTENCY ALL; TRACING ON; "
            "INSERT INTO ks.users (userid, firstname, lastname, age) "
            "VALUES (550e8400-e29b-41d4-a716-446655440000, 'Frodo', 'Baggins', 32)",
            cqlsh_options=['--request-timeout', '30'])
        logger.debug(out)
        assert 'Tracing session: ' in out

        assert node1.address_for_current_version_slashy() in out
        assert self.cluster.nodelist()[1].address_for_current_version_slashy() in out
        assert self.cluster.nodelist()[2].address_for_current_version_slashy() in out

        assert 'Parsing INSERT INTO ks.users ' in out
        assert 'Request complete ' in out

        # Queries
        out, err, _ = node1.run_cqlsh('CONSISTENCY ALL; TRACING ON; '
                                      'SELECT firstname, lastname '
                                      'FROM ks.users WHERE userid = 550e8400-e29b-41d4-a716-446655440000',
                                      cqlsh_options=['--request-timeout', '30'])
        logger.debug(out)
        assert 'Tracing session: ' in out

        assert ' 127.0.0.1 ' in out
        assert ' 127.0.0.2 ' in out
        assert ' 127.0.0.3 ' in out
        assert 'Request complete ' in out
        assert " Frodo |  Baggins" in out

    @since('2.2')
    def test_tracing_simple(self):
        """
        Test tracing using the default tracing class. See trace().

        @jira_ticket CASSANDRA-10392
        @jira_ticket CASSANDRA-11598
        # Restricted to 2.2+ due to flakiness on 2.1.  See CASSANDRA-11598 and CASSANDRA-12407 for details.
        """
        session = self.prepare()
        self.trace(session)

    @since('3.4')
    def test_tracing_unknown_impl(self):
        """
        Test that Cassandra logs an error, but keeps its default tracing
        behavior, when a nonexistent tracing class is specified.

        * set a nonexistent custom tracing class
        * run trace()
        * if running the test on a version with custom tracing classes
          implemented, check that an error about the nonexistent class was
          logged.

        @jira_ticket CASSANDRA-10392
        """
        expected_error = 'Cannot use class junk for tracing'
        self.fixture_dtest_setup.ignore_log_patterns = [expected_error]
        session = self.prepare(jvm_args=['-Dcassandra.custom_tracing_class=junk'])
        self.trace(session)

        errs = self.cluster.nodelist()[0].grep_log_for_errors()
        logger.debug('Errors after attempted trace with unknown tracing class: {errs}'.format(errs=errs))
        assert len(errs) == 1
        if self.cluster.version() >= LooseVersion('3.10'):
            # See CASSANDRA-11706 and PR #1281
            assert len(errs[0]) > 0
        else:
            assert len(errs[0]) == 1
        err = errs[0][0]
        assert expected_error in err

    @since('3.4')
    def test_tracing_default_impl(self):
        """
        Test that Cassandra logs an error, but keeps its default tracing
        behavior, when the default tracing class is specified.

        This doesn't work because the constructor for the default
        implementation isn't accessible.

        * set the default tracing class as a custom tracing class
        * run trace()
        * if running the test on a version with custom tracing classes
          implemented, check that an error about the class was
          logged.

        @jira_ticket CASSANDRA-10392
        """
        expected_error = 'Cannot use class org.apache.cassandra.tracing.TracingImpl'
        self.fixture_dtest_setup.ignore_log_patterns = [expected_error]
        session = self.prepare(jvm_args=['-Dcassandra.custom_tracing_class=org.apache.cassandra.tracing.TracingImpl'])
        self.trace(session)

        errs = self.cluster.nodelist()[0].grep_log_for_errors()
        logger.debug('Errors after attempted trace with default tracing class: {errs}'.format(errs=errs))
        assert len(errs) == 1
        if self.cluster.version() >= LooseVersion('3.10'):
            # See CASSANDRA-11706 and PR #1281
            assert len(errs[0]) > 0
        else:
            assert len(errs[0]) == 1
        err = errs[0][0]
        assert expected_error in err
        # make sure it logged the error for the correct reason. this isn't
        # part of the expected error to avoid having to escape parens and
        # periods for regexes.

        if self.cluster.version() >= LooseVersion('3.10'):
            # See CASSANDRA-11706 and PR #1281
            check_for_errs_in = errs[0][1]
        else:
            check_for_errs_in = err
        assert "Default constructor for Tracing class 'org.apache.cassandra.tracing.TracingImpl' is inaccessible." \
               in check_for_errs_in
