import configparser
import copy
import logging
import os
import re
import subprocess
import sys
import threading
import time
import traceback
from distutils.version import LooseVersion

import pytest
import cassandra

from subprocess import CalledProcessError

from flaky import flaky

from cassandra import ConsistencyLevel, OperationTimedOut
from cassandra.auth import PlainTextAuthProvider
from cassandra.cluster import ExecutionProfile
from cassandra.policies import RetryPolicy, RoundRobinPolicy
from ccmlib.common import get_version_from_build
from ccmlib.node import ToolError, TimeoutError
from tools.misc import retry_till_success

from upgrade_tests.upgrade_manifest import build_upgrade_pairs


LOG_SAVED_DIR = "logs"
try:
    os.mkdir(LOG_SAVED_DIR)
except OSError:
    pass

LAST_LOG = os.path.join(LOG_SAVED_DIR, "last")

LAST_TEST_DIR = 'last_test_dir'

BYTEMAN_DIR = os.path.join(os.path.dirname(__file__), 'byteman')

DEFAULT_DIR = './'
config = configparser.RawConfigParser()
if len(config.read(os.path.expanduser('~/.cassandra-dtest'))) > 0:
    if config.has_option('main', 'default_dir'):
        DEFAULT_DIR = os.path.expanduser(config.get('main', 'default_dir'))

MAJOR_VERSION_4 = LooseVersion('4.0')
MAJOR_VERSION_5 = LooseVersion('5.0')

logger = logging.getLogger(__name__)


def get_sha(repo_dir):
    try:
        output = subprocess.check_output(['git', 'rev-parse', 'HEAD'], cwd=repo_dir).strip()
        prefix = 'github:apache/'
        local_repo_location = os.environ.get('LOCAL_GIT_REPO')
        if local_repo_location is not None:
            prefix = 'local:{}:'.format(local_repo_location)
            # local: slugs take the form 'local:/some/path/to/cassandra/:branch_name_or_sha'
        return "{}{}".format(prefix, output)
    except CalledProcessError as e:
        if re.search(str(e), 'Not a git repository') is not None:
            # we tried to get a sha, but repo_dir isn't a git repo. No big deal, must just be
            # working from a non-git install.
            return None
        else:
            # git call failed for some unknown reason
            raise

def mk_bman_path(path):
    return os.path.join(BYTEMAN_DIR, path)


# copy the initial environment variables so we can reset them later:
initial_environment = copy.deepcopy(os.environ)


class DtestTimeoutError(Exception):
    pass


logger.debug("Python driver version in use: {}".format(cassandra.__version__))


class FlakyRetryPolicy(RetryPolicy):
    """
    A retry policy that retries 5 times by default, but can be configured to
    retry more times.
    """

    def __init__(self, max_retries=5):
        self.max_retries = max_retries

    def on_read_timeout(self, *args, **kwargs):
        if kwargs['retry_num'] < self.max_retries:
            logger.debug("Retrying read after timeout. Attempt #" + str(kwargs['retry_num']))
            return (self.RETRY, None)
        else:
            return (self.RETHROW, None)

    def on_write_timeout(self, *args, **kwargs):
        if kwargs['retry_num'] < self.max_retries:
            logger.debug("Retrying write after timeout. Attempt #" + str(kwargs['retry_num']))
            return (self.RETRY, None)
        else:
            return (self.RETHROW, None)

    def on_unavailable(self, *args, **kwargs):
        if kwargs['retry_num'] < self.max_retries:
            logger.debug("Retrying request after UE. Attempt #" + str(kwargs['retry_num']))
            return (self.RETRY, None)
        else:
            return (self.RETHROW, None)


class Runner(threading.Thread):

    def __init__(self, func):
        threading.Thread.__init__(self)
        self.__func = func
        self.__error = None
        self.__stopped = False
        self.daemon = True

    def run(self):
        i = 0
        while True:
            if self.__stopped:
                return
            try:
                self.__func(i)
            except Exception as e:
                self.__error = e
                return
            i = i + 1

    def stop(self):
        if self.__stopped:
            return

        self.__stopped = True
        # pytests may appear to hang forever waiting for cluster tear down. are all driver session objects shutdown?
        # to debug hang you can add the following at the top of the test
        #     import faulthandler
        #     faulthandler.enable()
        #
        # and then when the hang occurs send a SIGABRT to the pytest process (e.g. kill -SIGABRT <pytest_pid>)
        # this will print a python thread dump of all currently alive threads
        self.join(timeout=30)
        if self.__error is not None:
            raise self.__error

    def check(self):
        if self.__error is not None:
            raise self.__error


def make_execution_profile(retry_policy=FlakyRetryPolicy(), consistency_level=ConsistencyLevel.ONE, **kwargs):
    if 'load_balancing_policy' in kwargs:
        return ExecutionProfile(retry_policy=retry_policy,
                                consistency_level=consistency_level,
                                **kwargs)
    else:
        return ExecutionProfile(retry_policy=retry_policy,
                                consistency_level=consistency_level,
                                load_balancing_policy=RoundRobinPolicy(),
                                **kwargs)


def running_in_docker():
    return os.path.isfile('/.dockerenv')


def cleanup_docker_environment_before_test_execution():
    """
    perform a bunch of system cleanup operations, like kill any instances that might be
    hanging around incorrectly from a previous run, sync the disk, and clear swap.
    Ideally we would also drop the page cache, but as docker isn't running in privileged
    mode there is no way for us to do this.
    """
    # attempt to wack all existing running Cassandra processes forcefully to get us into a clean state
    p_kill = subprocess.Popen('ps aux | grep -ie CassandraDaemon | grep java | grep -v grep | awk \'{print $2}\' | xargs kill -9',
                              stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, shell=True)
    p_kill.wait(timeout=10)

    # explicitly call "sync" to flush everything that might be pending from a previous test
    # so tests are less likely to hit a very slow fsync during the test by starting from a 'known' state
    # note: to mitigate this further the docker image is mounting /tmp as a volume, which gives
    # us an ext4 mount which should talk directly to the underlying device on the host, skipping
    # the aufs pain that we get with anything else running in the docker image. Originally,
    # I had a timeout of 120 seconds (2 minutes), 300 seconds (5 minutes) but sync was still occasionally timing out.
    p_sync = subprocess.Popen('sudo /bin/sync', shell=True)
    p_sync.wait(timeout=600)

    # turn swap off and back on to make sure it's fully cleared if anything happened to swap
    # from a previous test run
    p_swap = subprocess.Popen('sudo /sbin/swapoff -a && sudo /sbin/swapon -a', shell=True)
    p_swap.wait(timeout=60)


def test_failure_due_to_timeout(err, *args):
    """
    check if we should rerun a test with the flaky plugin or not.
    for now, only run if we failed the test for one of the following
    three exceptions: cassandra.OperationTimedOut, ccm.node.ToolError,
    and ccm.node.TimeoutError.

    - cassandra.OperationTimedOut will be thrown when a cql query made thru
    the python-driver times out.
    - ccm.node.ToolError will be thrown when an invocation of a "tool"
    (in the case of dtests this will almost always invoking stress).
    - ccm.node.TimeoutError will be thrown when a blocking ccm operation
    on a individual node times out. In most cases this tends to be something
    like watch_log_for hitting the timeout before the desired pattern is seen
    in the node's logs.

    if we failed for one of these reasons - and we're running in docker - run
    the same "cleanup" logic we run before test execution and test setup begins
    and for good measure introduce a 2 second sleep. why 2 seconds? because it's
    magic :) - ideally this gets the environment back into a good state and makes
    the rerun of flaky tests likely to suceed if they failed in the first place
    due to environmental issues.
    """
    if issubclass(err[0], OperationTimedOut) or issubclass(err[0], ToolError) or issubclass(err[0], TimeoutError):
        if running_in_docker():
            cleanup_docker_environment_before_test_execution()
            time.sleep(2)
        return True
    else:
        return False


@flaky(rerun_filter=test_failure_due_to_timeout)
class Tester(object):

    def __getattribute__(self, name):
        try:
            return object.__getattribute__(self, name)
        except AttributeError:
            fixture_dtest_setup = object.__getattribute__(self, 'fixture_dtest_setup')
            return object.__getattribute__(fixture_dtest_setup , name)

    @pytest.fixture(scope='function', autouse=True)
    def set_dtest_setup_on_function(self, fixture_dtest_setup):
        self.fixture_dtest_setup = fixture_dtest_setup
        self.dtest_config = fixture_dtest_setup.dtest_config
        return None

    @pytest.fixture(autouse=True)
    def cleanup_connections(self):
        yield None
        logger.info('Cleaning up CQL Connections...')
        self.fixture_dtest_setup.cleanup_connections()

    def assert_supported_upgrade_path(self, from_version, to_version):
        for path in build_upgrade_pairs():
            if from_version.startswith(path.starting_meta.family) and to_version.startswith(path.upgrade_meta.family):
                return None
        pytest.fail("Upgrades from {} to {} are not supported and should not be tested".format(from_version, to_version))

    def set_node_to_current_version(self, node):
        version = os.environ.get('CASSANDRA_VERSION')

        if version:
            self.assert_supported_upgrade_path(node.get_cassandra_version().vstring, version)
            node.set_install_dir(version=version)
        else:
            self.assert_supported_upgrade_path(node.get_cassandra_version().vstring, get_version_from_build(self.dtest_config.cassandra_dir).vstring)
            node.set_install_dir(install_dir=self.dtest_config.cassandra_dir)
            os.environ['CASSANDRA_DIR'] = self.dtest_config.cassandra_dir

    def go(self, func):
        runner = Runner(func)
        self.runners.append(runner)
        runner.start()
        return runner

    def assert_log_had_msg(self, node, msg, timeout=600, **kwargs):
        """
        Wrapper for ccmlib.node.Node#watch_log_for to cause an assertion failure when a log message isn't found
        within the timeout.
        :param node: Node which logs we should watch
        :param msg: String message we expect to see in the logs.
        :param timeout: Seconds to wait for msg to appear
        """
        try:
            node.watch_log_for(msg, timeout=timeout, **kwargs)
        except TimeoutError:
            pytest.fail("Log message was not seen within timeout:\n{0}".format(msg))


def get_eager_protocol_version(cassandra_version):
    """
    Returns the highest protocol version accepted
    by the given C* version
    """
    if LooseVersion('4.0') <= cassandra_version:
        protocol_version = 5
    elif LooseVersion('3.0') <= cassandra_version:
        protocol_version = 4
    elif LooseVersion('2.1') <= cassandra_version:
        protocol_version = 3
    elif LooseVersion('2.0') <= cassandra_version:
        protocol_version = 2
    else:
        protocol_version = 1
    return protocol_version


# We default to UTF8Type because it's simpler to use in tests
def create_cf(session, name, key_type="varchar", speculative_retry=None, read_repair=None, compression=None,
              gc_grace=None, columns=None, validation="UTF8Type", compact_storage=False, compaction_strategy='SizeTieredCompactionStrategy',
              primary_key=None, clustering=None, legacy_compression_class = False):

    compaction_fragment = "compaction = {'class': '%s', 'enabled': 'true'}"
    if compaction_strategy == '':
        compaction_fragment = compaction_fragment % 'SizeTieredCompactionStrategy'
    else:
        compaction_fragment = compaction_fragment % compaction_strategy

    additional_columns = ""
    if columns is not None:
        for k, v in list(columns.items()):
            additional_columns = "{}, {} {}".format(additional_columns, k, v)

    if additional_columns == "":
        query = 'CREATE COLUMNFAMILY %s (key %s, c varchar, v varchar, PRIMARY KEY(key, c)) WITH comment=\'test cf\'' % (name, key_type)
    else:
        if primary_key:
            query = 'CREATE COLUMNFAMILY %s (key %s%s, PRIMARY KEY(%s)) WITH comment=\'test cf\'' % (name, key_type, additional_columns, primary_key)
        else:
            query = 'CREATE COLUMNFAMILY %s (key %s PRIMARY KEY%s) WITH comment=\'test cf\'' % (name, key_type, additional_columns)

    if compaction_fragment is not None:
        query = '%s AND %s' % (query, compaction_fragment)

    if clustering:
        query = '%s AND CLUSTERING ORDER BY (%s)' % (query, clustering)

    if compression is not None:
        if legacy_compression_class:
            query = '%s AND compression = { \'sstable_compression\': \'%sCompressor\' }' % (query, compression)
        else:
            query = '%s AND compression = { \'class\': \'%sCompressor\' }' % (query, compression)
    else:
        # if a compression option is omitted, C* will default to lz4 compression
        query += ' AND compression = {}'

    if read_repair is not None:
        query = '%s AND read_repair_chance=%f AND dclocal_read_repair_chance=%f' % (query, read_repair, read_repair)
    if gc_grace is not None:
        query = '%s AND gc_grace_seconds=%d' % (query, gc_grace)
    if speculative_retry is not None:
        query = '%s AND speculative_retry=\'%s\'' % (query, speculative_retry)

    if compact_storage:
        query += ' AND COMPACT STORAGE'

    try:
        retry_till_success(session.execute, query=query, timeout=120, bypassed_exception=cassandra.OperationTimedOut)
    except cassandra.AlreadyExists:
        logger.warn('AlreadyExists executing create cf query \'%s\'' % query)
    session.cluster.control_connection.wait_for_schema_agreement(wait_time=120)
    #Going to ignore OperationTimedOut from create CF, so need to validate it was indeed created
    session.execute('SELECT * FROM %s LIMIT 1' % name);


def create_cf_simple(session, name, query):
    try:
        retry_till_success(session.execute, query=query, timeout=120, bypassed_exception=cassandra.OperationTimedOut)
    except cassandra.AlreadyExists:
        logger.warn('AlreadyExists executing create cf query \'%s\'' % query)
    session.cluster.control_connection.wait_for_schema_agreement(wait_time=120)
    #Going to ignore OperationTimedOut from create CF, so need to validate it was indeed created
    session.execute('SELECT * FROM %s LIMIT 1' % name)


def create_ks(session, name, rf):
    query = 'CREATE KEYSPACE %s WITH replication={%s}'
    if isinstance(rf, int):
        # we assume simpleStrategy
        query = query % (name, "'class':'SimpleStrategy', 'replication_factor':%d" % rf)
    elif 'EverywhereStrategy' in rf:
        query = query % (name, "'class':'org.apache.cassandra.locator.EverywhereStrategy'")
    else:
        assert len(rf) >= 0, "At least one datacenter/rf pair is needed"
        # we assume networkTopologyStrategy
        options = (', ').join(['\'%s\':%d' % (d, r) for d, r in rf.items()])
        query = query % (name, "'class':'NetworkTopologyStrategy', %s" % options)

    try:
        retry_till_success(session.execute, query=query, timeout=120, bypassed_exception=cassandra.OperationTimedOut)
    except cassandra.AlreadyExists:
        logger.warn('AlreadyExists executing create ks query \'%s\'' % query)

    session.cluster.control_connection.wait_for_schema_agreement(wait_time=120)
    #Also validates it was indeed created even though we ignored OperationTimedOut
    #Might happen some of the time because CircleCI disk IO is unreliable and hangs randomly
    session.execute('USE {}'.format(name))


def get_auth_provider(user, password):
    return PlainTextAuthProvider(username=user, password=password)


def make_auth(user, password):
    def private_auth(node_ip):
        return {'username': user, 'password': password}
    return private_auth


def data_size(node, ks, cf):
    """
    Return the size in bytes for given table in a node.
    This gets the size from nodetool tablestats output.
    This might break if the format of nodetool tablestats change
    as it is looking for specific text "Space used (total)" in output.
    @param node: Node in which table size to be checked for
    @param ks: Keyspace name for the table
    @param cf: table name
    @return: data size in bytes
    """
    hack_legacy_parsing(node)
    tablestats = node.nodetool("tablestats {}.{}".format(ks,cf))[0]
    regex = re.compile(r'[\t]')
    stats_lines = [regex.sub("", s) for s in tablestats.split('\n')
                  if regex.sub("", s).startswith('Space used (total)')]
    if len(stats_lines) > 2:
        msg = ('Expected output from `nodetool tablestats` to contain at most 2 '
               'lines starting with "Space used (total)". Found:\n') + tablestats
        raise RuntimeError(msg)

    space_used_line = stats_lines[0].split() if len(stats_lines) == 1 else stats_lines[1].split()

    if len(space_used_line) == 4:
        return float(space_used_line[3])
    else:
        msg = ('Expected format for `Space used (total)` in nodetool tablestats is `Space used (total): <number>`.'
               'Found:\n') + stats_lines[0]
        raise RuntimeError(msg)


def get_port_from_node(node):
    """
    Return the port that this node is listening on.
    We only use this to connect the native driver,
    so we only care about the binary port.
    """
    try:
        return node.network_interfaces['binary'][1]
    except Exception:
        raise RuntimeError("No network interface defined on this node object. {}".format(node.network_interfaces))


def get_ip_from_node(node):
    if node.network_interfaces['binary']:
        node_ip = node.network_interfaces['binary'][0]
    else:
        node_ip = node.network_interfaces['thrift'][0]
    return node_ip


def kill_windows_cassandra_procs():
    # On Windows, forcefully terminate any leftover previously running cassandra processes. This is a temporary
    # workaround until we can determine the cause of intermittent hung-open tests and file-handles.
    if is_win():
        try:
            import psutil
            for proc in psutil.process_iter():
                try:
                    pinfo = proc.as_dict(attrs=['pid', 'name', 'cmdline'])
                except psutil.NoSuchProcess:
                    pass
                else:
                    if (pinfo['name'] == 'java.exe' and '-Dcassandra' in pinfo['cmdline']):
                        print('Found running cassandra process with pid: ' + str(pinfo['pid']) + '. Killing.')
                        psutil.Process(pinfo['pid']).kill()
        except ImportError:
            logger.debug("WARN: psutil not installed. Cannot detect and kill "
                  "running cassandra processes - you may see cascading dtest failures.")


class MultiError(Exception):
    """
    Extends Exception to provide reporting multiple exceptions at once.
    """

    def __init__(self, exceptions, tracebacks):
        # an exception and the corresponding traceback should be found at the same
        # position in their respective lists, otherwise __str__ will be incorrect
        self.exceptions = exceptions
        self.tracebacks = tracebacks

    def __str__(self):
        output = "\n****************************** BEGIN MultiError ******************************\n"

        for (exc, tb) in zip(self.exceptions, self.tracebacks):
            output += str(exc)
            output += str(tb) + "\n"

        output += "****************************** END MultiError ******************************"

        return output


def run_scenarios(scenarios, handler, deferred_exceptions=tuple()):
    """
    Runs multiple scenarios from within a single test method.

    "Scenarios" are mini-tests where a common procedure can be reused with several different configurations.
    They are intended for situations where complex/expensive setup isn't required and some shared state is acceptable (or trivial to reset).

    Arguments: scenarios should be an iterable, handler should be a callable, and deferred_exceptions should be a tuple of exceptions which
    are safe to delay until the scenarios are all run. For each item in scenarios, handler(item) will be called in turn.

    Exceptions which occur will be bundled up and raised as a single MultiError exception, either when: a) all scenarios have run,
    or b) on the first exception encountered which is not whitelisted in deferred_exceptions.
    """
    errors = []
    tracebacks = []

    for i, scenario in enumerate(scenarios, 1):
        logger.debug("running scenario {}/{}: {}".format(i, len(scenarios), scenario))

        try:
            handler(scenario)
        except deferred_exceptions as e:
            tracebacks.append(traceback.format_exc())
            errors.append(type(e)('encountered {} {} running scenario:\n  {}\n'.format(e.__class__.__name__, str(e), scenario)))
            logger.debug("scenario {}/{} encountered a deferrable exception, continuing".format(i, len(scenarios)))
        except Exception as e:
            # catch-all for any exceptions not intended to be deferred
            tracebacks.append(traceback.format_exc())
            errors.append(type(e)('encountered {} {} running scenario:\n  {}\n'.format(e.__class__.__name__, str(e), scenario)))
            logger.debug("scenario {}/{} encountered a non-deferrable exception, aborting".format(i, len(scenarios)))
            raise MultiError(errors, tracebacks)

    if errors:
        raise MultiError(errors, tracebacks)

def hack_legacy_parsing(node):
    """ Hack node's shell script for nodetool legacy URL parsing, ala CASSANDRA-17581 """
    if hasattr(node, 'get_install_dir'):
        nodetool = os.path.join(node.get_install_dir(), 'bin', 'nodetool')
    else:
        # assume raw path
        nodetool = node
    with open(nodetool, 'r+') as fd:
        contents = fd.readlines()
        if "legacy" in contents[len(contents)-6]:
            logger.debug("nodetool already hacked")
        elif not contents[len(contents)-5].endswith('\\\n'):
            logger.debug("version does not appear to need hacking")
        else:
            contents.insert(len(contents)-5, "      -Dcom.sun.jndi.rmiURLParsing=legacy \\\n")
            fd.seek(0)
            fd.writelines(contents)

