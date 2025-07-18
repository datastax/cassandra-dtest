import conftest
import logging
import re
import os

from collections import namedtuple

import ccmlib.repository
from ccmlib.cluster import Cluster
from ccmlib.dse.dse_cluster import DseCluster
from ccmlib.hcd.hcd_cluster import HcdCluster
from ccmlib.common import get_version_from_build, get_jdk_version_int

from enum import Enum

logger = logging.getLogger(__name__)

# UpgradePath's contain data about upgrade paths we wish to test
# They also contain VersionMeta's for each version the path is testing
UpgradePath = namedtuple('UpgradePath', ('name', 'starting_version', 'upgrade_version', 'starting_meta', 'upgrade_meta'))

VERSION_FAMILY = None
CONFIG = None

# TODO add a new item whenever Cassandra is branched and update TRUNK to the version present in trunk
CASSANDRA_2_0 = '2.0'
CASSANDRA_2_1 = '2.1'
CASSANDRA_2_2 = '2.2'
CASSANDRA_3_0 = '3.0'
CASSANDRA_3_11 = '3.11'
CASSANDRA_4_0 = '4.0'
CASSANDRA_4_1 = '4.1'
CASSANDRA_5_0 = '5.0'
CASSANDRA_5_1 = '5.1'
TRUNK = CASSANDRA_5_1

# CC and DSE versions are prefixed with their C* version, as these are used in places
CC4 = '4.0.11-cc4' # update if CC4 rebases off a newer C* version
CC4_RE = re.compile(r'^4\.0\.\d+\.\d+$')
CC5 = '5.0.2-cc5' # update if CC5 rebases off a newer C* version
CC5_RE = re.compile(r'^5\.0\.\d+\.\d+$')
# TODO – these don't work yet – how will HCD*_RE detection distinguish from CC*_RE
HCD_1 = '4.0.11-hcd1' # update if CC4 rebases off a newer C* version
HCD_1_RE = re.compile(r'^4\.0\.\d+\.\d+$')
HCD_2 = '5.0.2-hcd2' # update if CC5 rebases off a newer C* version
HCD_2_RE = re.compile(r'^5\.0\.\d+\.\d+$')

DSE_5_1 = '3.11.3-DSE511'
DSE_5_1_RE = re.compile(r'^3\.11\.3\.51\d+$')
DSE_6_8 = '4.0.0-DSE68'
DSE_6_8_RE = re.compile(r'^4\.0\.0\.68\d+$')
DSE_6_9 = '4.0.0-DSE69'
DSE_6_9_RE = re.compile(r'^4\.0\.0\.69\d+$')


RUN_STATIC_UPGRADE_MATRIX = os.environ.get('RUN_STATIC_UPGRADE_MATRIX', '').lower() in ('yes', 'true')

def get_cluster_class(family):
    if family in (DSE_5_1, DSE_6_8, DSE_6_9):
        return DseCluster
    elif family in (HCD_1, HCD_2):
        return HcdCluster
    else:
        return Cluster

def is_same_family_current_to_indev(origin, destination):
    """
    Within a version family it is useful to test that a prior release can upgrade to the indev version
    """
    return origin.family == destination.family and origin.variant == "current" and destination.variant == "indev"


class VersionSelectionStrategies(Enum):
    """
    Allow all versions
    """
    ALL=(lambda origin, destination: True,)
    """
    Test upgrading from indev -> indev, current -> current across versions, and current -> indev within a version
    """
    BOTH=(lambda origin, destination: (origin.variant == destination.variant) or is_same_family_current_to_indev(origin,destination),)
    """
    Exclusively test in development branches so your bug fixes show up
    """
    INDEV=(lambda origin, destination: origin.variant == 'indev' and destination.variant == 'indev' or is_same_family_current_to_indev(origin, destination),)
    """
    Test upgrading from releases to the latest release as well as from the current release to the indev tip
    within the same version.
    """
    RELEASES=(lambda origin, destination: not VersionSelectionStrategies.INDEV.value[0](origin, destination) or is_same_family_current_to_indev(origin, destination),)


def set_config(config):
    global CONFIG
    CONFIG = config
    set_version_family()


def set_version_family():
    """
    Detects the version family (line) using dtest.py:CASSANDRA_VERSION_FROM_BUILD
    """
    # todo CASSANDRA-14421
    # current_version = CASSANDRA_VERSION_FROM_BUILD
    # There are times when we want to know the C* version we're testing against
    # before we call Tester.setUp. In the general case, we can't know that -- the
    # test method could use any version it wants for self.cluster. However, we can
    # get the version from build.xml in the C* repository specified by
    # CASSANDRA_VERSION or CASSANDRA_DIR. This should use the same resolution
    # strategy as the actual checkout code in Tester.setUp; if it does not, that is
    # a bug.
    cassandra_version_slug = CONFIG.getoption("--cassandra-version")
    cassandra_dir = CONFIG.getoption("--cassandra-dir") or CONFIG.getini("cassandra_dir")
    # Prefer CASSANDRA_VERSION if it's set in the environment. If not, use CASSANDRA_DIR
    if cassandra_version_slug:
        # fetch but don't build the specified C* version
        ccm_repo_cache_dir, _ = ccmlib.repository.setup(cassandra_version_slug)
        current_version = get_version_from_build(ccm_repo_cache_dir)
    else:
        current_version = get_version_from_build(cassandra_dir)

    # TODO add a new item whenever Cassandra is branched
    if DSE_5_1_RE.match(current_version.vstring):
        version_family = DSE_5_1
    elif DSE_6_8_RE.match(current_version.vstring):
        version_family = DSE_6_8
    elif DSE_6_9_RE.match(current_version.vstring):
        version_family = DSE_6_9
    elif CC4_RE.match(current_version.vstring):
        version_family = CC4
    elif CC5_RE.match(current_version.vstring):
        version_family = CC5
    elif current_version.vstring.startswith('2.0'):
        version_family = CASSANDRA_2_0
    elif current_version.vstring.startswith('2.1'):
        version_family = CASSANDRA_2_1
    elif current_version.vstring.startswith('2.2'):
        version_family = CASSANDRA_2_2
    elif current_version.vstring.startswith('3.0'):
        version_family = CASSANDRA_3_0
    elif current_version.vstring.startswith('3.11'):
        version_family = CASSANDRA_3_11
    elif current_version.vstring.startswith('4.0'):
        version_family = CASSANDRA_4_0
    elif current_version.vstring.startswith('4.1'):
        version_family = CASSANDRA_4_1
    elif current_version.vstring.startswith('5.0'):
        version_family = CASSANDRA_5_0
    elif current_version.vstring.startswith('5.1'):
        version_family = CASSANDRA_5_1
    else:
        # when this occurs, it's time to update this manifest a bit!
        raise RuntimeError("Testing upgrades from/to version %s is not supported. Please use a custom manifest (see upgrade_manifest.py)" % current_version.vstring)

    global VERSION_FAMILY
    VERSION_FAMILY = version_family
    logger.info("Setting version family to %s\n" % VERSION_FAMILY)


class VersionMeta(namedtuple('_VersionMeta', ('name', 'family', 'variant', 'version', 'min_proto_v', 'max_proto_v', 'java_versions'))):
    """
    VersionMeta's are namedtuples that capture data about version family, protocols supported, and current version identifiers
    they must have a 'variant' value of 'current' or 'indev', where 'current' means most recent released version,
    'indev' means where changing code is found.
    """
    @property
    def java_version(self):
        return max(self.java_versions)

    @property
    def matches_current_env_version_family(self):
        """
        Returns boolean indicating whether this meta matches the current version family of the environment.

        e.g. Returns true if the current env version family is 3.x and the meta's family attribute is a match.
        """
        return self.family == VERSION_FAMILY

    @property
    def matches_current_env_version_family_and_is_indev(self):
        """
        Returns boolean indicating whether this meta matches the current version family of the environment
        and whether this meta is in indev variant
        """
        return self.family == VERSION_FAMILY and self.variant == "indev"

    def clone_with_local_env_version(self):
        """
        Returns a new object cloned from this one, with the version replaced with the local env version.
        """
        cassandra_dir, cassandra_version = conftest.cassandra_dir_and_version(CONFIG)
        if cassandra_version:
            return self._replace(version=cassandra_version)
        return self._replace(version="clone:{}".format(cassandra_dir))


# TODO define new versions whenever Cassandra is branched

indev_2_1_x = VersionMeta(name='indev_2_1_x', family=CASSANDRA_2_1, variant='indev', version='github:apache/cassandra-2.1', min_proto_v=1, max_proto_v=3, java_versions=(7, 8))
current_2_1_x = VersionMeta(name='current_2_1_x', family=CASSANDRA_2_1, variant='current', version='2.1.22', min_proto_v=1, max_proto_v=3, java_versions=(7, 8))

indev_2_2_x = VersionMeta(name='indev_2_2_x', family=CASSANDRA_2_2, variant='indev', version='github:apache/cassandra-2.2', min_proto_v=1, max_proto_v=3, java_versions=(7, 8))
current_2_2_x = VersionMeta(name='current_2_2_x', family=CASSANDRA_2_2, variant='current', version='2.2.19', min_proto_v=1, max_proto_v=3, java_versions=(7, 8))

indev_3_0_x = VersionMeta(name='indev_3_0_x', family=CASSANDRA_3_0, variant='indev', version='github:apache/cassandra-3.0', min_proto_v=3, max_proto_v=4, java_versions=(8,))
current_3_0_x = VersionMeta(name='current_3_0_x', family=CASSANDRA_3_0, variant='current', version='3.0.32', min_proto_v=3, max_proto_v=4, java_versions=(8,))

indev_3_11_x = VersionMeta(name='indev_3_11_x', family=CASSANDRA_3_11, variant='indev', version='github:apache/cassandra-3.11', min_proto_v=3, max_proto_v=4, java_versions=(8,))
current_3_11_x = VersionMeta(name='current_3_11_x', family=CASSANDRA_3_11, variant='current', version='3.11.19', min_proto_v=3, max_proto_v=4, java_versions=(8,))

indev_dse_5_1 = VersionMeta(name='indev_dse_5_1', family=DSE_5_1, variant='indev', version='alias:bdp/5.1-dev', min_proto_v=3, max_proto_v=4, java_versions=(8,)) # FIXME also support proto_v=65 ("dse-v1")
current_dse_5_1 = VersionMeta(name='current_dse_5_1', family=DSE_5_1, variant='current', version='5.1.48', min_proto_v=3, max_proto_v=4, java_versions=(8,)) # FIXME also support proto_v=65 ("dse-v1")

indev_4_0_x = VersionMeta(name='indev_4_0_x', family=CASSANDRA_4_0, variant='indev', version='github:apache/cassandra-4.0', min_proto_v=3, max_proto_v=5, java_versions=(8,11))
current_4_0_x = VersionMeta(name='current_4_0_x', family=CASSANDRA_4_0, variant='current', version='4.0.17', min_proto_v=4, max_proto_v=5, java_versions=(8,11))

indev_dse_6_8 = VersionMeta(name='indev_dse_6_8', family=DSE_6_8, variant='indev', version='alias:bdp/6.8-dev', min_proto_v=3, max_proto_v=4, java_versions=(8,)) # FIXME also support proto_v=65+66 ("dse-v1" and "dse-v2")
current_dse_6_8 = VersionMeta(name='current_dse_6_8', family=DSE_6_8, variant='current', version='6.8.59', min_proto_v=3, max_proto_v=4, java_versions=(8,)) # FIXME also support proto_v=65+66 ("dse-v1" and "dse-v2")
indev_dse_6_9 = VersionMeta(name='indev_dse_6_9', family=DSE_6_8, variant='indev', version='alias:bdp/6.9-dev', min_proto_v=3, max_proto_v=5, java_versions=(11,)) # FIXME also support proto_v=65+66 ("dse-v1" and "dse-v2")
current_dse_6_9 = VersionMeta(name='current_dse_6_9', family=DSE_6_8, variant='current', version='6.9.12', min_proto_v=3, max_proto_v=5, java_versions=(11,)) # FIXME also support proto_v=65+66 ("dse-v1" and "dse-v2")

indev_cc4 = VersionMeta(name='indev_cc4', family=CC4, variant='indev', version='github:datastax/main', min_proto_v=3, max_proto_v=4, java_versions=(11,))
# TODO - HCD-128
#indev_hcd_1_2 = VersionMeta(name='current_hcd_1_2', family=HCD_1_2, variant='indev', version='xxx', min_proto_v=4, max_proto_v=5, java_versions=(11))

indev_cc5 = VersionMeta(name='indev_cc5', family=CC5, variant='indev', version='github:datastax/main-5.0', min_proto_v=4, max_proto_v=5, java_versions=(11,17,22))
# TODO - HCD-128
#indev_hcd_2_0 = VersionMeta(name='current_hcd_2_0', family=HCD_2_0, variant='indev', version='xxx', min_proto_v=4, max_proto_v=5, java_versions=(11,17,22))

indev_4_1_x = VersionMeta(name='indev_4_1_x', family=CASSANDRA_4_1, variant='indev', version='github:apache/cassandra-4.1', min_proto_v=4, max_proto_v=5, java_versions=(8,11))
current_4_1_x = VersionMeta(name='current_4_1_x', family=CASSANDRA_4_1, variant='current', version='4.1.9', min_proto_v=4, max_proto_v=5, java_versions=(8,11))

indev_5_0_x = VersionMeta(name='indev_5_0_x', family=CASSANDRA_5_0, variant='indev', version='github:apache/cassandra-5.0', min_proto_v=4, max_proto_v=5, java_versions=(11,17))
current_5_0_x = VersionMeta(name='current_5_0_x', family=CASSANDRA_5_0, variant='current', version='5.0.4', min_proto_v=4, max_proto_v=5, java_versions=(11,17))

indev_trunk = VersionMeta(name='indev_trunk', family=TRUNK, variant='indev', version='github:apache/trunk', min_proto_v=4, max_proto_v=5, java_versions=(11,17))
# TODO – add current_5_1_x when this gets uncommented (when 5.1-alpha1 is released)
# current_5_1_x = VersionMeta(name='current_5_1_x', family=CASSANDRA_5_1, variant='current', version='5.1-alpha1', min_proto_v=4, max_proto_v=5, java_versions=(11,17))


# MANIFEST maps a VersionMeta representing a line/variant to a list of other VersionMeta's representing supported upgrades
# Note on versions: 2.0 must upgrade to 2.1. Once at 2.1 or newer, upgrade is supported to any later version, including trunk (for now).
# "supported upgrade" means a few basic things, for an upgrade of version 'A' to higher version 'B':
#   1) The cluster will function in a mixed-version state, with some nodes on version A and some nodes on version B. Schema modifications are not supported on mixed-version clusters.
#   2) Features exclusive to version B may not work until all nodes are running version B.
#   3) Nodes upgraded to version B can read data stored by the predecessor version A, and from a data standpoint will function the same as if they always ran version B.
#   4) If a new sstable format is present in version B, writes will occur in that format after upgrade. Running sstableupgrade on version B will proactively convert version A sstables to version B.
# TODO define new upgrade scenarios whenever Cassandra is branched
MANIFEST = {
    current_2_1_x: [indev_2_2_x, indev_3_0_x, indev_3_11_x],
    current_2_2_x: [indev_2_2_x, indev_3_0_x, indev_3_11_x],
    current_3_0_x: [indev_3_0_x, indev_3_11_x, indev_4_0_x, indev_cc4, indev_4_1_x],
    current_3_11_x: [indev_3_11_x, indev_4_0_x, indev_cc4, indev_4_1_x],
    current_4_0_x:  [indev_4_0_x, indev_cc4, indev_4_1_x, indev_5_0_x, indev_cc5, indev_trunk],
    current_4_1_x:  [indev_4_1_x, indev_5_0_x, indev_cc5, indev_trunk],
    current_5_0_x:  [indev_5_0_x, indev_cc5, indev_trunk],

    indev_2_1_x: [indev_2_2_x, indev_3_0_x, indev_3_11_x],
    indev_2_2_x: [indev_3_0_x, indev_3_11_x],
    indev_3_0_x: [indev_3_11_x, indev_4_0_x, indev_cc4, indev_4_1_x],
    indev_3_11_x: [indev_4_0_x, indev_cc4, indev_4_1_x],
    indev_4_0_x:  [indev_cc4, indev_4_1_x, indev_5_0_x, indev_cc5, indev_trunk],
    indev_4_1_x:  [indev_5_0_x, indev_cc5, indev_trunk],
    indev_5_0_x:  [indev_cc5, indev_trunk],

    #indev_dse_5_1: [indev_cc4], # FIXME HCD-149
    current_dse_5_1: [indev_cc4],
    #indev_dse_6_8: [indev_cc4, indev_cc5], # FIXME HCD-149
    current_dse_6_8: [indev_cc4, indev_cc5],
    #indev_dse_6_9: [indev_cc4, indev_cc5], # FIXME HCD-149
    current_dse_6_9: [indev_cc4, indev_cc5]
    # TODO these below will need to be added to above, where applicable
    #indev_hcd_1_2: [indev_cc4, indev_cc5, current_hcd_1_2, indev_hcd_2_0]
    #current_hcd_1_2: [indev_cc4, indev_hcd_1_2, indev_cc5, current_hcd_1_2, indev_hcd_2_0]
    #indev_hcd_2_0: [indev_cc5]
    #current_hcd_2_0: [indev_cc5, indev_hcd_2_0]
}

def _have_common_proto(origin_meta, destination_meta):
    """
    Takes two VersionMeta objects, in order of test from start version to next version.
    Returns a boolean indicating if the given VersionMetas have a common protocol version.
    """
    return origin_meta.max_proto_v >= destination_meta.min_proto_v

def current_env_java_version():
    # $JAVA_HOME/bin/java takes precedence over any java found in $PATH
    if 'JAVA_HOME' in os.environ:
        java_command = os.path.join(os.environ['JAVA_HOME'], 'bin', 'java')
    else:
        java_command = 'java'

    return get_jdk_version_int(java_command)

CURRENT_JAVA_VERSION = current_env_java_version()

def jdk_compatible_steps(version_metas):
    metas = []
    included_current_jdk_versions = []
    included_java_xx_home_versions = []
    for version_meta in version_metas:
        # if you want multi-step upgrades to work with versions that require different jdks
        #   then define the JAVA<jdk_version>_HOME vars (e.g. JAVA8_HOME)
        #   ccm detects these variables and changes the jdk when starting/upgrading the node
        # otherwise the default behaviour is to only do upgrade steps that work with the current jdk
        javan_home_defined = False
        for meta_java_version in version_meta.java_versions:
            javan_home_defined |= 'JAVA{}_HOME'.format(meta_java_version) in os.environ
        if CURRENT_JAVA_VERSION in version_meta.java_versions or javan_home_defined:
            metas.append(version_meta)
            if CURRENT_JAVA_VERSION in version_meta.java_versions:
                included_current_jdk_versions.append(version_meta.version)
            else:
                included_java_xx_home_versions.append(version_meta.version)
        else:
            logger.info("Skipping version {} because it requires JDK {}. Current JDK is {} and none of {} env variables are defined."
                        .format(version_meta.version, version_meta.java_versions, CURRENT_JAVA_VERSION, ["JAVA{}_HOME".format(v) for v in version_meta.java_versions]))

    logger.info("JDK compatible steps for {} compatible with current JDK {} or JAVAxx_HOME {}"
                .format([m.version for m in version_metas], CURRENT_JAVA_VERSION, [v for v in included_current_jdk_versions + included_java_xx_home_versions]))
    return metas

def build_upgrade_pairs():
    """
    Using the manifest (above), builds a set of valid upgrades, according to current testing practices.

    Returns a list of UpgradePath's.
    """
    valid_upgrade_pairs = []
    manifest = MANIFEST

    configured_strategy = CONFIG.getoption("--upgrade-version-selection").upper()
    version_select_strategy = VersionSelectionStrategies[configured_strategy].value[0]
    filter_for_current_family = CONFIG.getoption("--upgrade-target-version-only")

    for origin_meta, destination_metas in list(manifest.items()):
        for destination_meta in destination_metas:
            if not version_select_strategy(origin_meta, destination_meta):
                continue

            if not (origin_meta and destination_meta):  # None means we don't care about that version, which means we don't care about iterations involving it either
                logger.debug("skipping class creation as a version is undefined (this is normal), versions: {} and {}".format(origin_meta, destination_meta))
                continue

            if not _have_common_proto(origin_meta, destination_meta):
                logger.debug("skipping class creation, no compatible protocol version between {} and {}".format(origin_meta.name, destination_meta.name))
                continue

            # if either origin or destination match version, then do the test
            # the assumption is that a change in 3.0 could break upgrades to trunk, so include those tests as well
            if filter_for_current_family and not origin_meta.matches_current_env_version_family_and_is_indev and not destination_meta.matches_current_env_version_family:
                logger.debug("skipping class creation, origin version {} and destination version {} do not match target version {}, and --upgrade-target-version-only was set".format(origin_meta.name, destination_meta.name, VERSION_FAMILY))
                continue

            path_name = 'Upgrade_' + origin_meta.name + '_To_' + destination_meta.name

            if not RUN_STATIC_UPGRADE_MATRIX:
                if destination_meta.matches_current_env_version_family:
                    # looks like this test should actually run in the current env, so let's set the final version to match the env exactly
                    oldmeta = destination_meta
                    newmeta = destination_meta.clone_with_local_env_version()
                    logger.debug("{} appears applicable to current env. Overriding final test version from {} to {}".format(path_name, oldmeta.version, newmeta.version))
                    destination_meta = newmeta

            if len(jdk_compatible_steps([origin_meta, destination_meta])) > 1:
                valid_upgrade_pairs.append(
                    UpgradePath(
                        name=path_name,
                        starting_version=origin_meta.version,
                        upgrade_version=destination_meta.version,
                        starting_meta=origin_meta,
                        upgrade_meta=destination_meta
                    )
                )

    return valid_upgrade_pairs
