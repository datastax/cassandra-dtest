from unittest import TestCase

from conftest import is_skippable
from mock import Mock

class DTestConfigMock():
    def __init__(self):
        self.execute_upgrade_tests = False
        self.execute_upgrade_tests_only = False
        self.force_execution_of_resource_intensive_tests = False
        self.only_resource_intensive_tests = False
        self.skip_resource_intensive_tests = False
        self.use_vnodes = False
        self.use_off_heap_memtables = False

def _mock_responses(responses, default_response=None):
    return lambda arg: responses[arg] if arg in responses else default_response

class ConfTestTest(TestCase):
    regular_test = Mock(name="regular_test_mock")
    upgrade_test = Mock(name="upgrade_test_mock")
    resource_intensive_test = Mock(name="resource_intensive_test_mock")
    vnodes_test = Mock(name="vnodes_test_mock")
    no_vnodes_test = Mock(name="no_vnodes_test_mock")
    no_offheap_memtables_test = Mock(name="no_offheap_memtables_test_mock")
    depends_driver_test = Mock(name="depends_driver_test_mock")

    def setup_method(self, method):
        self.regular_test.get_closest_marker.side_effect = _mock_responses({})
        self.upgrade_test.get_closest_marker.side_effect = _mock_responses({"upgrade_test": True})
        self.resource_intensive_test.get_closest_marker.side_effect = _mock_responses({"resource_intensive": True})
        self.vnodes_test.get_closest_marker.side_effect = _mock_responses({"vnodes": True})
        self.no_vnodes_test.get_closest_marker.side_effect = _mock_responses({"no_vnodes": True})
        self.no_offheap_memtables_test.get_closest_marker.side_effect = _mock_responses({"no_offheap_memtables": True})
        self.depends_driver_test.get_closest_marker.side_effect = _mock_responses({"depends_driver": True})

    def test_regular_test(self):
        dtest_config = DTestConfigMock()
        # No configuration arguments provided
        assert not is_skippable(self.regular_test, dtest_config, True)
        assert not is_skippable(self.regular_test, dtest_config, False)

        # Configuration arguments do not affect tests without mark
        dtest_config.execute_upgrade_tests = True
        dtest_config.force_execution_of_resource_intensive_tests = True
        dtest_config.skip_resource_intensive_tests = True
        dtest_config.use_vnodes = True
        dtest_config.use_off_heap_memtables = True
        assert not is_skippable(self.regular_test, dtest_config, False)
        dtest_config.execute_upgrade_tests = False
        dtest_config.force_execution_of_resource_intensive_tests = False
        dtest_config.skip_resource_intensive_tests = False
        dtest_config.use_vnodes = False
        dtest_config.use_off_heap_memtables = False

        # Configurations making tests without mark to be skipped
        dtest_config.only_resource_intensive_tests = True
        assert is_skippable(self.regular_test, dtest_config, True)
        assert is_skippable(self.regular_test, dtest_config, False)
        dtest_config.only_resource_intensive_tests = False

        dtest_config.execute_upgrade_tests_only = True
        assert is_skippable(self.regular_test, dtest_config, True)
        dtest_config.execute_upgrade_tests_only = False

    def test_upgrade_test(self):
        dtest_config = DTestConfigMock()
        assert is_skippable(self.upgrade_test, dtest_config, True)

        dtest_config.execute_upgrade_tests = True
        assert not is_skippable(self.upgrade_test, dtest_config, True)
        dtest_config.execute_upgrade_tests = False

        dtest_config.execute_upgrade_tests_only = True
        assert not is_skippable(self.upgrade_test, dtest_config, True)
        dtest_config.execute_upgrade_tests_only = False

    def test_resource_intensive_test(self):
        dtest_config = DTestConfigMock()
        assert not is_skippable(self.resource_intensive_test, dtest_config, True)
        assert is_skippable(self.resource_intensive_test, dtest_config, False)

        dtest_config.force_execution_of_resource_intensive_tests = True
        assert not is_skippable(self.resource_intensive_test, dtest_config, True)
        assert not is_skippable(self.resource_intensive_test, dtest_config, False)
        dtest_config.force_execution_of_resource_intensive_tests = False

        dtest_config.only_resource_intensive_tests = True
        assert not is_skippable(self.resource_intensive_test, dtest_config, True)
        assert not is_skippable(self.resource_intensive_test, dtest_config, False)
        dtest_config.only_resource_intensive_tests = False

        dtest_config.skip_resource_intensive_tests = True
        assert is_skippable(self.resource_intensive_test, dtest_config, True)
        dtest_config.skip_resource_intensive_tests = False

    def test_vnodes_test(self):
        dtest_config = DTestConfigMock()
        assert is_skippable(self.vnodes_test, dtest_config, True)

        dtest_config.use_vnodes = True
        assert not is_skippable(self.vnodes_test, dtest_config, True)
        dtest_config.use_vnodes = False

    def test_no_vnodes_test(self):
        dtest_config = DTestConfigMock()
        assert not is_skippable(self.no_vnodes_test, dtest_config, True)

        dtest_config.use_vnodes = True
        assert is_skippable(self.no_vnodes_test, dtest_config, True)
        dtest_config.use_vnodes = False

    def test_no_offheap_memtables_test(self):
        dtest_config = DTestConfigMock()
        assert not is_skippable(self.no_offheap_memtables_test, dtest_config, True)

        dtest_config.use_off_heap_memtables = True
        assert is_skippable(self.no_offheap_memtables_test, dtest_config, True)
        dtest_config.use_off_heap_memtables = False

    def test_depends_driver_test(self):
        dtest_config = DTestConfigMock()
        assert is_skippable(self.depends_driver_test, dtest_config, True)
