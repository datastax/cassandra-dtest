from unittest import TestCase

import pytest

from conftest import is_skippable
from mock import Mock, patch

class DTestConfigMock():
    def __init__(self):
        self.execute_upgrade_tests = False
        self.execute_upgrade_tests_only = False
        self.force_execution_of_resource_intensive_tests = False
        self.only_resource_intensive_tests = False
        self.skip_resource_intensive_tests = False
        self.use_vnodes = False
        self.use_off_heap_memtables = False

    def set(self, config):
        if config != "":
            setattr(self, config, True)

def _mock_responses(responses, default_response=None):
    return lambda arg: responses[arg] if arg in responses else default_response

def _mock_responses_any(responses, default_response=None):
    return lambda i, m: responses[m] if m in responses else default_response

class TestConfTest(object):
    regular_test = Mock(name="regular_test_mock")
    upgrade_test = Mock(name="upgrade_test_mock")
    resource_intensive_test = Mock(name="resource_intensive_test_mock")
    vnodes_test = Mock(name="vnodes_test_mock")
    no_vnodes_test = Mock(name="no_vnodes_test_mock")
    no_offheap_memtables_test = Mock(name="no_offheap_memtables_test_mock")
    depends_driver_test = Mock(name="depends_driver_test_mock")

    no_patch = Mock(name="any_class_has_mark_none", side_effect= _mock_responses_any({}))
    upgrade_test_patch = Mock(name="any_class_has_mark_upgrade_test",
                              side_effect= _mock_responses_any({"upgrade_test": True}))
    resource_intensive_patch = Mock(name="any_class_has_mark_resource_intensive",
                                    side_effect= _mock_responses_any({"resource_intensive": True}))
    vnodes_patch = Mock(name="any_class_has_mark_vnodes", side_effect= _mock_responses_any({"vnodes": True}))
    no_vnodes_patch = Mock(name="any_class_has_mark_no_vnodes", side_effect= _mock_responses_any({"no_vnodes": True}))
    no_offheap_memtables_patch = Mock(name="any_class_has_mark_no_offheap_memtables",
                                      side_effect= _mock_responses_any({"no_offheap_memtables": True}))
    depends_driver_patch = Mock(name="any_class_has_mark_depends_dirver",
                                side_effect= _mock_responses_any({"depends_driver": True}))

    def setup_method(self, method):
        self.regular_test.get_closest_marker.side_effect = _mock_responses({})
        self.upgrade_test.get_closest_marker.side_effect = _mock_responses({"upgrade_test": True})
        self.resource_intensive_test.get_closest_marker.side_effect = _mock_responses({"resource_intensive": True})
        self.vnodes_test.get_closest_marker.side_effect = _mock_responses({"vnodes": True})
        self.no_vnodes_test.get_closest_marker.side_effect = _mock_responses({"no_vnodes": True})
        self.no_offheap_memtables_test.get_closest_marker.side_effect = _mock_responses({"no_offheap_memtables": True})
        self.depends_driver_test.get_closest_marker.side_effect = _mock_responses({"depends_driver": True})

    @pytest.mark.parametrize("item", [upgrade_test, resource_intensive_test, vnodes_test, depends_driver_test])
    @pytest.mark.parametrize("a_class_marked", [no_patch, upgrade_test_patch, resource_intensive_patch, vnodes_patch,
                                           no_vnodes_patch, no_offheap_memtables_patch, depends_driver_patch])
    def test_skip_if_no_config(self, item, a_class_marked):
        dtest_config = DTestConfigMock()
        with patch('conftest.any_class_in_module_has_mark', a_class_marked):
            assert is_skippable(item, dtest_config, False)

    @pytest.mark.parametrize("config", ["execute_upgrade_tests_only", "only_resource_intensive_tests"])
    @pytest.mark.parametrize("a_class_marked", [no_patch, resource_intensive_patch, vnodes_patch, no_vnodes_patch,
                                           no_offheap_memtables_patch, depends_driver_patch])
    @pytest.mark.parametrize("sufficient_resources", [True, False])
    def test_skip_if_no_direct_marks(self, config, a_class_marked, sufficient_resources):
        dtest_config = DTestConfigMock()
        dtest_config.set(config)
        with patch('conftest.any_class_in_module_has_mark', a_class_marked):
            assert is_skippable(self.regular_test, dtest_config, sufficient_resources)

    @pytest.mark.parametrize("item", [regular_test, resource_intensive_test, no_vnodes_test, no_offheap_memtables_test])
    @pytest.mark.parametrize("a_class_marked", [no_patch, resource_intensive_patch, vnodes_patch, no_vnodes_patch,
                                           no_offheap_memtables_patch, depends_driver_patch])
    def test_include_if_no_config(self, item, a_class_marked):
        dtest_config = DTestConfigMock()
        with patch('conftest.any_class_in_module_has_mark', a_class_marked):
            assert not is_skippable(self.regular_test, dtest_config, True)

    @pytest.mark.parametrize("a_class_marked", [no_patch, resource_intensive_patch, vnodes_patch, no_vnodes_patch,
                                           no_offheap_memtables_patch, depends_driver_patch])
    @pytest.mark.parametrize("sufficient_resources", [True, False])
    def test_include_if_no_direct_marks(self, a_class_marked, sufficient_resources):
        dtest_config = DTestConfigMock()
        dtest_config.execute_upgrade_tests = True
        dtest_config.force_execution_of_resource_intensive_tests = True
        dtest_config.skip_resource_intensive_tests = True
        dtest_config.use_vnodes = True
        dtest_config.use_off_heap_memtables = True
        with patch('conftest.any_class_in_module_has_mark', a_class_marked):
            assert not is_skippable(self.regular_test, dtest_config, sufficient_resources)

    @pytest.mark.parametrize("item,a_class_marked", [(regular_test, upgrade_test_patch),
                                                (upgrade_test, no_patch),
                                                (upgrade_test, upgrade_test_patch)])
    def test_skip_upgrade_test(self, item, a_class_marked):
        dtest_config = DTestConfigMock()
        with patch('conftest.any_class_in_module_has_mark', a_class_marked):
            assert is_skippable(item, dtest_config, True)

    @pytest.mark.parametrize("item, a_class_marked", [(regular_test, upgrade_test_patch),
                                                 (upgrade_test, no_patch),
                                                 (upgrade_test, upgrade_test_patch)])
    @pytest.mark.parametrize("config", ["execute_upgrade_tests", "execute_upgrade_tests_only"])
    def test_include_upgrade_test(self, item, a_class_marked, config):
        dtest_config = DTestConfigMock()
        dtest_config.set(config)
        with patch('conftest.any_class_in_module_has_mark', a_class_marked):
            assert not is_skippable(item, dtest_config, False)

    @pytest.mark.parametrize("config, sufficient_resources", [("", False),
                                                              ("skip_resource_intensive_tests", True),
                                                              ("skip_resource_intensive_tests", False)])
    def test_skip_resource_intensive(self, config, sufficient_resources):
        dtest_config = DTestConfigMock()
        dtest_config.set(config)
        assert is_skippable(self.resource_intensive_test, dtest_config, sufficient_resources)

    @pytest.mark.parametrize("config", ["force_execution_of_resource_intensive_tests", "only_resource_intensive_tests"])
    @pytest.mark.parametrize("sufficient_resources", [True, False])
    def test_include_resource_intensive_if_any_resources(self, config, sufficient_resources):
        dtest_config = DTestConfigMock()
        dtest_config.set(config)
        assert not is_skippable(self.resource_intensive_test, dtest_config, sufficient_resources)

    def test_include_resource_intensive_if_sufficient_resources_no_config(self):
        dtest_config = DTestConfigMock()
        assert not is_skippable(self.resource_intensive_test, dtest_config, True)

    @pytest.mark.parametrize("a_class_marked", [no_patch, vnodes_patch, no_vnodes_patch])
    def test_include_vnodes(self, a_class_marked):
        dtest_config = DTestConfigMock()
        dtest_config.use_vnodes = True
        with patch('conftest.any_class_in_module_has_mark', a_class_marked):
            assert not is_skippable(self.vnodes_test, dtest_config, False)

    @pytest.mark.parametrize("a_class_marked", [no_patch, vnodes_patch, no_vnodes_patch])
    def test_include_no_vnodes(self, a_class_marked):
        dtest_config = DTestConfigMock()
        with patch('conftest.any_class_in_module_has_mark', a_class_marked):
            assert not is_skippable(self.no_vnodes_test, dtest_config, False)

    def test_skip_no_offheap_memtables(self):
        dtest_config = DTestConfigMock()
        dtest_config.use_off_heap_memtables
        assert not is_skippable(self.no_offheap_memtables_test, dtest_config, True)

    @pytest.mark.parametrize("config", ["", "execute_upgrade_tests", "execute_upgrade_tests_only",
                                        "force_execution_of_resource_intensive_tests", "only_resource_intensive_tests",
                                        "skip_resource_intensive_tests","use_vnodes", "use_off_heap_memtables"])
    @pytest.mark.parametrize("sufficient_resources", [True, False])
    def test_skip_depends_driver_always(self, config, sufficient_resources):
        dtest_config = DTestConfigMock()
        dtest_config.set(config)
        assert is_skippable(self.depends_driver_test, dtest_config, sufficient_resources)
