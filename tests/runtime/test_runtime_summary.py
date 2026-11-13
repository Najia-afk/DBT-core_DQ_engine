import pytest

from engine.runtime.store import _has_warnings


@pytest.mark.parametrize(
    "summary",
    [
        {"counts": {"blocked": True}},
        {"status": "blocked"},
        {"counters": {"crm": {"blocked": True}, "erp": {"blocked": False}}},
        {"warnings": False, "counts": {"blocked": True}},
        {"warnings": ["partial delivery"]},
    ],
)
def test_blocked_lane_or_source_is_a_warning(summary):
    assert _has_warnings(summary) is True


@pytest.mark.parametrize(
    "summary", [None, {}, {"status": "ok", "counts": {"blocked": False}}, {"warnings": 0}]
)
def test_clean_lane_summary_has_no_warning(summary):
    assert _has_warnings(summary) is False
