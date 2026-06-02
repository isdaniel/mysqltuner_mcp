"""Tests for the _compat helpers themselves (unit-style)."""

from tests.integration._compat import _meets


class TestMeets:
    def test_equal(self):
        assert _meets("8.0.18", "8.0.18")

    def test_greater_patch(self):
        assert _meets("8.0.19", "8.0.18")

    def test_greater_minor(self):
        assert _meets("8.1.0", "8.0.99")

    def test_greater_major(self):
        assert _meets("9.0.0", "8.99.99")

    def test_smaller_patch(self):
        assert not _meets("8.0.17", "8.0.18")

    def test_smaller_major(self):
        assert not _meets("5.7.44", "8.0.0")

    def test_handles_suffix(self):
        # MySQL 5.7 reports "5.7.44-log" or similar - should still parse
        assert _meets("8.0.36-log", "8.0.18")
        assert not _meets("5.7.44-log", "8.0.0")
