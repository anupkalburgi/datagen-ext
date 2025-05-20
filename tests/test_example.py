"""Example test file using pytest for datagen_ext."""

import pytest
def test_always_passes():
    """A simple test that should always pass."""
    assert True

# Example of skipping a test
@pytest.mark.skip(reason="Not implemented yet")
def test_future_feature():
    assert False
