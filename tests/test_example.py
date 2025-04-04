"""Example test file using pytest for datagen_ext."""

import pytest
from datagen_ext.logic import add_one # Updated import path

def test_add_one_success():
    """Test that add_one correctly adds one."""
    assert add_one(3) == 4
    assert add_one(0) == 1
    assert add_one(-1) == 0

def test_add_one_type_error():
    """Test that add_one raises TypeError for non-int input."""
    with pytest.raises(TypeError):
        add_one("hello")
    with pytest.raises(TypeError):
        add_one(5.5)

def test_always_passes():
    """A simple test that should always pass."""
    assert True

# Example of skipping a test
@pytest.mark.skip(reason="Not implemented yet")
def test_future_feature():
    assert False
