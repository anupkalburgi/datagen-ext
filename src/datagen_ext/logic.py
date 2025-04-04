"""Sample logic file for datagen_ext."""

def add_one(number: int) -> int:
    """Adds one to the given number."""
    if not isinstance(number, int):
        raise TypeError("Input must be an integer")
    return number + 1

