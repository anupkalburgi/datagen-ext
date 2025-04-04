"""Basic CLI entry point defined in pyproject.toml."""

import sys
from .datagen_ext.logic import add_one

def main():
    print(f"Running CLI for {PROJECT_NAME} (package {PACKAGE_NAME})")
    # Example: process command line args
    args = sys.argv[1:]
    if not args:
        print("No arguments provided. Try providing a number.")
        return 1 # Indicate error

    try:
        num_arg = int(args[0])
        result = add_one(num_arg)
        print(f"Result of add_one({num_arg}): {result}")
        return 0 # Indicate success
    except ValueError:
        print(f"Error: Could not convert argument '{args[0]}' to an integer.")
        return 1
    except TypeError as e:
        print(f"Error: {e}")
        return 1

if __name__ == "__main__":
    sys.exit(main())

