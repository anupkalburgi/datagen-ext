try:
    import pyspark
except ImportError:
    # Optionally, check if running in a known environment that should provide Spark
    import os

    if "DATABRICKS_RUNTIME_VERSION" not in os.environ:
        # Not in Databricks and pyspark is missing
        raise ImportError(
            "pyspark is not installed or available in your Python environment. "
            "If running locally, ensure pyspark is installed (e.g., 'pip install pyspark' or "
            "install this package with the 'dev' extra: 'pip install .[dev]'). "
            "In Databricks, pyspark should be provided by the runtime."
        )
    # If in Databricks and still failing, it's an unexpected issue.
    # Or simply let the ImportError propagate if it's always expected to be there.


__version__ = "0.1.0"

# Optionally expose core functions
# from .datagen_ext.logic import add_one
