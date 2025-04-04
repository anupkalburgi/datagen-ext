# tests/test_generator_scenarios.py

import pytest
import os
import json
import shutil
from pathlib import Path

from pydantic import ValidationError

# Assuming your models and generator are importable from src
# Adjust imports based on your exact structure and how you run pytest
try:
    from src.config_models import DatagenConfig
    # Decide if you want to test the main script's wrapper or the core function
    # Importing the core function is often better for unit/integration testing
    from src.generator import generate_data_from_pydantic_config
    # If testing main.py, you might need subprocess or monkeypatching
except ImportError:
    pytest.skip("Skipping tests, could not import required src modules.", allow_module_level=True)

from pyspark.sql import SparkSession
from pyspark.sql.types import (
    LongType, StringType, BooleanType, IntegerType, DecimalType,
    TimestampType, ArrayType, StructType # Import necessary types
)

# --- Fixtures ---

@pytest.fixture(scope="session")
def spark_session():
    """Provides a SparkSession for the test session."""
    spark = (
        SparkSession.builder.appName("pytest_datagen_tests")
        .master("local[2]") # Use 2 cores for local testing
        .config("spark.sql.session.timeZone", "UTC") # Consistent timezone
        .config("spark.ui.showConsoleProgress", "false") # Quieter logs
        .config("spark.sql.shuffle.partitions", "2") # Small shuffle partitions
        # Add Delta Lake specific configs if testing Delta output
        # .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        # .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .getOrCreate()
    )
    # Set log level for less verbose Spark output during tests
    spark.sparkContext.setLogLevel("WARN")
    yield spark
    spark.stop()

@pytest.fixture(scope="function")
def temp_output_dir(tmp_path_factory):
    """Creates a temporary output directory for each test function."""
    # tmp_path_factory is a pytest fixture providing temporary paths
    # Use function scope to ensure each test gets a fresh directory
    path = tmp_path_factory.mktemp("datagen_output_")
    yield path
    # Optional: Explicit cleanup if needed, though pytest handles tmp_path
    # shutil.rmtree(path, ignore_errors=True)


# --- Test Functions ---

def run_generation_for_test(config_file: Path, output_dir: Path):
    """Helper to load, validate, modify path, and run generation."""
    if not config_file.is_file():
        pytest.fail(f"Test configuration file not found: {config_file}")

    try:
        with open(config_file, 'r') as f:
            config_data = json.load(f)

        # Validate config
        config_model = DatagenConfig.model_validate(config_data)

        # IMPORTANT: Override output path for the test
        original_prefix = config_model.output_path_prefix
        test_prefix = str(output_dir / original_prefix.strip('/\\')) # Use relative path within tmp dir
        config_model.output_path_prefix = test_prefix

        print(f"\nRunning generator for {config_file.name} -> output: {test_prefix}")
        # Call the core generator function with the validated model
        generate_data_from_pydantic_config(str(config_file))
        return test_prefix # Return the modified path used for output

    except ValidationError as e:
         pytest.fail(f"Pydantic validation failed for {config_file.name}:\n{e}")
    except Exception as e:
        pytest.fail(f"Exception during generation for {config_file.name}: {e}")


@pytest.mark.parametrize("config_filename", ["simple_table.json"])
def test_scenario_simple(spark_session, temp_output_dir, config_filename):
    """Tests the simple single-table configuration."""
    config_file = Path("config") / config_filename # Assumes running pytest from project root
    output_prefix = run_generation_for_test(config_file, temp_output_dir)

    # --- Verification ---
    users_path = os.path.join(output_prefix, "Users")
    assert os.path.exists(users_path), f"Output directory not found: {users_path}"

    df_users = spark_session.read.parquet(users_path)
    df_users.printSchema()
    df_users.show(5)

    # Check row count
    assert df_users.count() == 50, "Row count mismatch for Users table"

    # Check schema
    expected_cols = {"user_id", "username", "is_active", "login_count"}
    assert set(df_users.columns) == expected_cols, "Column mismatch for Users table"
    assert isinstance(df_users.schema["user_id"].dataType, LongType)
    assert isinstance(df_users.schema["username"].dataType, StringType)
    assert isinstance(df_users.schema["is_active"].dataType, BooleanType)
    assert isinstance(df_users.schema["login_count"].dataType, IntegerType)

    # Check basic value constraints (optional, example)
    assert df_users.filter("user_id < 101").count() == 0, "user_id should start at 101"
    assert df_users.filter("login_count < 0 or login_count > 100").count() == 0, "login_count out of range"
    # Check nulls - approx check
    null_count = df_users.where("is_active is null").count()
    assert 0 < null_count < 15, f"is_active null count ({null_count}) unexpected for 10% target" # Allow some variance


@pytest.mark.parametrize("config_filename", ["config_complex_types.json"])
def test_scenario_complex(spark_session, temp_output_dir, config_filename):
    """Tests joins, complex types, distributions, nulls."""
    config_file = Path("config") / config_filename
    output_prefix = run_generation_for_test(config_file, temp_output_dir)

    # --- Verification ---
    profile_path = os.path.join(output_prefix, "UserProfile")
    activity_path = os.path.join(output_prefix, "UserActivity")
    assert os.path.exists(profile_path), f"Output directory not found: {profile_path}"
    assert os.path.exists(activity_path), f"Output directory not found: {activity_path}"

    # Use format("delta") if testing delta output
    df_profile = spark_session.read.format("delta").load(profile_path)
    df_activity = spark_session.read.format("delta").load(activity_path)

    df_profile.printSchema()
    df_profile.show(5, truncate=False)
    df_activity.printSchema()
    df_activity.show(5)

    # Check row counts
    assert df_profile.count() == 500, "Row count mismatch for UserProfile"
    assert df_activity.count() == 2500, "Row count mismatch for UserActivity"

    # Check complex types
    assert isinstance(df_profile.schema["balance"].dataType, DecimalType), "balance should be Decimal"
    assert df_profile.schema["balance"].dataType.precision == 10, "Decimal precision mismatch"
    assert df_profile.schema["balance"].dataType.scale == 2, "Decimal scale mismatch"
    # Checking array type requires inspecting the element type as well
    assert isinstance(df_profile.schema["tags"].dataType, StringType), "tags type should be String (as expr returns stringified array)"
    # To properly check array, expr would need to return actual array type, e.g., `array(lit('tag1'), ...)`
    # Let's verify it contains array-like characters for now
    first_tag = df_profile.select("tags").first()[0]
    assert first_tag.startswith("[") and first_tag.endswith("]"), "tags doesn't look like a stringified array"

    assert isinstance(df_profile.schema["last_login"].dataType, TimestampType)
    assert isinstance(df_activity.schema["activity_ts"].dataType, TimestampType)

    # Check nulls (tags has 5% null target)
    tag_null_count = df_profile.where("tags is null").count()
    assert 10 < tag_null_count < 40, f"tags null count ({tag_null_count}) unexpected for 5% target"

    # Check join validity (all activity user IDs should exist in profiles)
    distinct_activity_users = df_activity.select("user_id_ref").distinct()
    distinct_profile_users = df_profile.select("profile_id").distinct()
    joined_df = distinct_activity_users.join(distinct_profile_users,
                                             distinct_activity_users["user_id_ref"] == distinct_profile_users["profile_id"],
                                             "inner")
    assert joined_df.count() == distinct_activity_users.count(), "Not all activity user IDs found in profiles"


@pytest.mark.parametrize("config_filename", ["config_variations.json"])
def test_scenario_variations(spark_session, temp_output_dir, config_filename):
    """Tests weights, baseColumnType hash, relative dates."""
    config_file = Path("config") / config_filename
    output_prefix = run_generation_for_test(config_file, temp_output_dir)

    # --- Verification ---
    devices_path = os.path.join(output_prefix, "Devices")
    events_path = os.path.join(output_prefix, "Events")
    assert os.path.exists(devices_path), f"Output directory not found: {devices_path}"
    assert os.path.exists(events_path), f"Output directory not found: {events_path}"

    df_devices = spark_session.read.parquet(devices_path)
    df_events = spark_session.read.parquet(events_path)

    df_devices.printSchema()
    df_devices.show(5)
    df_events.printSchema()
    df_events.show(5)

    # Check row counts
    assert df_devices.count() == 100, "Row count mismatch for Devices"
    assert df_events.count() == 5000, "Row count mismatch for Events"

    # Check schema
    assert isinstance(df_devices.schema["device_identifier"].dataType, StringType)
    assert isinstance(df_devices.schema["status"].dataType, StringType)
    assert isinstance(df_events.schema["event_id"].dataType, LongType)
    assert isinstance(df_events.schema["device_ref"].dataType, StringType)
    assert isinstance(df_events.schema["event_code"].dataType, IntegerType) # Based on hash, should be int
    assert isinstance(df_events.schema["event_time"].dataType, TimestampType)

    # Check weights (approximate check)
    status_counts = df_devices.groupBy("status").count().orderBy("status").collect()
    # Example: check 'ACTIVE' count is roughly 70% +/- tolerance
    active_row = next((row for row in status_counts if row["status"] == "ACTIVE"), None)
    assert active_row is not None, "ACTIVE status missing"
    # Looser check due to low row count
    assert 60 < active_row["count"] < 80, f"ACTIVE count ({active_row['count']}) not roughly 70%"

    # Check hash base type result (difficult to verify exact values, check type and range)
    assert df_events.filter("event_code < 100 or event_code > 105").count() == 0, "event_code out of range"

    # Check join validity
    distinct_event_devs = df_events.select("device_ref").distinct()
    distinct_devices = df_devices.select("device_identifier").distinct()
    joined_df = distinct_event_devs.join(distinct_devices,
                                         distinct_event_devs["device_ref"] == distinct_devices["device_identifier"],
                                         "inner")
    assert joined_df.count() == distinct_event_devs.count(), "Not all event device refs found in devices"