import json
import os
import sys
import logging  # Import logging module
from pyspark.sql import SparkSession
import dbldatagen as dg
import argparse

# Assuming models are in src/models/config_models.py relative to execution
# Adjust import path if necessary based on your execution context
try:
    from .config_models import DatagenConfig
except ImportError:
    # Fallback if running directly from src directory or structure differs
    try:
        from .config_models import DatagenConfig
    except ImportError:
        print("FATAL ERROR: Could not import DatagenConfig. Ensure models/config_models.py exists and Python path is correct.")
        sys.exit(1)

# --- Basic Logging Setup ---
# Configure logging once at the module level or in the main execution block
# This basic config logs INFO and higher to the console
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
# Get a logger specific to this module
logger = logging.getLogger(__name__)
# --- End Logging Setup ---


def generate_data_from_pydantic_config(config_model: DatagenConfig, config_path: str):
    """
    Generates synthetic data using a validated Pydantic configuration model.

    Args:
        config_model: A validated DatagenConfig Pydantic object.
        config_path: The original path of the config file (for context in logs).
    """
    # Config already validated, proceed to Spark init and generation

    # --- 2. Initialize Spark Session ---
    spark = None
    try:
        # Consider making appName more dynamic if needed
        spark_builder = SparkSession.builder \
            .appName(f"DataGen_{os.path.basename(config_path)}") \
            .master("local[*]") # Keep local for now, parameterize for prod

        # Add any Spark conf settings needed for production (e.g., memory, shuffle partitions)
        # spark_builder = spark_builder.config("spark.sql.shuffle.partitions", "...")

        spark = spark_builder.getOrCreate()
        logger.info("SparkSession initialized successfully.")
    except Exception as e:
        logger.exception("Fatal Error: Failed to initialize SparkSession.") # .exception includes traceback
        return # Cannot proceed without Spark

    # --- 3. Prepare for Generation (Using validated Pydantic model) ---
    tables_config = config_model.tables
    shared_key_generators = config_model.shared_key_generators
    output_format = config_model.output_format
    output_path_prefix = config_model.output_path_prefix
    global_gen_options = config_model.generator_options

    generation_order = list(tables_config.keys())
    logger.info(f"Processing tables in order: {generation_order}")

    # --- 4. Generate Data Table by Table ---
    all_successful = True
    for table_name, table_spec in tables_config.items():
        logger.info(f"--- Starting generation for table: {table_name} ---")

        rows = table_spec.rows
        # Safely get default parallelism if partitions not specified
        try:
            default_partitions = spark.sparkContext.defaultParallelism
        except Exception:
            logger.warning("Could not get default parallelism, defaulting to 4.")
            default_partitions = 4 # Fallback default
        partitions = table_spec.partitions if table_spec.partitions is not None else default_partitions
        columns_spec = table_spec.columns

        try:
            # Initialize DataGenerator
            data_gen = (dg.DataGenerator(sparkSession=spark,
                                         name=f"{table_name}_spec",
                                         rows=rows,
                                         partitions=partitions,
                                         **global_gen_options) # Pass generator options
                       )
            defined_intermediate_cols = set()
            logger.debug(f"Initialized DataGenerator for {table_name} with {rows} rows, {partitions} partitions.")

            # --- Define columns ---
            for col_def in columns_spec:
                col_name = col_def.name # Already validated by Pydantic
                shared_key_gen_name = col_def.use_shared_key_gen

                if shared_key_gen_name:
                    # Shared key generator details already validated
                    key_gen_spec = shared_key_generators[shared_key_gen_name]
                    inter_base_col_name = key_gen_spec.intermediate_base.name
                    inter_base_col_spec_dict = key_gen_spec.intermediate_base.spec
                    final_key_col_spec_dict = key_gen_spec.final_key.spec

                    # 1. Add INTERMEDIATE base column
                    if inter_base_col_name not in defined_intermediate_cols:
                        logger.debug(f"Table '{table_name}': Defining intermediate base '{inter_base_col_name}'")
                        data_gen = data_gen.withColumn(colName=inter_base_col_name, **inter_base_col_spec_dict)
                        defined_intermediate_cols.add(inter_base_col_name)

                    # 2. Add FINAL key column
                    logger.debug(f"Table '{table_name}': Defining final key column '{col_name}' based on '{inter_base_col_name}'")
                    data_gen = data_gen.withColumn(colName=col_name, **final_key_col_spec_dict)

                else:
                    # Regular column definition
                    logger.debug(f"Table '{table_name}': Defining regular column '{col_name}'")
                    kwargs = col_def.options.copy() if col_def.options else {} # Start with options dict
                    if col_def.type: kwargs['colType'] = col_def.type
                    if col_def.baseColumn: kwargs['baseColumn'] = col_def.baseColumn
                    if col_def.baseColumnType: kwargs['baseColumnType'] = col_def.baseColumnType
                    if col_def.omit: kwargs['omit'] = col_def.omit

                    data_gen = data_gen.withColumn(colName=col_name, **kwargs)

            # --- 5. Build and Save DataFrame ---
            logger.info(f"Table '{table_name}': Building DataFrame...")
            df = data_gen.build()
            logger.info(f"Table '{table_name}': Successfully built DataFrame with {df.count()} rows.")

            # Ensure output path is absolute or handle relative paths carefully
            # For prod, usually absolute paths (HDFS, S3, ADLS) are preferred
            output_path = os.path.abspath(os.path.join(output_path_prefix, table_name))
            logger.info(f"Table '{table_name}': Writing {output_format} data to: {output_path}")
            # Consider adding options for partitioning, etc. for production writes
            df.write.format(output_format).mode("overwrite").save(output_path)
            logger.info(f"Table '{table_name}': Finished writing.")

        except Exception as e:
            # Log the specific error with traceback for the failing table
            logger.exception(f"--- FATAL ERROR during generation or writing for table '{table_name}' ---")
            all_successful = False
            break # Stop processing further tables

    # --- 6. Cleanup ---
    if spark:
        try:
            spark.stop()
            logger.info("SparkSession stopped.")
        except Exception:
            logger.warning("Exception occurred while stopping SparkSession.", exc_info=True)


    if all_successful:
        logger.info("--- Data generation completed successfully. ---")
    else:
        logger.error("--- Data generation completed with errors. ---")
