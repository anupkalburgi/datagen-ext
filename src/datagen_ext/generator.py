from collections import defaultdict
from typing import Optional, Dict, Tuple, List

from pyspark.sql import SparkSession
from datagen_ext.models import DatagenSpec



import logging # Ensure logging is imported for the main module
import dbldatagen as dg

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)

INTERNAL_ID_COLUMN_NAME = "id"

# Helper function to identify critical ValueErrors that should propagate
def is_critical_pk_error(ve: ValueError) -> bool:
    """Checks if a ValueError message indicates a critical PK configuration error."""
    msg = str(ve).lower()
    critical_phrases = [
        "pk", # Catches "PK ... missing type"
        "primary key and therefore cannot be nullable"
    ]
    return any(phrase in msg for phrase in critical_phrases)

class Generator:
    def __init__(self, spark: SparkSession, app_name: str = "DataGen_ClassBased"):
        """
        Initializes the Generator.
        Args:
            spark: An existing SparkSession.
            app_name: The name for the Spark application (used if a session were created here).
        """
        if spark:
            self.spark = spark
            self._created_spark_session = False
            logger.info("Using provided SparkSession.")
        else:
            logger.error("SparkSession cannot be None during Generator initialization.")
            raise RuntimeError("SparkSession cannot be None")

    def prepare_data_generators(self, config: DatagenSpec, config_source_name: str = "PydanticConfig") \
            -> Tuple[Dict[str, Optional[dg.DataGenerator]], bool]:
        """
        Prepares DataGenerator specifications for each table based on the config.
        This step does NOT build or write data.
        Critical PK configuration errors will raise ValueError and halt preparation.
        Other errors are logged, and the specific table's generator is set to None.
        Args:
            config: A DatagenSpec Pydantic object.
            config_source_name: A name for the configuration source (for logging).
        Returns:
            A tuple containing:
            - A dictionary mapping table names to their configured dbldatagen.DataGenerator objects (or None if prep failed for that table).
            - True if all specs for all tables were prepared successfully (and no critical errors occurred), False otherwise.
        """
        if not self.spark:
            logger.error("SparkSession is not available in Generator. Cannot prepare data generators.")
            raise RuntimeError("SparkSession is not available. Cannot prepare data generators.")

        logger.info(f"Starting data generator preparation for config: {config_source_name}")
        tables_config = config.tables
        global_gen_options = config.generator_options if config.generator_options else {}

        prepared_generators: Dict[str, Optional[dg.DataGenerator]] = defaultdict(lambda: None)
        all_specs_successful = True

        generation_order = list(tables_config.keys())
        logger.info(f"Preparing DataGenerator specs for tables in order: {generation_order}")

        for table_name in generation_order:
            if table_name not in tables_config:
                logger.warning(f"Table '{table_name}' listed in generation order but not found in tables config. Skipping.")
                all_specs_successful = False
                continue

            table_spec = tables_config[table_name]
            logger.info(f"--- Preparing DataGenerator spec for table: {table_name} ---")

            rows = table_spec.number_of_rows
            try:
                default_partitions = self.spark.sparkContext.defaultParallelism
            except Exception as ex:
                logger.warning(f"Could not get default Spark parallelism for table '{table_name}', defaulting to 4. Error: {ex}")
                default_partitions = 4

            partitions = table_spec.partitions if table_spec.partitions is not None else default_partitions
            columns_spec = getattr(table_spec, 'columns', [])

            try:
                data_gen = dg.DataGenerator(sparkSession=self.spark,
                                            name=f"{table_name}_spec_from_{config_source_name}",
                                            rows=rows,
                                            partitions=partitions,
                                            **global_gen_options)

                for col_def in columns_spec:
                    col_name = getattr(col_def, 'name', 'unknown_column')
                    final_kwargs = {}
                    is_primary = getattr(col_def, 'primary', False)
                    col_type = getattr(col_def, 'type', None)
                    is_nullable = getattr(col_def, 'nullable', False) # Default to False if not present

                    if is_primary:
                        # Explicit check for nullable primary key (Pydantic should catch this first, but defensive check here)
                        if is_nullable is True: # Explicitly check for True
                            msg = f"Column '{col_name}' in table '{table_name}' is a primary key and therefore cannot be nullable (nullable=True is not allowed)."
                            logger.error(msg)
                            raise ValueError(msg)

                        # Check for missing type for primary key
                        if not col_type:
                            msg = f"PK '{col_name}' in table '{table_name}' missing type."
                            logger.error(msg)
                            raise ValueError(msg)

                        logger.info(f"Table '{table_name}': Column '{col_name}' is PRIMARY KEY. Deriving from '{INTERNAL_ID_COLUMN_NAME}'.")
                        pk_kwargs = getattr(col_def, 'options', {}).copy()
                        pk_kwargs['colType'] = col_type
                        pk_kwargs['baseColumn'] = INTERNAL_ID_COLUMN_NAME

                        if col_type == "string":
                            pk_kwargs['format'] = pk_kwargs.get('format', "0x%013X")
                            logger.debug(f"PK '{col_name}' (string) using base '{INTERNAL_ID_COLUMN_NAME}', format '{pk_kwargs['format']}'")
                        elif col_type not in ["int", "long", "integer", "bigint", "short", "byte"]:
                            logger.warning(f"PK '{col_name}' type '{col_type}' based on '{INTERNAL_ID_COLUMN_NAME}'.")

                        conflicting_opts_for_pk = ['min', 'max', 'uniqueValues', 'random', 'baseValue',
                                                   'template', 'values', 'dataRange', 'distribution', 'expr']
                        for opt_key in conflicting_opts_for_pk:
                            if opt_key in pk_kwargs:
                                logger.debug(f"For PK '{col_name}', option '{opt_key}' may be ignored due to baseColumn logic.")

                        col_omit = getattr(col_def, 'omit', None)
                        if col_omit is not None: pk_kwargs['omit'] = col_omit
                        final_kwargs = pk_kwargs
                    else:
                        regular_kwargs = getattr(col_def, 'options', {}).copy()
                        if col_type: regular_kwargs['colType'] = col_type
                        base_col = getattr(col_def, 'baseColumn', None)
                        if base_col: regular_kwargs['baseColumn'] = base_col
                        base_col_type = getattr(col_def, 'baseColumnType', None)
                        if base_col_type: regular_kwargs['baseColumnType'] = base_col_type
                        col_omit = getattr(col_def, 'omit', None)
                        if col_omit is not None: regular_kwargs['omit'] = col_omit
                        final_kwargs = regular_kwargs

                    logger.debug(f"Table '{table_name}': Defining column '{col_name}' with spec: {final_kwargs}")
                    data_gen = data_gen.withColumn(colName=col_name, **final_kwargs)

                prepared_generators[table_name] = data_gen
                logger.info(f"--- Successfully prepared DataGenerator spec for table: {table_name} ---")

            except ValueError as ve:
                if is_critical_pk_error(ve):
                    logger.error(f"Critical PK configuration error for table '{table_name}': {ve}. Halting preparation.")
                    raise # Re-raise critical PK ValueErrors to be caught by the caller
                else:
                    # Handle other ValueErrors locally
                    logger.exception(f"--- Handled ValueError during DataGenerator spec preparation for table '{table_name}' ---")
                    prepared_generators[table_name] = None
                    all_specs_successful = False
            except Exception as e:
                logger.exception(f"--- General ERROR during DataGenerator spec preparation for table '{table_name}' ---")
                prepared_generators[table_name] = None
                all_specs_successful = False

        if not all_specs_successful: # This will be true if non-critical errors occurred
            logger.error(f"--- DataGenerator spec preparation completed WITH NON-CRITICAL ERRORS for config: {config_source_name} ---")
        elif not prepared_generators and generation_order : # No generators but tables were expected
            logger.warning(f"--- DataGenerator spec preparation completed, but no generators were successfully created for config: {config_source_name} ---")
        else:
            logger.info(f"--- DataGenerator spec preparation completed successfully for config: {config_source_name} ---")

        return prepared_generators, all_specs_successful

    def write_prepared_data(self,
                            prepared_generators: Dict[str, Optional[dg.DataGenerator]],
                            output_format: str,
                            output_path_prefix: str,
                            config_source_name: str = "PydanticConfig") -> bool:
        if not self.spark:
            logger.error("SparkSession is not available. Cannot write data.")
            return False

        if not prepared_generators:
            logger.warning(f"No prepared data generators to write for config: {config_source_name}. Skipping write phase.")
            return True

        logger.info(f"Starting data writing from prepared generators for config: {config_source_name}")
        all_writes_successful = True
        attempted_any_write = False

        for table_name, data_gen in prepared_generators.items():
            if data_gen is None:
                logger.warning(f"Skipping write for table '{table_name}' as its DataGenerator spec was not prepared successfully.")
                continue

            attempted_any_write = True
            logger.info(f"--- Starting data build and write for table: {table_name} ---")
            try:
                logger.info(f"Table '{table_name}': Building DataFrame...")
                df = data_gen.build()

                requested_rows = data_gen.rowCount
                actual_row_count = df.count()
                logger.info(f"Table '{table_name}': Built DataFrame with {actual_row_count} rows (requested: {requested_rows}).")

                if actual_row_count == 0 and requested_rows > 0:
                    logger.warning(f"Table '{table_name}': Requested {requested_rows} rows but built 0. Check definitions.")

                table_output_path = f"{output_path_prefix}/{table_name}"
                logger.info(f"Table '{table_name}': Writing {output_format} data to: {table_output_path}")
                df.write.format(output_format).mode("overwrite").save(table_output_path)
                logger.info(f"Table '{table_name}': Finished writing.")

            except Exception as e:
                logger.exception(f"--- FATAL ERROR during build or write for table '{table_name}' ---")
                all_writes_successful = False

        if not attempted_any_write and prepared_generators:
            logger.info(f"--- No valid data generators found to write for config: {config_source_name} ---")
            # If all generators were None, and we didn't attempt any write, this is not a write failure.
            # The success of this method depends on whether writes were *attempted* and failed.
            return True


        if all_writes_successful:
            logger.info(f"--- Data writing completed successfully for attempted tables for config: {config_source_name} ---")
        else:
            logger.error(f"--- Data writing completed WITH ERRORS for config: {config_source_name} ---")

        return all_writes_successful

    def generate_and_write_data(self, config: DatagenSpec, config_source_name: str = "PydanticConfig") -> bool:
        if not self.spark:
            logger.error("SparkSession is not available in Generator. Cannot proceed.")
            return False

        logger.info(f"Starting combined data generation and writing for config: {config_source_name}")

        try:
            validated_output_path = config.validate_output_path(config)
            logger.info(f"Using validated output path prefix: {validated_output_path}")
        except ValueError as ve: # Catch validation error from get_validated_output_path
            logger.error(f"Invalid output_path_prefix in config '{config_source_name}': {ve}. Halting.")
            return False
        except AttributeError: # Catch if get_validated_output_path doesn't exist on config
            logger.error(f"The 'config' object of type '{type(config).__name__}' does not have 'get_validated_output_path' method. Ensure it's the correct DatagenSpec model. Halting.")
            return False

        try:
            prepared_generators_map, overall_prep_success = self.prepare_data_generators(config, config_source_name)
        except ValueError as ve_prep: # Catch critical ValueErrors propagated from prepare_data_generators
            logger.error(f"Critical error during data generator preparation for config '{config_source_name}': {ve_prep}. Halting.")
            return False

        if not overall_prep_success: # Handles cases where non-critical errors occurred but didn't propagate
            failed_tables = [table for table, gen in prepared_generators_map.items() if gen is None]
            logger.error(f"Data generator spec preparation failed for one or more tables in config '{config_source_name}'. "
                         f"Failed tables (if any specific ones identified): {failed_tables}. "
                         "Halting before write phase.")
            return False

        if not prepared_generators_map and list(config.tables.keys()): # Check if tables were expected but none prepared
            logger.warning(f"No data generators were successfully prepared for config '{config_source_name}', though tables were defined. Nothing to write.")
            return True # Technically no failure in prep or write if nothing was actionable.

        write_success = self.write_prepared_data(
            prepared_generators_map,
            config.output_format,
            validated_output_path,
            config_source_name
        )

        if write_success:
            logger.info(f"Combined data generation and writing completed successfully for config: {config_source_name}")
        else:
            logger.error(f"Combined data generation and writing failed during the write phase for config: {config_source_name}")

        return write_success