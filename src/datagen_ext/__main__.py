"""Basic CLI entry point defined in pyproject.toml."""

import sys
from .logic import add_one
import argparse
import logging
from pydantic import ValidationError
from .config_models import DatagenConfig
from .generator import generate_data_from_pydantic_config
import json

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
# Get a logger specific to this module
logger = logging.getLogger(__name__)

# --- Main execution block ---
def main():
    # Use argparse for command-line arguments - more robust than hardcoding
    parser = argparse.ArgumentParser(description="Generate synthetic data based on a JSON config.")
    parser.add_argument(
        "-c", "--config",
        required=True,
        help="Path to the configuration JSON file."
    )
    parser.add_argument(
        "-v", "--verbose",
        action="store_true",
        help="Enable DEBUG level logging."
    )

    args = parser.parse_args()
    config_path = args.config

    # Adjust logging level if verbose flag is set
    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG) # Set root logger level
        logger.debug("Verbose logging enabled.")

    logger.info(f"Starting data generation process with config: {config_path}")

    # --- Load and Validate Configuration ---
    config_model_validated = None
    try:
        with open(config_path, 'r') as f:
            config_data_raw = json.load(f)
        config_model_validated = DatagenConfig.model_validate(config_data_raw)
        logger.info(f"Configuration loaded and validated successfully from '{config_path}'")

    except FileNotFoundError:
        logger.error(f"Configuration file not found: '{config_path}'")
        sys.exit(1) # Exit with error code
    except json.JSONDecodeError as e:
        logger.error(f"Invalid JSON in configuration file '{config_path}': {e}")
        sys.exit(1)
    except ValidationError as e:
        logger.error("--- Configuration Validation Failed (Pydantic) ---")
        logger.error(e) # Pydantic's error formatting is helpful
        logger.error("-------------------------------------------------")
        sys.exit(1)
    except Exception as e:
         logger.exception(f"An unexpected error occurred during configuration loading/validation for '{config_path}'")
         sys.exit(1)

    # --- Run Generation (only if validation succeeded) ---
    if config_model_validated:
        generate_data_from_pydantic_config(config_model_validated, config_path)
    else:
        # Should not happen if validation is correct, but as a safeguard
        logger.critical("Validation succeeded but config model is None. Aborting.")
        sys.exit(1)

    # logger.info("Script finished.")
    return 1



# def main():
#     # Example: process command line args
#     args = sys.argv[1:]
#     if not args:
#         print("No arguments provided. Try providing a number.")
#         return 1 # Indicate error

#     try:
#         num_arg = int(args[0])
#         result = add_one(num_arg)
#         print(f"Result of add_one({num_arg}): {result}")
#         return 0 # Indicate success
#     except ValueError:
#         print(f"Error: Could not convert argument '{args[0]}' to an integer.")
#         return 1
#     except TypeError as e:
#         print(f"Error: {e}")
#         return 1

if __name__ == "__main__":
    sys.exit(main())



