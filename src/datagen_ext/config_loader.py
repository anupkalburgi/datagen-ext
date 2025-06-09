from typing import List, Dict, Optional, Any, Union, Literal, Type, TypeVar
import json
from pydantic import BaseModel, ValidationError

from datagen_ext.models import DatagenSpec

# --- Config Loader Class ---
T = TypeVar("T", bound=BaseModel)  # Generic type for the model


class ConfigLoader:
    """
    A utility class to load configurations into a Pydantic model.
    """

    @staticmethod
    def _preprocess_data(data_dict: Dict[str, Any]) -> Dict[str, Any]:
        """
        Internal method for any preprocessing steps before validation.
        Currently, it checks for 'generator_options' and prints a note.
        You could add more complex transformations here if needed.
        """
        if "generator_options" in data_dict:
            print(
                "Note: 'generator_options' found in input data. It will be ignored if not defined in the Pydantic model (Pydantic v2 default behavior)."
            )
            # If you want to strictly remove it, you could do:
            # data_dict.pop("generator_options", None)
        return data_dict

    @classmethod
    def from_dict(cls, data_dict: Dict[str, Any], model: Type[T] = DatagenSpec) -> T:
        """
        Loads configuration from a Python dictionary.

        Args:
            data_dict: The dictionary containing configuration data.
            model: The Pydantic model to validate against (defaults to DatagenConfig).

        Returns:
            An instance of the Pydantic model.

        Raises:
            ValidationError: If the data doesn't match the model schema.
            TypeError: If data_dict is not a dictionary.
        """
        if not isinstance(data_dict, dict):
            raise TypeError("Input data_dict must be a dictionary.")

        processed_data = cls._preprocess_data(data_dict.copy())  # Use a copy to avoid modifying original dict
        try:
            return model.model_validate(processed_data)
        except ValidationError as e:
            print(f"Pydantic validation error when loading from dictionary: {e}")
            raise

    @classmethod
    def from_json_string(cls, json_string: str, model: Type[T] = DatagenSpec) -> T:
        """
        Loads configuration from a JSON string.

        Args:
            json_string: The JSON string containing configuration data.
            model: The Pydantic model to validate against (defaults to DatagenConfig).

        Returns:
            An instance of the Pydantic model.

        Raises:
            json.JSONDecodeError: If the string is not valid JSON.
            ValidationError: If the data doesn't match the model schema.
            TypeError: If json_string is not a string.
        """
        if not isinstance(json_string, str):
            raise TypeError("Input json_string must be a string.")
        try:
            data_dict = json.loads(json_string)
        except json.JSONDecodeError as e:
            print(f"Error decoding JSON string: {e}")
            raise
        return cls.from_dict(data_dict, model)

    @classmethod
    def from_file(cls, file_path: str, model: Type[T] = DatagenSpec) -> T:
        """
        Loads configuration from a JSON file.

        Args:
            file_path: The path to the JSON file.
            model: The Pydantic model to validate against (defaults to DatagenConfig).

        Returns:
            An instance of the Pydantic model.

        Raises:
            FileNotFoundError: If the file does not exist.
            IOError: If there's an error reading the file.
            json.JSONDecodeError: If the file content is not valid JSON.
            ValidationError: If the data doesn't match the model schema.
        """
        try:
            with open(file_path, "r") as f:
                json_string = f.read()
        except FileNotFoundError:
            print(f"Error: File not found at path '{file_path}'")
            raise
        except IOError as e:
            print(f"Error reading file at path '{file_path}': {e}")
            raise

        return cls.from_json_string(json_string, model)
