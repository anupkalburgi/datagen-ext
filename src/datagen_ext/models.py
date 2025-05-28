from typing import List, Dict, Optional, Any, Union, Literal, Type, TypeVar
from pydantic import root_validator, BaseModel, ValidationError
import pandas as pd
from IPython.display import display


DbldatagenBasicType = Literal[
    "string", "int", "long", "float", "double", "decimal",
    "boolean", "date", "timestamp", "short", "byte", "binary",
    "integer", "bigint", "tinyint"
]


class ColumnDefinition(BaseModel):
    name: str
    type: Optional[DbldatagenBasicType] = None
    primary: bool = False  # For custom primary key logic/validation
    options: Optional[Dict[str, Any]] = {}
    nullable: Optional[bool] = False  # New field: specifies if the column can contain nulls
    omit: Optional[bool] = False  # dbldatagen option to omit column from final output
    baseColumn: Optional[str] = "id"
    baseColumnType: Optional[str] = "auto"  # Type of base column(s) if not obvious


    @root_validator(skip_on_failure=True)
    def check_column_constraints(cls, values: Dict[str, Any]) -> Dict[str, Any]:
        primary = values.get('primary', False)
        options = values.get('options', {})
        nullable = values.get('nullable', False)
        name = values.get('name', '<unknown>')

        # Constraint 1: primary columns can't have min/max options
        if primary and options:
            if 'min' in options or 'max' in options:
                raise ValueError(
                    f"Column '{name}' is marked as primary and has 'min' or 'max' in options. "
                    "This conflicts with the custom validation rule."
                )

        # Constraint 2: primary columns can't be nullable
        if primary and nullable:
            raise ValueError(
                f"Column '{name}' is a primary key and therefore cannot be nullable (nullable=True is not allowed)."
            )

        return values


class TableDefinition(BaseModel):
    number_of_rows: int
    partitions: Optional[int] = 1
    columns: List[ColumnDefinition]


class DatagenSpec(BaseModel):
    tables: Dict[str, TableDefinition]
    output_format: str = "parquet"
    output_path_prefix: str = "synthetic_data_pydantic/" # Default value
    generator_options: Optional[Dict[str, Any]] = {}

    def validate_output_path(cls, values):
        path = values.output_path_prefix

        # Condition 1: Starts with /Volume
        starts_with_volume = path.startswith("/Volume")

        # Condition 2: Three-level namespace
        is_three_level_namespace = False
        parts = path.split('.')
        if len(parts) == 3 and all(part.strip() for part in parts):
            is_three_level_namespace = True

        if not (starts_with_volume or is_three_level_namespace):
            raise ValueError(
                f"output_path_prefix '{path}' is invalid. "
                "It must either start with '/Volume' or be a three-level namespace "
                "(e.g., 'your_catalog.your_schema.your_table')."
            )
        return path

    def display_all_tables(self):
        """
        Display all tables with columns in a user-friendly way using Databricks or IPython display.
        """
        for table_name, table_def in self.tables.items():
            print(f"\nTable: {table_name}")
            print(f"\tOutput Path/Table: {self.output_path_prefix}{table_name}")
            df = pd.DataFrame([col.dict() for col in table_def.columns])
            display(df)

