from __future__ import annotations

from typing import List, Dict, Optional, Any, Literal, Union
from pydantic import BaseModel, model_validator, field_validator
import pandas as pd
from IPython.display import display, HTML
import os
import logging
from enum import Enum

logger = logging.getLogger(__name__)


class PathType(Enum):
    UC_TABLE = "Unity Catalog Table"
    UC_VOLUME = "Unity Catalog Volume"
    RELATIVE = "Relative Path"
    ABSOLUTE_NON_UC = "Absolute Non-UC Path"  # Covers DBFS, local absolute, etc.
    UNKNOWN = "Unknown or Invalid Path Structure"


# Allowed (PathType, output_format_lowercase) combinations
_OFF_DB_STRICT_ALLOWED_COMBINATIONS = {
    (PathType.RELATIVE, "delta"),
    (PathType.RELATIVE, "parquet"),
    (PathType.RELATIVE, "csv"),
    # Add more formats like orc, json, text as needed
}

_DATABRICKS_COMPATIBLE_ALLOWED_COMBINATIONS = {
    (PathType.UC_TABLE, "delta"),
    (PathType.UC_VOLUME, "parquet"),
    (PathType.UC_VOLUME, "csv"),
    # Add more formats like orc, json, text as needed
}


DbldatagenBasicType = Literal[
    "string",
    "int",
    "long",
    "float",
    "double",
    "decimal",
    "boolean",
    "date",
    "timestamp",
    "short",
    "byte",
    "binary",
    "integer",
    "bigint",
    "tinyint",
]


# Note: There is no way to specify the per table output.
# One will have to create a different config for each schema
class UCSchemaTarget(BaseModel):
    catalog: str
    schema_: str
    output_format: str = "delta"  # Default to delta for UC Schema

    @field_validator("catalog", "schema_", mode="after")
    def validate_identifiers(cls, v):
        if not v.strip():
            raise ValueError("Identifier must be non-empty.")
        if not v.isidentifier():
            logger.warning(f"'{v}' is not a basic Python identifier. Ensure validity for Unity Catalog.")
        return v.strip()

    def __str__(self):
        return f"{self.catalog}.{self.schema_} (Format: {self.output_format}, Type: UC Table)"


class FilePathTarget(BaseModel):
    base_path: str
    output_format: Literal["csv", "parquet"]  # No default, must be specified

    @field_validator("base_path", mode="after")
    def validate_base_path(cls, v):
        if not v.strip():
            raise ValueError("base_path must be non-empty.")
        return v.strip()

    def __str__(self):
        return f"{self.base_path} (Format: {self.output_format}, Type: File Path)"


class ColumnDefinition(BaseModel):
    name: str
    type: Optional[DbldatagenBasicType] = None
    key_ref: Optional[str] = None
    is_primary: bool = False
    options: Optional[Dict[str, Any]] = {}
    nullable: Optional[bool] = False
    omit: Optional[bool] = False
    baseColumn: Optional[str] = "id"
    baseColumnType: Optional[str] = "auto"

    @model_validator(mode="after")
    def check_constraints(self):
        if self.is_primary:
            if "min" in self.options or "max" in self.options:
                raise ValueError(f"Primary column '{self.name}' cannot have min/max options.")
            if self.nullable:
                raise ValueError(f"Primary column '{self.name}' cannot be nullable.")
        return self


class TableDefinition(BaseModel):
    number_of_rows: int
    partitions: Optional[int] = 1
    columns: List[ColumnDefinition]


class KeyDefinition(BaseModel):
    type: Literal["long", "int", "integer", "string", "date"] | str = "long"
    options: Dict[str, Any] = {}         # e.g. {"fk_mode": "hash"|"random"}
    cardinality: Optional[int] = None


class DatagenSpec(BaseModel):
    tables: Dict[str, TableDefinition]
    output_destination: Optional[Union[UCSchemaTarget, FilePathTarget]] = None
    keys: Dict[str, KeyDefinition] = {}
    generator_options: Optional[Dict[str, Any]] = {}
    intended_for_databricks: Optional[bool] = None

    @staticmethod
    def _is_running_on_databricks() -> bool:
        return "DATABRICKS_RUNTIME_VERSION" in os.environ

    @staticmethod
    def _determine_path_type(path_prefix: str) -> PathType:
        path = path_prefix.strip()
        if len(path.split(".")) == 3:
            return PathType.UC_TABLE
        if path.startswith("/Volumes/"):
            return PathType.UC_VOLUME
        if path.startswith("/") or path.startswith("\\") or (len(path) > 1 and path[1] == ":"):
            return PathType.ABSOLUTE_NON_UC
        return PathType.RELATIVE

    @model_validator(mode="before")
    def validate_paths_and_formats(cls, values: Dict[str, Any]) -> Dict[str, Any]:
        output = values.get("output_destination")
        intended = values.get("intended_for_databricks") or cls._is_running_on_databricks()

        if isinstance(output, FilePathTarget):
            path_type = cls._determine_path_type(output.base_path)
            fmt = output.output_format.lower()
            allowed = {PathType.UC_VOLUME, PathType.RELATIVE, PathType.ABSOLUTE_NON_UC}
            if path_type not in allowed:
                raise ValueError(f"Invalid path type '{path_type.value}' for FilePathTarget")
            values["path_type"] = path_type

        elif isinstance(output, UCSchemaTarget):
            path_type = PathType.UC_TABLE
            values["path_type"] = path_type

        return values


    @model_validator(mode="after")
    def _validate_keys(cls, spec: "DatagenSpec") -> "DatagenSpec":
        referenced_keys: set[str] = set()
        pk_tables: dict[str, list[str]] = {}

        for t_name, t_def in spec.tables.items():
            for col in t_def.columns:
                if col.key_ref is None:
                    continue
                if col.key_ref not in spec.keys:
                    raise ValueError(
                        f"Column '{t_name}.{col.name}' references undefined key_ref '{col.key_ref}'"
                    )
                referenced_keys.add(col.key_ref)
                if col.is_primary:
                    pk_tables.setdefault(col.key_ref, []).append(t_name)

        # 2.  Derive cardinality if *exactly one* PK table & cardinality missing
        for kref, tables_with_pk in pk_tables.items():
            kdef = spec.keys[kref]
            if kdef.cardinality is None and len(tables_with_pk) == 1:
                tbl = tables_with_pk[0]
                kdef.cardinality = spec.tables[tbl].number_of_rows
                logger.warning(
                    "Cardinality for key '%s' derived from primary table '%s' (rows=%d)",
                    kref,
                    tbl,
                    kdef.cardinality,
                )

        # 3.  Validate every referenced key now has cardinality
        for kref in referenced_keys:
            if spec.keys[kref].cardinality is None:
                raise ValueError(
                    f"Key '{kref}' is referenced in tables but 'cardinality' is still undefined.")

        # 4.  Optional warning for keys never used
        unused = [k for k in spec.keys if k not in referenced_keys]
        if unused:
            logger.info("Keys defined but never referenced: %s", ", ".join(unused))

        return spec

    def finalize(self) -> None:
        if self.output_destination is None:
            raise ValueError("output_destination must be specified before finalization.")

    def display_all_tables(self):
        for table_name, table_def in self.tables.items():
            print(f"Table: {table_name}")

            if self.output_destination:
                output = f"{self.output_destination}"
                display(HTML(f"<strong>Output destination:</strong> {output}"))
            else:
                display(
                    HTML(
                        "<strong>Output destination:</strong> "
                        "<span style='color: red; font-weight: bold;'>None</span><br>"
                        "<span style='color: gray;'>Set it using the <code>output_destination</code> attribute on your "
                        "<code>DatagenSpec</code> object (e.g., <code>my_spec.output_destination = UCSchemaTarget(...)</code>).</span>"
                    )
                )

            df = pd.DataFrame([col.dict() for col in table_def.columns])
            try:
                display(df)
            except NameError:
                print(df.to_string())
