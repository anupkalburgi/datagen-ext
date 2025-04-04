# src/models/config_models.py

from typing import List, Dict, Optional, Any, Union, Literal, Set
from pydantic import BaseModel, Field, field_validator, model_validator
# --- Pydantic Models (V2 Syntax with Enhanced Validation) ---

DbldatagenBasicType = Literal[
    "string", "int", "long", "float", "double", "decimal",
    "boolean", "date", "timestamp", "short", "byte", "binary",
    "integer", "bigint", "tinyint"
]
DbldatagenSpec = Dict[str, Any]

class IntermediateBaseSpec(BaseModel):
    name: str = Field(..., description="Internal name for the intermediate base column")
    spec: DbldatagenSpec = Field(..., description="dbldatagen spec for the intermediate base column")

    @classmethod
    @field_validator('spec')
    def check_uniqueValues_present(cls, spec_dict: DbldatagenSpec) -> DbldatagenSpec:
        if 'uniqueValues' not in spec_dict:
            raise ValueError("intermediate_base spec MUST contain 'uniqueValues'")
        return spec_dict

class FinalKeySpec(BaseModel):
    spec: DbldatagenSpec = Field(..., description="dbldatagen spec for the final key column, based on intermediate")

    @classmethod
    @field_validator('spec')
    def check_baseColumn_present(cls, spec_dict: DbldatagenSpec) -> DbldatagenSpec:
        if 'baseColumn' not in spec_dict:
            raise ValueError("final_key spec MUST contain 'baseColumn'")
        return spec_dict

class SharedKeyGenerator(BaseModel):
    intermediate_base: IntermediateBaseSpec = Field(..., alias='intermediate_base')
    final_key: FinalKeySpec = Field(..., alias='final_key')

    @model_validator(mode='after')
    def check_baseColumn_match(self) -> 'SharedKeyGenerator':
        intermediate_base = self.intermediate_base
        final_key = self.final_key
        if intermediate_base and final_key:
            inter_name = intermediate_base.name
            final_base_col = final_key.spec.get('baseColumn')
            if final_base_col != inter_name:
                raise ValueError(f"Mismatch: 'final_key.spec.baseColumn' ('{final_base_col}') must match 'intermediate_base.name' ('{inter_name}')")
        return self

class ColumnDefinition(BaseModel):
    name: str
    use_shared_key_gen: Optional[str] = Field(None, alias='use_shared_key_gen')
    type: Optional[DbldatagenBasicType] = None
    options: Optional[Dict[str, Any]] = {}
    baseColumn: Optional[str] = None
    baseColumnType: Optional[str] = None
    omit: Optional[bool] = False

    @classmethod
    @model_validator(mode='before')
    def check_exclusive_fields(cls, data: Any) -> Any:
        if not isinstance(data, dict): return data
        has_shared_key = data.get('use_shared_key_gen') is not None
        has_type = data.get('type') is not None
        has_expr = data.get('options', {}).get('expr') is not None
        if has_shared_key and (has_type or has_expr or data.get('options')):
             raise ValueError(f"Column '{data.get('name')}': Cannot use 'use_shared_key_gen' together with 'type', 'options.expr', or other options.")
        if not has_shared_key and not has_type and not has_expr:
            raise ValueError(f"Column '{data.get('name')}': Must specify either 'use_shared_key_gen', 'type', or 'options.expr'.")
        return data

class TableDefinition(BaseModel):
    rows: int
    partitions: Optional[int] = None
    columns: List[ColumnDefinition]
    # We remove the less effective @field_validator for baseColumn here,
    # as it will be handled globally in DatagenConfig validator.

class DatagenConfig(BaseModel):
    shared_key_generators: Optional[Dict[str, SharedKeyGenerator]] = Field({}, alias='shared_key_generators')
    tables: Dict[str, TableDefinition]
    output_format: str = "parquet"
    output_path_prefix: str = "/tmp/synthetic_data_pydantic/"
    generator_options: Optional[Dict[str, Any]] = Field({}, alias='generator_options')

    @model_validator(mode='after')
    def check_all_references(self) -> 'DatagenConfig':
        shared_gens = self.shared_key_generators if self.shared_key_generators else {}
        tables = self.tables

        if not tables: # No tables defined
            return self

        default_seed_col_name = self.generator_options.get("seedColumnName", "id") # Get default ID name

        for table_name, table_def in tables.items():
            if not table_def or not table_def.columns: continue # Skip if table is empty

            available_cols_for_table: Set[str] = {default_seed_col_name} # Start with default ID
            intermediate_cols_for_table: Set[str] = set()

            # First pass: identify all intermediate columns for this table
            for col_def in table_def.columns:
                shared_key_name = col_def.use_shared_key_gen
                if shared_key_name:
                    if shared_key_name not in shared_gens:
                         # This check ensures the generator exists before trying to access it
                         raise ValueError(f"Validation Error in Table '{table_name}', Column '{col_def.name}': "
                                          f"References unknown shared key generator '{shared_key_name}'")
                    # If generator exists, add its intermediate name
                    intermediate_cols_for_table.add(shared_gens[shared_key_name].intermediate_base.name)

            # Second pass: validate baseColumn references for all columns in order
            defined_cols_in_order: Set[str] = set()
            for i, col_def in enumerate(table_def.columns):
                current_col_name = col_def.name
                base_col = col_def.baseColumn

                # Check the baseColumn reference if it exists
                if base_col:
                    # Is base_col the default ID? (implicitly available)
                    is_default_id = (base_col == default_seed_col_name)
                    # Is base_col an intermediate col for this table?
                    is_intermediate = (base_col in intermediate_cols_for_table)
                    # Is base_col a column defined earlier in this table's list?
                    is_defined_earlier = (base_col in defined_cols_in_order)

                    if not (is_default_id or is_intermediate or is_defined_earlier):
                        # If none of the above, it's an invalid reference
                        raise ValueError(f"Validation Error in Table '{table_name}', Column '{current_col_name}': "
                                         f"References 'baseColumn' ('{base_col}') which is not defined earlier in the column list, "
                                         f"is not an intermediate column from a used shared generator, and is not the default seed column ('{default_seed_col_name}').")

                # Add the current column (and its intermediate if applicable) to the set of defined columns for subsequent checks
                defined_cols_in_order.add(current_col_name)
                if col_def.use_shared_key_gen:
                     # Add the intermediate name as well, as it's defined logically at this point
                     defined_cols_in_order.add(shared_gens[col_def.use_shared_key_gen].intermediate_base.name)


        return self

# --- End Pydantic Models ---