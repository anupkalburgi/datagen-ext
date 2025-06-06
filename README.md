# datagen-ext

A Python library for generating synthetic data and writing it to Databricks Unity Catalog tables Volumes. Built for easy integration with Spark and Databricks workflows.

Note: This library is built on top of databrickslabs/dbldatagen. It does not support all the features of the original dbldatagen library, nor is it intended to. For advanced or highly customized data generation use cases, please consider using dbldatagen directly.

## Getting Started

### Installation
```bash
pip install datagen-ext
```

### Example Usage
```python
from datagen_ext.spec_generators.uc_generator import DatabricksUCSpecGenerator
from datagen_ext.models import DatagenSpec, UCSchemaTarget
from datagen_ext.generator import Generator

uc_table_identifier = ["anup_kalburgi.person.person_details"]
config_generator = DatabricksUCSpecGenerator(tables=uc_table_identifier)
obj = config_generator.generate_spec()
obj.display_all_tables()
ust = UCSchemaTarget(
  catalog="my_cat",
  schema_="datagen"
)
obj.output_destination = ust

generator = Generator(spark=spark)
df = generator.generate_and_write_data(obj)
```

For more details, see the [Developer Guide](docs/developer_guide.md).