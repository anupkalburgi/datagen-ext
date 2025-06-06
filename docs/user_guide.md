# User Guide
Coming soon

## Example Usage
```python
from datagen_ext.spec_generators.uc_generator import DatabricksUCSpecGenerator
from datagen_ext.models import DatagenSpec, UCSchemaTarget
from datagen_ext.generator import Generator

uc_table_identifier = ["anup_kalburgi.person.person_details"]
config_generator = DatabricksUCSpecGenerator(tables=uc_table_identifier)
obj = config_generator.generate_spec()
obj.display_all_tables()
ust = UCSchemaTarget(
  catalog="anup_kalburgi"
  , schema_="datagen"
)
obj.output_destination = ust

generator = Generator(spark=spark)
df = generator.generate_and_write_data(obj)
```
