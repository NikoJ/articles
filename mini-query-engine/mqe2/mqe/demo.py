import pyarrow as pa

from core.datatypes import INT32, STRING, ArrowColumn
from core.tables import DataBatch, SchemaField, TableSchema

# 1. Define the schema for the `users` table
schema = TableSchema(
    fields=[
        SchemaField("id", INT32),
        SchemaField("name", STRING),
        SchemaField("age", INT32),
    ]
)

# 2. Create Arrow arrays for each column
id_array = pa.array([1, 2, 3], type=INT32)
name_array = pa.array(["Niko", "on", "Data"], type=STRING)
age_array = pa.array([25, 30, 35], type=INT32)

# 3. Wrap Arrow arrays into our ColumnData abstraction
id_col = ArrowColumn(id_array)
name_col = ArrowColumn(name_array)
age_col = ArrowColumn(age_array)

# 4. Build a DataBatch from the schema and column data
batch = DataBatch(schema=schema, fields=[id_col, name_col, age_col])

# Print a human-readable representation of the batch
print(batch)

# Also show the underlying Arrow schema
print(f"\narrow format:\n{schema.to_arrow()}")
