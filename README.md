# Incremental_logic_Big_data

### README

# Delta Table Management for Advertising Data

This project involves using **PySpark** to load advertising data from CSV files into Delta tables, update the schema to include a new column (`Sales_2024`), and perform upserts (merge) operations. Delta Lake's schema evolution feature allows you to manage incremental changes in the data, such as adding new columns or updating records.

---

## Prerequisites

- Apache Spark with Delta Lake
- PySpark installed and configured
- A Spark session initialized
- Delta Lake enabled in the Spark environment

Ensure that Delta Lake is available in your Spark environment by including it in your dependencies or Spark session setup:

```python
spark = SparkSession.builder \
    .appName("DeltaExample") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()
```

---

## Steps Involved

### 1. Load Data from CSV

The CSV file `Advertising-6.csv` is loaded into a Spark DataFrame with the following command:

```python
csv_df = spark.read.csv("/FileStore/tables/Advertising-6.csv", header=True, inferSchema=True)
csv_df.show()
```
![image](https://github.com/user-attachments/assets/27b437e5-e78e-4b9f-956b-23464fbb341c)


Here, `header=True` ensures that the first row is treated as headers, and `inferSchema=True` automatically infers data types.

### 2. Save DataFrame as a Delta Table

Once the data is loaded, it's saved as a Delta table at the specified location:

```python
delta_path = "/FileStore/tables/Advertising.delta"
csv_df.write.format("delta").save(delta_path)
```
![image](https://github.com/user-attachments/assets/e78d06d8-ec2e-42a1-9145-82514586f227)



### 3. Load New Data

The new CSV data (`Advertising___new-2.csv`) containing the `Sales_2024` column is loaded:

```python
new_data_df = spark.read.csv("/FileStore/tables/Advertising___new-2.csv", header=True, inferSchema=True)
```
![image](https://github.com/user-attachments/assets/dbd4deb7-4a77-4df5-9dd4-7b9e26b85ec8)


### 4. Merge Data with Schema Evolution

Using Delta Lake’s merge operation with schema evolution, the new column `Sales_2024` is added and updated, ensuring any new data is correctly merged into the existing Delta table:

```python
delta_table.alias("old") \
    .merge(
        new_data_df.alias("new"),
        "old._c0 = new._c0"
    ) \
    .whenMatchedUpdate(
        set={
            "TV": "new.TV",
            "Radio": "new.Radio",
            "Newspaper": "new.Newspaper",
            "Sales": "new.Sales",
            "Sales_2024": "new.Sales_2024"
        }
    ) \
    .whenNotMatchedInsert(
        values={
            "_c0": "new._c0",
            "TV": "new.TV",
            "Radio": "new.Radio",
            "Newspaper": "new.Newspaper",
            "Sales": "new.Sales",
            "Sales_2024": "new.Sales_2024"
        }
    ) \
    .execute()
```

This block of code:
- Updates existing records where the `_c0` column matches in both old and new data.
- Inserts new records where no match is found.
- The `Sales_2024` column is updated or inserted as needed.

### 5. Schema Evolution

In case the `Sales_2024` column doesn’t exist in the original Delta table, schema evolution is enabled by initially adding the column with `null` values:

```python
delta_df = delta_df.withColumn("Sales_2024", lit(None).cast("integer"))
delta_df.write.format("delta").mode("overwrite").option("mergeSchema", "true").save(delta_path)
```

### 6. View Updated Table

After the merge operation, the contents of the updated Delta table can be viewed:

```python
delta_table.toDF().show()
```

The schema is printed to confirm the successful addition of the `Sales_2024` column:

```python
delta_table.toDF().printSchema()
```
![image](https://github.com/user-attachments/assets/cc69ea7f-72ad-4ce8-bae3-27cbd3a90d04)

---

## File Structure

- **`/FileStore/tables/Advertising-6.csv`**: The initial CSV file containing the advertising data.
- **`/FileStore/tables/Advertising___new-2.csv`**: The new CSV file containing updates, including the new `Sales_2024` column.
- **`/FileStore/tables/Advertising.delta`**: The location where the Delta table is stored.

---

## Key Functions and Features

- **Schema Evolution**: Automatically adapts to changes in the schema, such as adding new columns (`Sales_2024`).
- **Delta Merge**: Performs upserts (insert/update) operations, merging new and old data efficiently.
- **DataFrame Operations**: Leverages Spark DataFrame functions to manipulate and manage data.

---

## How to Run

1. Ensure PySpark and Delta Lake are configured in your environment.
2. Use the provided code to initialize the Spark session and load the CSV files.
3. Save the initial data as a Delta table and perform the schema evolution and merge operation.
4. Inspect the final Delta table to confirm the updates.

---


## Troubleshooting

- **Schema Mismatch**: If new columns are not added, ensure schema evolution is enabled with the `mergeSchema` option.
- **Missing Data**: Verify that the matching condition in the merge operation is correct (`"old._c0 = new._c0"`).
  
For any issues with Spark session configuration or Delta Lake setup, refer to the [Delta Lake documentation](https://docs.delta.io/latest/index.html).

---

## License

This project is licensed under the MIT License.

--- 
