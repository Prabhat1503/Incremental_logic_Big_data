from pyspark.sql import SparkSession

# Initialize Spark session
spark = SparkSession.builder.appName("Advertisment").getOrCreate()

# Load the CSV file into a Spark DataFrame
csv_df = spark.read.csv("/FileStore/tables/Advertising-6.csv", header=True, inferSchema=True)

# Show the first few rows of the DataFrame
csv_df.show() 

csv_df.columns

# Define the path where you want to save the Delta table
delta_path = "/FileStore/tables/Advertising.delta"

# Save the DataFrame as a Delta table
csv_df.write.format("delta").save(delta_path)

# Print the schema to check the column names
csv_df.printSchema()

from delta.tables import DeltaTable

# Load the existing Delta table
delta_table = DeltaTable.forPath(spark, "/FileStore/tables/Advertising.delta")

# Show the existing Delta table
delta_table.toDF().show()

# Load new data (replace this path with the actual path of new data)
new_data_df = spark.read.csv("/FileStore/tables/Advertising___new-2.csv", header=True, inferSchema=True)

new_data_df.columns


from delta.tables import DeltaTable
from pyspark.sql.functions import col

# Load the new data with the Sales_2024 column
new_data_df = spark.read.csv("/FileStore/tables/Advertising___new-2.csv", header=True, inferSchema=True)

# Load the existing Delta table
delta_table = DeltaTable.forPath(spark, "/FileStore/tables/Advertising.delta")

# Perform the merge operation with schema evolution enabled
delta_table.alias("old") \
    .merge(
        new_data_df.alias("new"),
        "old._c0 = new._c0"  # Matching condition based on the '_c0' column
    ) \
    .whenMatchedUpdateAll() \
    .whenNotMatchedInsertAll() \
    .execute()

# Display the contents of the updated Delta table
delta_table.toDF().show()

# Print the schema of the updated Delta table to confirm the addition of the Sales_2024 column
delta_table.toDF().printSchema()

from pyspark.sql.functions import lit

# Load the existing Delta table as a DataFrame
delta_df = delta_table.toDF()

# Add the Sales_2024 column with null values initially
delta_df = delta_df.withColumn("Sales_2024", lit(None).cast("integer"))

# Write back the updated DataFrame with the new column and enable schema evolution
delta_df.write.format("delta").mode("overwrite").option("mergeSchema", "true").save("/FileStore/tables/Advertising.delta")


from delta.tables import DeltaTable

# Reload the Delta table to get the latest schema
delta_table = DeltaTable.forPath(spark, "/FileStore/tables/Advertising.delta")

# Perform the merge operation with schema evolution enabled
delta_table.alias("old") \
    .merge(
        new_data_df.alias("new"),
        "old._c0 = new._c0"  # Matching condition based on the '_c0' column
    ) \
    .whenMatchedUpdate(
        set={
            "TV": "new.TV",
            "Radio": "new.Radio",
            "Newspaper": "new.Newspaper",
            "Sales": "new.Sales",
            "Sales_2024": "new.Sales_2024"  # Update the new column
        }
    ) \
    .whenNotMatchedInsert(
        values={
            "_c0": "new._c0",
            "TV": "new.TV",
            "Radio": "new.Radio",
            "Newspaper": "new.Newspaper",
            "Sales": "new.Sales",
            "Sales_2024": "new.Sales_2024"  # Insert the new column
        }
    ) \
    .execute()

# Display the contents of the updated Delta table
delta_table.toDF().show()

# Print the updated schema
delta_table.toDF().printSchema()
