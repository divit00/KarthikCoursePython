import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

os.environ["PYSPARK_PYTHON"] = "python"  # or "python" depending on your Python executable

# Create a Spark session
spark = SparkSession.builder.appName("PySparkExample").getOrCreate()

#spark.sparkContext.setLogLevel("ERROR")#

# Sample data
data = [("Alice", 25), ("Bob", 30), ("Charlie", 35), ("David", 40)]
columns = ["Name", "Age"]

# Create a DataFrame
df = spark.createDataFrame(data, columns)

# Show the DataFrame
df.show()

# Perform some operations
# Add 5 to the Age column
df = df.withColumn("Age", col("Age") + 5)