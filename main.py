from pyspark.sql import SparkSession

# Create a Spark session
spark = (SparkSession.
         builder.master("local[*]").appName("PySparkReaderExample").getOrCreate())

# Path to the CSV file (replace with your file path)
csv_file_path = "C:/Users/Karthik Kondpak/Desktop/Registration.csv"

# Read data from the CSV file into a DataFrame with options
df = spark. \
      read.  \
      format("csv") \
      .option("header", "true") \
      .load(csv_file_path)

# Show the DataFrame content
df.show()

# Stop the Spark session
spark.stop()