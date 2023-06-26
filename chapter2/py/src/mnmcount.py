import sys

from pyspark.sql import SparkSession
from pyspark.sql.functions import count

if __name__ == "__main__":
    # Safeguard usage
    if len(sys.argv) != 2:
        print("Usage: mnmcount <file>", file=sys.stderr)
        sys.exit(-1)

    # Build a SparkSession (Create one if it doesn't exist)
    spark = SparkSession.builder.appName("PythonMnMCount").getOrCreate()

    # Load data into dataframe
    # from the file specified as command-line argument
    mnm_file = sys.argv[1]
    mnm_df = (
        spark.read.format("csv")
        .option("header", "true")
        .option("inferSchema", "true")
        .load(mnm_file)
    )

    # Calculate the count for all states
    # 1. Select "State", "Color", "Count"
    # 2. Group each state and its M&M colors' counts
    # 3. Aggregate each state's colors' counts and alias the count column name to "Total"
    # 4. Order the results by count in descending order
    count_mnm_df = (
        mnm_df.select("State", "Color", "Count")
        .groupBy("State", "Color")
        .agg(count("Count").alias("Total"))
        .orderBy("Total", ascending=False)
    )
    # show the result aggregations for all states and colors
    count_mnm_df.show(n=60, truncate=False)
    print(f"Total Rows = {count_mnm_df.count()}")

    # Calculate the count just for state California
    ca_count_mnm_df = (
        mnm_df.select("State", "Color", "Count")
        .where(mnm_df.State == "CA")
        .groupBy("State", "Color")
        .agg(count("Count").alias("Total"))
        .orderBy("Total", ascending=False)
    )
    # Show California result
    ca_count_mnm_df.show(n=10, truncate=False)

    # Stop the SparkSession
    spark.stop()
