import argparse
import awswrangler as wr

from pyspark.sql import SparkSession
from pyspark.sql.functions import col.udf


def main(input_path, output_path):
  spark = SparkSession.builder.appName("pyspark").getOrCreate()
  df = spark.read.option("header", True).csv(input_path)

  df.show()

  #df.repartition(1).write.mode("append").option("header", True).option("compression","gzip").csv(output_path)

  pdDF = df.toPandas()

  wr.s3.to_csv(
    df = pdDF,
    path = output_path,
    compression = "gzip",
    index = False
  )

  if __name__ == "__main__":
    parser = argparser.ArgumentParser()
    parser.add_argument('--input_path', help="path to input csv")
    parser.add_argument('--output_path', help="path to output csv")
    args = parser.parse_args()

    main(args.input_path, args.output_path)