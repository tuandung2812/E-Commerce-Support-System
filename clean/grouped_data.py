from pyspark.sql.functions import lower, regexp_replace, regexp_extract, col, trim, when, instr, lit, concat_ws, size, split, row_number
from pyspark.sql.types import StructType,StructField, StringType
from pyspark.sql import Window

from pyspark.sql import SparkSession
import argparse

spark = (SparkSession
    .builder
    .appName("group_data")
    .getOrCreate())

def load_file(path):

    # to convert attrs to String
    schema = StructType([
        StructField("attrs", StringType(), True),
        StructField("avg_rating", StringType(), True),
        StructField("num_review", StringType(), True),
        StructField("num_sold", StringType(), True),
        StructField("price", StringType(), True),
        StructField("product_desc", StringType(), True),
        StructField("product_name", StringType(), True),
        StructField("shipping", StringType(), True),
        StructField("shop_info", StringType(), True),
        StructField("url", StringType(), True)
    ])
    df = spark.read.format("csv").schema(schema)\
    .load(path)

    return df

def grouping(df):
    w = Window.partitionBy('product_name').orderBy(col("price").desc())
    df = df.withColumn("row",row_number().over(w)).filter(col("row") == 1).drop("row")
    return df

def write_file(df, destination):

    (df  
        .coalesce(1)
        .write.option("header", True)
        .format("csv")
        .mode('overwrite')
        .csv(destination))

    print("Succeed bitch!")

    return df

def get_grouped_data(path, destination):
    df = load_file(path)
    df = grouping(df)
    write_file(df, destination)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Get grouped data')

    parser.add_argument('--origin', 
                        type=str,
                        help='Read location')

    parser.add_argument('--destination', 
                        type=str,
                        help='Save location')

    args = parser.parse_args()

    get_grouped_data(args.origin, args.destination)