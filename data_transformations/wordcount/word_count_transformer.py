from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import Row
from pyspark.sql.functions import explode, split, col, lower, regexp_replace, trim
import logging

def count_words(spark: SparkSession, input_text: str) -> DataFrame:

    input_df = spark.createDataFrame([Row(value=input_text)])
    
    return input_df



def run(spark: SparkSession, input_path: str, output_path: str) -> None:
    logging.info("Reading text file from: %s", input_path)
    
    with open(input_path, 'r') as file:
        input_text = file.read()
    
    word_count_df = count_words(spark, input_text)

    logging.info("Writing word counts to directory: %s", output_path)
    word_count_df.coalesce(1).write.csv(output_path, header=True)
