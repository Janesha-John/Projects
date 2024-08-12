import pytest
import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, explode, lower, split, regexp_replace, trim
from pyspark.sql.types import StructType, StructField, StringType

from data_transformations.wordcount.word_count_transformer import count_words


print("Hello")
logging.basicConfig(level=logging.DEBUG)
# Sample Spark session fixture
@pytest.fixture(scope="session")
def spark():
    return SparkSession.builder.master("local[1]").appName("UnitTest").getOrCreate()

def test_counting_words(spark):
    data = "In my younger and more vulnerable years my father--a great man--gave me some advice that I've been"
    
    word_count_df = count_words(spark, data)

    expected_data = [
        ("a", 1), ("advice", 1), ("and", 1), ("been", 1), ("father", 1), ("gave", 1), ("great", 1),
        ("i've", 1), ("in", 1), ("man", 1), ("me", 1), ("more", 1), ("my", 2), ("some", 1), ("that", 1), 
        ("vulnerable", 1), ("years", 1), ("younger", 1)
    ]
    expected_df = spark.createDataFrame(expected_data, ["word", "count"])
    
    sorted_word_count_df = word_count_df.orderBy("word")
    sorted_expected_df = expected_df.orderBy("word")
    
    print("Word Count DataFrame Schema:")
    word_count_df.printSchema()

    print("Expected DataFrame Schema:")
    expected_df.printSchema()
    
    # Collect the sorted DataFrames
    word_count_list = sorted_word_count_df.collect()
    expected_list = sorted_expected_df.collect()
# Print the lengths of the lists to verify they were collected
    logging.debug(f"Number of rows in actual DataFrame: {len(word_count_list)}")
    logging.debug(f"Number of rows in expected DataFrame: {len(expected_list)}")

    # Print the lengths of the lists to verify they were collected
   # print(f"Number of rows in actual DataFrame: {len(word_count_list)}")
    #print(f"Number of rows in expected DataFrame: {len(expected_list)}")

    # Compare the collected lists
    assert word_count_list == expected_list
     # Print the content of the actual DataFrame
    logging.debug("Actual DataFrame:")
    for row in word_count_list:
        logging.debug(row.asDict())

    # Print the content of the expected DataFrame
    logging.debug("Expected DataFrame:")
    for row in expected_list:
        logging.debug(row.asDict())
     
     # Print the content of the actual DataFrame
    # print("Actual DataFrame:")
    # for row in word_count_list:
    #     print(row.asDict())

    # Print the content of the expected DataFrame
    # print("Expected DataFrame:")
    # for row in expected_list:
    #     print(row.asDict())


    # assert word_count_df.collect() == expected_df.collect()