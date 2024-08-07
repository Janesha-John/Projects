import pytest
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, explode, lower, split, regexp_replace, trim
from pyspark.sql.types import StructType, StructField, StringType

from data_transformations.wordcount.word_count_transformer import count_words

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

    assert word_count_df.collect() == expected_df.collect()