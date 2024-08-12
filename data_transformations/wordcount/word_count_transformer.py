from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import explode, split, col, lower, regexp_replace
from pyspark.sql.types import Row
import logging

def count_words(spark: SparkSession, input_text: str) -> DataFrame:
    """
    Processes the input text to count the occurrences of each word.
    This function is incomplete and is meant to be implemented by the candidate.

    Parameters:
    - spark: SparkSession object
    - input_text: A string containing the text to be processed

    Returns:
    - DataFrame: A DataFrame containing the word counts with columns 'word' and 'count'
    """
    # Placeholder: The candidate needs to implement this function to
    # tokenize the text, count word occurrences, and return a DataFrame.
    
    # Create a DataFrame from the input text
    input_df = spark.createDataFrame([Row(value=input_text)])
    words_df = input_df.select(
        explode(
            split(
                regexp_replace(lower(col("value")), "[^a-zA-Z0-9\\s']", " "),  # Remove punctuation and fill it with space
                "\\s+"  # Split by whitespace
            )
        ).alias("word")
    ).filter(col("word") != "")  # Filter out empty strings
     # Group by word and count occurrences
    word_count_df = words_df.groupBy("word").count()
    
    # .withColumnRenamed("count", "count")

    return word_count_df
    # The candidate needs to implement the text processing steps here:
    # 1. Remove punctuation and convert text to lowercase.
    # 2. Split text into individual words.
    # 3. Explode the words into separate rows.
    # 4. Group by word and count occurrences.
    # 5. Return the resulting DataFrame with columns 'word' and 'count'.
    
    return input_df

def run(spark: SparkSession, input_path: str, output_path: str) -> None:
    """
    Main function to read a text file, count word occurrences, and write the results to a CSV file.
    This function is incomplete and is meant to be expanded by the candidate.

    Parameters:
    - spark: SparkSession object
    - input_path: Path to the input text file
    - output_path: Directory path where the output CSV file will be saved
    """
    logging.info("Reading text file from: %s", input_path)
    
    # Use Spark's read functionality to read the input text file
    input_df = spark.read.text(input_path)
    
    # Convert the DataFrame to a single string
    input_text = " ".join(row.value for row in input_df.collect())
    
    # Count words in the input text
    word_count_df = count_words(spark, input_text)

    logging.info("Writing word counts to directory: %s", output_path)
    # Write the word counts to a CSV file
    word_count_df.coalesce(1).write.csv(output_path, header=True)

# The candidate needs to ensure the count_words function tokenizes the text,
# performs necessary transformations, and counts the occurrences of each word.
# They should also consider edge cases such as punctuation, case sensitivity,
# and handling large text inputs efficiently.
