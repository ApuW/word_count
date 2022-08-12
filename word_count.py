from pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark = SparkSession. \
    builder. \
    master('local'). \
    appName('Word Count'). \
    getOrCreate()

df = spark.read.text('C:/Users/Apurva Waghmode/PycharmProjects/word_count/WORD_COUNT.txt').toDF('word')

word_count = df. \
    select(explode(split('word', ' ')).alias('word')). \
    groupBy('word'). \
    agg(count(lit(1)).alias('word_count'))
word_count.show()