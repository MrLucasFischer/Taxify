from pyspark.sql import *
from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark import SparkContext

spark = SparkSession.builder.master('local[*]').appName('taxify').getOrCreate()
sc = spark.sparkContext

