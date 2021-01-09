from pyspark.sql import SparkSession
from pyspark.sql.types import StructField, StructType, StringType, IntegerType

spark = SparkSession.builder.appName('dataframe_basics').getOrCreate()
df = spark.read.json('data/people.json')
df.show()
df.printSchema()
df.describe().show()
print(df.columns)

manual_schema = [StructField('age', IntegerType(), True),
                 StructField('name', StringType(), True)]
struct = StructType(fields=manual_schema)
df2 = spark.read.json(path='data/people.json', schema=struct)
df2.printSchema()

