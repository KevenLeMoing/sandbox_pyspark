from pyspark.sql import SparkSession
from pyspark.sql.types import StructField, StructType, StringType, IntegerType
from pyspark.sql.functions import (avg,
                                   stddev,
                                   format_number,
                                   mean,
                                   year)

spark = SparkSession.builder.appName('dataframe_basics').getOrCreate()
df = spark.read.json('data/people.json')
df.show()
df.printSchema()
df.describe().show()
print(df.columns)

manual_schema = [StructField('age', IntegerType(), True),
                 StructField('name', StringType(), True)]
struct = StructType(fields=manual_schema)
df2 = spark.read.json(path='../data/people.json', schema=struct)
df2.printSchema()

print('^^^^^^')
print(type(df['age']))
print(type(df.select('age')))
print(type(df.head(2)[0]))
print(type())
print(type(df.select(['age', 'name'])))
df.withColumn('triple_age', df2['age']*3).show()
df.withColumnRenamed('age', 'new_age').show()
df.createOrReplaceTempView('people')
query = spark.sql('SELECT * FROM people')
query.show()

df3 = spark.read.csv('data/appl_stock.csv', inferSchema=True, header=True)
df3.filter((df3['Close'] < 200) & (df3['Open'] > 200)).show()
result_filter = df3.filter(df3['Low'] == 197.16).collect()
row = result_filter[0]
print(row.asDict()['Volume'])
df3.withColumn('Year', year(df3['Date'])).show()

df4 = spark.read.csv('data/sales_info.csv', inferSchema=True, header=True)
group_data = df4.groupby('Conpany')
group_data.agg({'Sales': 'max'}).show()
df4.select(avg('Sales').alias('average_sales')).show()
std = df4.select(stddev('Sales').alias('std'))
std.select(format_number('std', 2).alias('std')).show()
df4.orderBy('Sales').show()

df5 = spark.read.csv('data/ContainsNull.csv', inferSchema=True, header=True)
df5.show()
df5.na.drop(thresh=2).show()
df5.na.drop(subset=['Sales']).show()
df5.na.fill(value='no_name', subset=['Name']).show()
mean_val = df5.select(mean(df5['Sales'])).collect()
mean_sales = mean_val[0][0]
