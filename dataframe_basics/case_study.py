from pyspark.sql import SparkSession
from pyspark.sql.functions import format_number, max, min, corr, year, month

spark = SparkSession.builder.appName('walmart_case_study').getOrCreate()
df = spark.read.csv('data/walmart_stock.csv', header=True, inferSchema=True)

# 1) Print out column names
print(df.columns)

# 2) Print out the schema
df.printSchema()

# 3) Print out the first 5 rows
rows = df.head(5)
for row in rows:
    print(row)

# 4) Describe the dataframe
df_describe = df.select(['Open', 'High', 'Low', 'Close', 'Volume']).describe()
df_describe.show()

# 5) Format describe output to two decimals numbers
df_describe.printSchema()
df_describe.select(format_number(df_describe['Open'].cast("float"), 2).alias('Open'),
                   format_number(df_describe['High'].cast("float"), 2).alias('High'),
                   format_number(df_describe['Low'].cast("float"), 2).alias('Low'),
                   format_number(df_describe['Close'].cast("float"), 2).alias('Close'),
                   format_number(df_describe['Volume'].cast("float"), 2).alias('Volume'),).show()

# 6) Create new column called "HV Ratio": High Price versus volume of stock traded for a day
df.withColumn('hv_ratio', df['High']/df['Volume']).show()

# 7) What day had the Peak High in Price?
print(df.orderBy(df["High"].desc()).head(1)[0][0])

# 8) What is the mean of the Close column?
df.agg({"Close": "avg"}).show()

# 9) What is the max and min of the Volume column?
df.select(max("Volume"), min("Volume")).show()

# 10) How many days was the Close lower than 60 dollars?
print(df.filter(df['Close'] < 60).count())

# 11) What percentage of the time was the High greater than 80 dollars ?
# In other words, (Number of Days High>80)/(Total Days in the dataset)
nb_days_80 = df.filter(df['High'] > 80).count()
nb_days_total = df.groupBy(df['Date']).count().count()
print(nb_days_80/nb_days_total)

# 12) What is the Pearson correlation between High and Volume?
df.select(corr(df['High'], df['Volume'])).show()

# 13) What is the max High per year?
df_with_year = df.withColumn('Year', year(df['Date']))
df_with_year.groupBy('Year'). agg({'High': 'max'}).show()

# 14) What is the average Close for each Calendar Month?
# In other words, across all the years, what is the average Close price for Jan,Feb, Mar, etc...
# Your result will have a value for each of these months.
df_with_month = df.withColumn('Month', month(df['Date']))
df_with_month.groupBy('Month'). agg({'Close': 'avg'}).show()
