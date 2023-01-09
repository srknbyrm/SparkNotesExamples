from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("ops").getOrCreate()

# header true will take first row as an column names
df = spark.read.csv('sales_info.csv', inferSchema=True, header=True)
df.printSchema()

'''
+-------+-----+
|Company|count|
+-------+-----+
|   APPL|    4|
|   GOOG|    3|
|     FB|    2|
|   MSFT|    3|
+-------+-----+
'''
df.groupBy('Company').count().show()

'''
+----------+
|sum(Sales)|
+----------+
|    4327.0|
+----------+
'''
df.agg({'Sales':'sum'}).show()

'''
Per company max salary data
'''
companies = df.groupBy('Company')
companies.agg({'Sales':'max'}).show()

from pyspark.sql.functions import countDistinct, avg, stddev, format_number

df.select(countDistinct('Sales')).show()
df.select(avg('Sales').alias('avg salary')).show()

sales_std = df.select(stddev('Sales').alias('std'))
sales_std.select(format_number('std', 2).alias('std'))

# Order By
df.orderBy(df['Sales']).show()
df.orderBy(df['Sales'].desc()).show()


