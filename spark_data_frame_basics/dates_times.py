from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("datesandtimes").getOrCreate()
df = spark.read.csv('appl_stock.csv', inferSchema=True, header=True)
df.select([df['Date'], df["Open"]]).show()

from pyspark.sql.functions import dayofmonth, hour, dayofyear, month, year, weekofyear, date_format, mean

df.select(year(df['Date'])).show()
new_df = df.withColumn('Year', year(df['Date']))
new_df.show()
result = new_df.groupBy('Year').mean().select(['Year', 'avg(Close)'])
result.show()

result = new_df.select(mean('Close').alias('Average')).show()