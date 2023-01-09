from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("ops").getOrCreate()

# header true will take first row as an column names
df = spark.read.csv('ContainsNull.csv', inferSchema=True, header=True)


df.show()
df.na.drop() # everything has na will be dropped
df.na.drop(thresh=2).show() # it means in a row at least two null value so it can drop
df.na.drop(how='all').show() # it means in a row has with all null
df.na.drop(subset=['Sales']).show() # drop if sales missing (null)

df.na.fill("No Name", subset=['Name']).show() # If Name missing put no name

from pyspark.sql.functions import mean

mean = df.select(mean(df['Sales'])).collect()[0][0]
print(mean)
df.na.fill(mean, subset=['Sales']).show()


