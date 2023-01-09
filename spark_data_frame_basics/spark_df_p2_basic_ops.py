from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("ops").getOrCreate()

# header true will take first row as an column names
df = spark.read.csv('appl_stock.csv', inferSchema=True, header=True)
df.printSchema()

df.filter("Close < 500").select('Open').show()

# OR

df.filter(df['Close'] < 500).select('Open').show()

# Two or more conditions
'''
Operators
    & --> And
    | --> Or
    ~ --> Not
'''

df.filter((df['Close'] < 500) & (df['Open'] > 200)).show()

open_results = df.filter(df['Close'] < 500).collect()

a = open_results[0]['High'] # will give the first row
print(a)
result = open_results[0].asDict() # turn into dict
print(result)

