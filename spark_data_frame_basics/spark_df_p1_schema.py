from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("Basics").getOrCreate()
df = spark.read.json("people.json")
df.show()

df.printSchema()
df.describe().show() # It will bring the descriptive statistics
df.describe()
df.columns

from pyspark.sql.types import StructField, StringType, IntegerType, StructType

data_schema = [StructField('age', IntegerType(), True),
               StructField('name', StringType(), True)] # True means if there is a null, it will ignore it

final_structure = StructType(fields=data_schema)

df = spark.read.json('people.json', schema=final_structure)
df.printSchema()


df['age'] # It will return a column

df.select('age') # It will return df

df.head(2) # takes first 2 row

df.select(['age', 'name']).show()

df.withColumn('newColumn', df['age']*2).show() # generate new column it's not adding to df

df.withColumnRenamed('age', 'new_age_name').show() #it's not adding to df

df.show()

df.createOrReplaceTempView('people_table')
result = spark.sql('SELECT * FROM people_table')
result.show()

age_over_25 = spark.sql('SELECT name FROM people_table WHERE age > 25')
age_over_25.show()


