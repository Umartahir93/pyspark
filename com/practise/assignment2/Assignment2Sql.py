from pyspark.sql import SparkSession
from pyspark.sql import Row

#Database constants
url = "jdbc:postgresql://localhost:5432/postgres"
username= "postgres"
password = "admin"
driver="org.postgresql.Driver"
customertablename= "public.pysparkpractise_customer"
occupationtablename="public.pysparkpractise_occupation"


# Create a SparkSession
spark = SparkSession.builder.master("local[*]").config("spark.jars", "postgresql-42.2.9.jar").appName("SparkSQL").getOrCreate()

customerDataFrame = spark.read \
    .format("jdbc") \
    .option("url", url) \
    .option("dbtable", customertablename) \
    .option("user", username) \
    .option("password", password) \
    .option("driver", driver) \
    .load()

occupationDataFrame = spark.read \
    .format("jdbc") \
    .option("url", url) \
    .option("dbtable", occupationtablename) \
    .option("user", username) \
    .option("password", password) \
    .option("driver", driver) \
    .load()

# Register DataFrame as temporary views
customerDataFrame.createOrReplaceTempView("customer")
occupationDataFrame.createOrReplaceTempView("occupation")

#joining table and saving database
customers = spark.sql("SELECT * FROM customer join occupation on customer.id = occupation.cust_id")
customers.write.mode("overwrite").saveAsTable("Customer_Occupation")
customers.collect()


result = spark.sql("SELECT * from Customer_Occupation")
result.show()

spark.stop()










