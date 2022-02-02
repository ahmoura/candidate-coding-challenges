import utils
import postgres
import sql_queries
import spark_queries

from pyspark.sql.functions import udf
from pyspark.sql.functions import col
from pyspark.sql.types import TimestampType, IntegerType
from datetime import datetime



databases = ["bronze", "silver", "gold"]
files = ["users", "courses", "certificates"]

spark, sc = utils.create_spark_env()

## Set all databases
conn, cursor = postgres.open_cursor_and_conn()
for db in databases:
    postgres.create_database(cursor, db)
postgres.close_cursor_and_conn(conn, cursor)

## Ingest data inside raw layer (bronze)
usersDF = utils.read_files(spark, "../spark-data/files", "users.json")
coursesDF = utils.read_files(spark, "../spark-data/files", "courses.json")
certificatesDF = utils.read_files(spark, "../spark-data/files", "certificates.json")

conn, cursor = postgres.open_cursor_and_conn_with_db("bronze")
for f in files:
    postgres.create_bronze_table(f, cursor, conn)
postgres.close_cursor_and_conn(conn, cursor)

utils.write_files(usersDF,"bronze", "users")
utils.write_files(coursesDF,"bronze", "courses")
utils.write_files(certificatesDF,"bronze", "certificates")

## Ingest data inside treated layer (silver)
usersDF = utils.read_table(spark, "bronze", "users")
coursesDF = utils.read_table(spark, "bronze", "courses")
certificatesDF = utils.read_table(spark, "bronze", "certificates")

joinedDF = certificatesDF \
    .join(usersDF, certificatesDF.user == usersDF.id, "left") \
    .drop(col("id")) \
    .join(coursesDF, certificatesDF.course == coursesDF.id, "left") \
    .drop(col("id")) \
    .distinct()

conn, cursor = postgres.open_cursor_and_conn_with_db("silver")
postgres.create_silver_table("certificates", cursor, conn)
postgres.close_cursor_and_conn(conn, cursor)

utils.write_files(joinedDF,"silver", "certificates")

## Ingest data inside business layer (gold)
certificatesDF = utils.read_table(spark, "silver", "certificates")

to_date = udf(lambda x: datetime.strptime(x, '%Y-%m-%dT%H:%M:%S.%fZ'), TimestampType())
now_time = udf(lambda: datetime.now(), TimestampType())

certificatesDF = certificatesDF \
    .drop(col("user")) \
    .drop(col("course")) \
    .drop(col("description")) \
    .drop(col("publishedat")) \
    .withColumn("startdate", to_date(col("startdate"))) \
    .withColumn("completeddate", to_date(col("completeddate"))) \
    .distinct()

conn, cursor = postgres.open_cursor_and_conn_with_db("gold")
postgres.create_gold_table("certificates", cursor, conn)
postgres.close_cursor_and_conn(conn, cursor)

utils.write_files(certificatesDF,"gold", "certificates")

## Running Queries

queries = ["q1","q2","q3","q4","q5"]

for query in queries:
    sql_queries.run_sql_queries(sc, query)
    spark_queries.run_spark_queries(spark, query)


## Data Visualization

### You provide a possibility to visualize the data in a ui tool with graphs
### You provide addtional possibilities of analyzing the data via that ui tool (e.g. which times users start mostly, most frequent used courses, etc.)