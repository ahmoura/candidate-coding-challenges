def create_spark_env():
    import findspark
    from pyspark.sql import SparkSession

    findspark.init()

    spark = SparkSession\
        .builder \
        .appName("Python Spark SQL - Perseus Challenge") \
        .config("spark.jars", "/opt/spark-data/jars/postgresql-42.3.1.jar") \
        .getOrCreate()
    sc = spark.sparkContext

    return spark, sc

def read_files(spark, path, file):
    import os

    return spark \
    .read \
    .option("multiline","true") \
    .json(os.path.join(path,file))

def write_files(df, db, table, mode = "append", url = "jdbc:postgresql://database/"):

    try:
        df \
        .write \
        .format("jdbc") \
        .option("url", url + db) \
        .option("driver", "org.postgresql.Driver") \
        .option("dbtable", table) \
        .option("user", "postgres") \
        .option("password", "perseus") \
        .mode(mode) \
        .save()
    except:
        print(f"ERROR: duplicate key value violates unique constraint")

def read_table(spark, db_name, t_name):
    return spark.read \
    .format("jdbc") \
    .option("url", "jdbc:postgresql://database/" + db_name) \
    .option("dbtable", t_name) \
    .option("user", "postgres") \
    .option("password", "perseus") \
    .option("driver", "org.postgresql.Driver") \
    .load()

def read_sql_table(sc, db_name, t_name):
    from pyspark.sql import SQLContext

    return SQLContext(sc) \
    .read \
    .format("jdbc") \
    .option("url", "jdbc:postgresql://database/" + db_name) \
    .option("dbtable", "(" + t_name + ") t") \
    .option("user", "postgres") \
    .option("password", "perseus") \
    .option("driver", "org.postgresql.Driver") \
    .option("useUnicode", "true") \
    .option("continueBatchOnError","true") \
    .option("useSSL", "false") \
    .load()
