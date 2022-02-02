# Spark Queries

import utils

from pyspark.sql.functions import col
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType

def run_spark_queries(spark, query):

    df = utils.read_table(spark, "gold", "certificates")

    if (query == "q1"):
        print("average complete time of a course")
        auxDF = df \
            .withColumn("completeTimeOfACourse", (col("completeddate").cast(IntegerType()) - col("startdate").cast(IntegerType())) / (60 * 60)) \
            .agg(F.avg("completeTimeOfACourse"))

    elif (query == "q2"):
        print("average amount of users time spent in a course")
        auxDF = df \
            .withColumn("timeSpentInACourse", (col("completeddate").cast(IntegerType()) - col("startdate").cast(IntegerType())) / (60 * 60)) \
            .groupBy(col("firstname"), col("lastname"), col("email")) \
            .avg("timeSpentInACourse")

    elif (query == "q3"):
        print("average amount of users time spent for each course individually")
        auxDF = df \
            .withColumn("dateDifferenceInHours", (col("completeddate").cast(IntegerType()) - col("startdate").cast(IntegerType())) / (60 * 60)) \
            .groupBy(col("title")) \
            .avg("dateDifferenceInHours")

    elif (query == "q4"):
        print("report of fastest vs. slowest users completing a course")
        auxDF = df \
            .withColumn("dateDifferenceInHours", (col("completeddate").cast(IntegerType()) - col("startdate").cast(IntegerType())) / (60 * 60))

        auxDF_min =  str(auxDF \
                .agg(F.min("dateDifferenceInHours")) \
                .first()[0])

        auxDF_max = str(auxDF \
                .agg(F.max("dateDifferenceInHours")) \
                .first()[0])

        auxDF = auxDF \
            .filter((col("dateDifferenceInHours") == auxDF_min) | (col("dateDifferenceInHours") == auxDF_max))
    
    elif (query == "q5"):
        print("amount of certificates per customer")
        auxDF = df \
            .groupBy(col("firstname"), col("lastname"), col("email")) \
            .count()

    auxDF.show(50)