from pyspark.sql import SparkSession, SQLContext


def get_spark():
    return (SparkSession.builder
            .appName("Training")
            .enableHiveSupport()
            .getOrCreate())


def get_sqlcontext():
    sc = get_spark().sparkContext
    sqlcontext = SQLContext(sc)
    return sqlcontext


def get_test_spark():
    return (SparkSession.builder
            .master("local[4]")
            .config("spark.jars.packages")
            .appName("unit_test_case")
            .enableHiveSupport()
            .getOrCreate())
