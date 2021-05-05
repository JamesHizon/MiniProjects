# Spark Mini-Project Python Script

# Steps:
# 1) Filter out accident incidents w/ make and year.
# 1a) Read the input data CSV file.
# 1b) Perform map operation.
# 1c) Perform group aggregation.
# 2) Count number of occurrence for accidents for the vehicle make and year.
# 2a) Perform map operation.
# 2b) Aggregate the key and count the number of records in total per key.
# 3) Save result to HDFS as text.
# 4) Use Shell script to run Spark jobs.

# Recall:
# - SparkContext is the entry point to any spark functionality. When we run any
# Spark application, a driver program starts, which has the main function and your
# SparkContext gets initiated here. The driver program then runs the operations inside
# the execution on worker nodes.
# - SparkContext uses Py4J to launch a JVM and creates a JavaSparkContext. By default,
# PySpark has SparkContext available as 'sc' so creating a SparkContext won't work.

# Import packages
from pyspark import SparkContext

# Read input data
sc = SparkContext("local", "My Application")
raw_rdd = sc.textFile("data.csv")

# Implement methods: extract_vin_key_value(), populate_make(), and
# extract_make_key_value()


def extract_vin_key_value():
    pass


def populate_make():
    pass


def extract_make_key_value():
    pass


# Propagate make and year to accident records (incident type A) using vin_number
# as the aggregate key (Map Output key: vin_number, value: make, year, incident_type).

vin_kv = raw_rdd.map(lambda x: extract_vin_key_value(x))

# -----> Please implement method extract_vin_key_value()

# Perform group aggregation to populate make and year to all the records.

enhance_make = vin_kv.groupByKey().flatMap(lambda kv: populate_make(kv[1]))

# -----> Please implement method populate_make()

# Count number of occurrence for accidents for the vehicle make and year

make_kv = enhance_make.map(lambda x: extract_make_key_value(x))

# -----> Please implement method extract_make_key_value()

# Aggregate the key and count the number of records in total per key.

# -----> Use Spark provided "reduceByKey" function to perform the sum of all the
# values from each record. As a result, we get the make and year combination key
# along w/ its total record count.













