# PySpark Practice

# Example:
# - We will be counting the number of lines w/ character 'a' or 'b' in the README.md file.
# - So, let us say if there are 5 lines in a file and 3 lines have the character 'a', then
# the output will be --> Line with a: 3. Same for character 'b'.

# Note:
# - We will not be creating any SparkContext object in the following example because by
# default, Spark automatically creates the SparkContext object named sc,
# when PySpark shell starts. In case you try to create another SparkContext object,
# you wil get the following error - "ValueError: Cannot run multiple SparkContexts
# at once.


# SparkContext Example - Python Program
# - Let us run the example using a Python program. Create a Python file called
# firstapp.py and enter the following code in that file.

# ------------------------------firstapp.py--------------------------------------

from pyspark import SparkContext
sc = SparkContext("local", "First App")
logFile = "file:///home/hadoop/spark-.../README.md"
logData = sc.textFile(logFile).cache()
# for all s in logData, we will obtain count if character exists
numAs = logData.filter(lambda s: 'a' in s).count()
numBs = logData.filter(lambda s: 'b' in s).count()

# ------------------------------firstapp.py--------------------------------------







