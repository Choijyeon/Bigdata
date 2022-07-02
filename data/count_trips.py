# Load pyspark, pandas
from pyspark import SparkConf, SparkContext
import pandas as pd

# Configure Spark
conf = SparkConf().setMaster("local").setAppName("uber-date-trips")
sc = SparkContext(conf=conf)

# Dataset sample
filename = "fhvhv_tripdata_2020-03.csv"

# Data parsing
lines = sc.textFile("./data/" + filename)   # .csv -> RDD object
header = lines.first()
filtered_lines = lines.filter(lambda row:row != header) # all lines excepting the header

# Run the Spark job
"""
    Run the Spark job
    - map(): apply the transformation on every element of RDD -> new RDD
    - countByValue(): action that returns the count of each unique value
    - x.split(", ")[2]: extract the value of pickup_datetime from a row
    e.g., 2020-03-01 00:03:40
    - .split(" ")[0]: extract the date from the pickup_datetime
    e.g., 2020-03-01
"""
dates = filtered_lines.map(lambda x: x.split(",")[2].split(" ")[0])
result = dates.countByValue()

# Save results as a csv file
pd.Series(result, name="trips").to_csv("trips_date.csv")

# Visualize the results
import matplotlib.pyplot as plt

trips = pd.read_csv("trips_date.csv")
trips.plot()
plt.show()
