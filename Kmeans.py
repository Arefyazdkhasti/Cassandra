from pyspark.sql import SparkSession
from pyspark.ml.clustering import KMeans
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.evaluation import BinaryClassificationEvaluator
from pyspark.sql.functions import col
import matplotlib.pyplot as plt
from pyspark.ml.evaluation import ClusteringEvaluator
from cassandra.cluster import Cluster
import datetime

# Define the Cassandra cluster
cluster = Cluster(['127.0.0.1'])
session = cluster.connect()

# Create a keyspace and a table in Cassandra
session.execute("""
    CREATE KEYSPACE IF NOT EXISTS uberKeySpace
    WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': 1}
""")

# session.execute("DROP TABLE IF EXISTS uberKeySpace.prediction")
session.execute("""
    CREATE TABLE IF NOT EXISTS uberKeySpace.prediction (
        id int,
        timestamp int,
        lat double,
        lon double,
        base text,
        prediction int,
        PRIMARY KEY (id)
    )
""")

def toTimeStamp(str):
    date_time_obj = datetime.datetime.strptime(str, '%m/%d/%Y %H:%M:%S')
    timestamp = int(date_time_obj.timestamp())
    return timestamp

# Create a SparkSession object
spark = SparkSession.builder.appName("UberAnalysis").getOrCreate()

# Read the data from CSV file into a DataFrame
df = spark.read.csv("uber.csv", header=True, inferSchema=True)

# Combine the 'Lat' and 'Lon' columns into a single feature vector column
assembler = VectorAssembler(inputCols=['Lat', 'Lon'], outputCol='features')
data = assembler.transform(df.select('Date/Time','Lat', 'Lon','Base'))


# Split the data into training and testing sets with a 80:20 ratio
(trainingData, testData) = data.randomSplit([0.8, 0.2], seed=0)

testData.toPandas().to_csv('user_fullTestData.csv')

# Train the K-means clustering model on the training set
kmeans = KMeans(k=5, seed=0)
model = kmeans.fit(trainingData)

# Predict the cluster labels for the testing set
predictions = model.transform(testData)

# Show predictions
predictions.show(20)

# Calculate the Silhouette score
evaluator = ClusteringEvaluator()
silhouette = evaluator.evaluate(predictions)
print("Silhouette score1: " + str(silhouette))

# Convert the PySpark DataFrame to a list of tuples
data_list = predictions.select("Date/Time", "Lat", "Lon", "Base", "prediction").collect()

index = 0
# Insert the data into the Cassandra table
for data in data_list:
    print(data)
    timestamp = toTimeStamp(data[0])
    session.execute("INSERT INTO uberKeySpace.prediction(id, timestamp, lat, lon, base, prediction) VALUES (%s, %s, %s, %s, %s, %s)", (index, timestamp, data[1], data[2], data[3],data[4]))
    index += 1
# Retrieve the data from Cassandra
rows = session.execute("SELECT * FROM uberKeySpace.prediction")

# Convert the data to a PySpark DataFrame
data = spark.createDataFrame(rows)

# Show the data
data.show(20)