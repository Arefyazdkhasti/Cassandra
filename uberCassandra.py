from cassandra.cluster import Cluster
import datetime

def toDateTime(timestamp):
    date_time_obj = datetime.datetime.fromtimestamp(timestamp)
    return date_time_obj.strftime('%m/%d/%Y %H:%M:%S')


# Connect to the Cassandra cluster
cluster = Cluster(['127.0.0.1'])
session = cluster.connect()

result_set = session.execute("SELECT * FROM uberKeySpace.prediction")
index = 0
for row in result_set:
    # print (row)
    index+=1
print(index)    


# Group the stored data according to the obtained cluster number and get the number of data in each cluster. approach 1
for i in range(0,5): 
    numberOfCluster = 0
    predictionWithTheirCount = session.execute(f"SELECT * FROM uberKeySpace.prediction WHERE prediction={i} ALLOW FILTERING")
    for row in predictionWithTheirCount:
        #print(row) 
        numberOfCluster += 1

    print(f"Number of item in cluset {i} -> {numberOfCluster}")  


# Group the stored data according to the obtained cluster number and get the number of data in each cluster. approach 2
session.execute("""
   CREATE MATERIALIZED VIEW IF NOT EXISTS uberKeySpace.prediction_by_prediction AS
   SELECT *
   FROM uberKeySpace.prediction
   WHERE id IS NOT NULL AND prediction IS NOT NULL
   PRIMARY KEY (prediction, id)
   WITH CLUSTERING ORDER BY (id ASC)
""")
numberOfDataInEachCluster = session.execute("SELECT prediction, count(*) FROM uberKeySpace.prediction_by_prediction GROUP BY prediction ")
for row in numberOfDataInEachCluster:
    print(f"Cluster {row[0]} -> {row[1]}")


# Define time intervals
week1_start = datetime.datetime.fromtimestamp(1406812800)
week1_end = datetime.datetime.fromtimestamp(1407638399)
week2_start = datetime.datetime.fromtimestamp(1407638400)
week2_end = datetime.datetime.fromtimestamp(1408252799)
week3_start = datetime.datetime.fromtimestamp(1408252800)
week3_end = datetime.datetime.fromtimestamp(1408867199)
week4_start = datetime.datetime.fromtimestamp(1408867200)
week4_end = datetime.datetime.fromtimestamp(1409529540)

# Construct WHERE clauses for each week
where_clauses = [
    f"timestamp >= {int(week1_start.timestamp())} AND timestamp <= {int(week1_end.timestamp())}",
    f"timestamp >= {int(week2_start.timestamp())} AND timestamp <= {int(week2_end.timestamp())}",
    f"timestamp >= {int(week3_start.timestamp())} AND timestamp <= {int(week3_end.timestamp())}",
    f"timestamp >= {int(week4_start.timestamp())} AND timestamp <= {int(week4_end.timestamp())}"
]


# Available services in one week
session.execute("""
   CREATE MATERIALIZED VIEW IF NOT EXISTS uberKeySpace.prediction_by_base AS
   SELECT *
   FROM uberKeySpace.prediction
   WHERE id IS NOT NULL AND base IS NOT NULL
   PRIMARY KEY (base, id)
   WITH CLUSTERING ORDER BY (id ASC , c DESC)
""")

week = 1                
for i, clause in enumerate(where_clauses):
    print(f"Week{week} -------------------- ")
    week += 1

    availableSessionsInAWeek = session.execute(f"""SELECT Base, timestamp, count(*) 
                        FROM uberKeySpace.prediction_by_base 
                        WHERE {clause}
                        GROUP BY Base ALLOW FILTERING""")
    
    for row in availableSessionsInAWeek:
        print(row)
        print(toDateTime(row.timestamp))


session.execute("""
   CREATE MATERIALIZED VIEW IF NOT EXISTS uberKeySpace.busient_cluster AS
   SELECT *
   FROM uberKeySpace.prediction
   WHERE id IS NOT NULL AND prediction IS NOT NULL
   PRIMARY KEY (prediction, id)
   WITH CLUSTERING ORDER BY (id ASC)
""")
                
# Query Cassandra for busiest cluster in each week
busiest_clusters_per_week = []
for i, where_clause in enumerate(where_clauses):
    query = f"SELECT prediction, count(*) FROM uberKeySpace.busient_cluster WHERE {where_clause} GROUP BY prediction ALLOW FILTERING"
    result = session.execute(query)
    sorted_result = sorted(result, key=lambda x: x[1], reverse=True)
    cluster_num = sorted_result[0][0]
    busiest_clusters_per_week.append(cluster_num)
    
    print(f"Week {i+1}:-----------------------------")

    for row in sorted_result:
        print(row)
        

session.execute("DROP MATERIALIZED VIEW IF EXISTS uberKeySpace.prediction_by_lat")
session.execute("""
   CREATE MATERIALIZED VIEW IF NOT EXISTS uberKeySpace.prediction_by_lat AS
   SELECT *
   FROM uberKeySpace.prediction
   WHERE id IS NOT NULL AND lat IS NOT NULL
   PRIMARY KEY (lat, id)
   WITH CLUSTERING ORDER BY (id ASC);
""")
session.execute("""
   CREATE MATERIALIZED VIEW IF NOT EXISTS uberKeySpace.prediction_by_lon AS
   SELECT *
   FROM uberKeySpace.prediction
   WHERE id IS NOT NULL AND lon IS NOT NULL
   PRIMARY KEY (lon, id)
   WITH CLUSTERING ORDER BY (id ASC);
""")    

# Calculate the timestamp from exactly 10 days ago
ten_days_ago = datetime.datetime(2014, 8, 31, 23, 59) - datetime.timedelta(days=10)
ten_days_ago_timestamp = int(ten_days_ago.timestamp())

# Query Cassandra for high traffic points in the past 10 days
results_GB_Lat = session.execute(f"SELECT lat, lon, count(*) FROM uberKeySpace.prediction_by_lat WHERE timestamp >= {ten_days_ago_timestamp} GROUP BY lat ALLOW FILTERING")

results_GB_Lon = session.execute(f"SELECT lat, lon, count(*) FROM uberKeySpace.prediction_by_lon WHERE timestamp >= {ten_days_ago_timestamp} GROUP BY lon ALLOW FILTERING")

# Define empty dictionaries to store the results
d1 = {}
d2 = {}

# Add the rows of each result set to the corresponding dictionary
for row in results_GB_Lat:
    d1[(row.lat, row.lon)] = row.count

for row in results_GB_Lon:
    d2[(row.lat, row.lon)] = row.count

# Merge the two dictionaries into a single dictionary
merged_dict = d1.copy()
merged_dict.update(d2)

sorted_dict = dict(sorted(merged_dict.items(), key=lambda item: item[1], reverse=True))

# Print the results
index = 0
for lat, lon in sorted_dict:
    count = sorted_dict[(lat,lon)]
    print(f"lat: {lat}, lon: {lon}, count: {count}")
    index += 1
    if index >= 20:
        break