from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, to_timestamp, count, radians, sin, cos, sqrt, asin, lag, sum as _sum, round as spark_round, count_distinct, count
)
from pyspark.sql.window import Window
from pyspark.sql.types import DoubleType
import os
import findspark
import folium
import pandas
from sklearn.cluster import DBSCAN
from pyspark.ml.clustering import KMeans
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.clustering import KMeans
from pyspark.ml.feature import VectorAssembler
from pyspark.sql.functions import col
import matplotlib.pyplot as plt
from pyspark.sql.functions import countDistinct, col
import folium
import builtins

# Staring up pyspark, based on the individual setup
# os.environ["SPARK_LOCAL_IP"] = "IP"
# os.environ["PYSPARK_PYTHON"] = r"C:\.....\pyspark_env\python.exe"
# os.environ["PYSPARK_DRIVER_PYTHON"] = r"C:\....\pyspark_env\python.exe"

findspark.init()
spark = (
    SparkSession.builder
    .master("local[8]")
    .appName("PortDetectionTask")
    .getOrCreate()
    )

# Read ship data
df = spark.read.csv("aisdk-2025-04-20.csv",
    header=True,
    inferSchema=True
)

# Define functions used for this task
def clean_data(df):
    '''
    Takes a dataframe, returns a cleaned one.

    Only take useful columns, drop NA values, cast to appopriate data types.
    Remove ships with non-standart coordinate types, helicopters.
    '''
    # Some standard cleaning
    df_clean = (
        df.select("MMSI", "# Timestamp", "Latitude", "Longitude", "SOG", "Ship type")
        .dropna(subset=["MMSI", "# Timestamp", "Latitude", "Longitude", "SOG"])
        .withColumn("Latitude", col("Latitude").cast(DoubleType()))
        .withColumn("Longitude", col("Longitude").cast(DoubleType()))
        .withColumn("SOG", col("SOG").cast(DoubleType()))
        .withColumn("Timestamp", to_timestamp(col("# Timestamp"), "dd/MM/yyyy HH:mm:ss"))
    )

    # Cleaning up the ships with weird coordinates
    df_clean = df_clean.filter(
        (col("Latitude").between(-90, 90)) &
        (col("Longitude").between(-180, 180))
    )

    # Remove search and rescue helicopters from the analysis
    df_clean = df_clean.filter(
        col("Ship type") != "SAR"
    )

    return df_clean


def filter_moving_ships(df_clean, sog_filter=0.5, sog_number_filter=5, total_distance_filter=10.0):
    '''
    Takes a dataframe and filtering parameters, returns a filtered one of stationary vessels.

    Remove vessels based on these two conditions:
        1. Moving vessels that send speed of 0.5 or higher for more than 5 times.
        2. Ones that have traveled for more than 10 kilometers in the given day.
    '''
    # Ships that are sending SOG >= (0.5) for more than (5) time are excluded.
    low_speed_df = df_clean.filter(col("SOG") < sog_filter)

    dwelling_ships = (
        low_speed_df.groupBy("MMSI")
                    .agg(count("*").alias("low_speed_msgs"))
                    .filter(col("low_speed_msgs") >= sog_number_filter)
    )

    slow_moving_df = (
        low_speed_df.join(dwelling_ships, on="MMSI", how="inner")
    )

    # Display the slow ships
    total_slow_moving_ships = slow_moving_df.select("MMSI").distinct().count()
    print(f"Total stationary/drifting ships: {total_slow_moving_ships}")

    slow_moving_df.show(5, truncate=False)

    # Now distance filtering is performed
    low_speed_filtered = slow_moving_df

    # Window.partition function for previous coordinates
    windowSpec = Window.partitionBy("MMSI").orderBy("Timestamp")

    df_distance = (
        low_speed_filtered.withColumn("prev_lat", lag("Latitude").over(windowSpec))
                        .withColumn("prev_lon", lag("Longitude").over(windowSpec))
                        .dropna(subset=["prev_lat", "prev_lon"])
    )

    # Convert to radians for distance calculation
    df_distance = (
        df_distance.withColumn("lat1", radians(col("prev_lat")))
                .withColumn("lon1", radians(col("prev_lon")))
                .withColumn("lat2", radians(col("Latitude")))
                .withColumn("lon2", radians(col("Longitude")))
    )


    # Calculate haversine distance in pyspark SQL
    df_distance = (
        df_distance.withColumn("dlat", col("lat2") - col("lat1"))
                .withColumn("dlon", col("lon2") - col("lon1"))
                .withColumn("a", sin(col("dlat") / 2) ** 2 +
                                    cos(col("lat1")) * cos(col("lat2")) *
                                    sin(col("dlon") / 2) ** 2)
                .withColumn("c", 2 * asin(sqrt(col("a"))))
                .withColumn("segment_km", col("c") * 6371) 
    )

    # Total distance traveled by MMSI
    total_distance = (
        df_distance.groupBy("MMSI")
                .agg(_sum("segment_km").alias("total_distance_km"))
    )

    # Filter ships that traveled more than (10)km
    stationary_ships = total_distance.filter(col("total_distance_km") < total_distance_filter)

    # Join the filtered low speed ships and the filtered small distance traveled ships
    df_filtered = low_speed_filtered.join(stationary_ships, on="MMSI", how="inner")

    return df_filtered


def bin_ships(df, filter_ships=5):
    '''
    Bin ships to clusters based on lattidute and longitude.
    Grid size is approximately 0.64 x 1.11km rectangles (roughly 0.7km squared), may differ depending on latidute and longitude.
    '''
    # Round of the coordinates to create bins
    binned_df = (
        df.withColumn("lat_bin", spark_round(col("Latitude"), 2))
        .withColumn("lon_bin", spark_round(col("Longitude"), 2))
    )

    # Assing vessel count to bins and filter small potential ports
    port_candidates = (
        binned_df.groupBy("lat_bin", "lon_bin")
                .agg(count_distinct("MMSI").alias("vessel_count"))
                .filter(col("vessel_count") >= filter_ships)
    )
    
    # Print out the biggest ports
    port_candidates.orderBy(col("vessel_count").desc()).show(5, truncate=False)
    
    return port_candidates

# Alternative for bining ports to larger single ports (does not work as intended due to large grid)
def bin_ports(df):
    '''
    Bin ports to clusters based on lattidute and longitude.
    Grid cell size is approximately 6.4 x 11.1km rectangles (roughly 71km squared), may differ depending on latidute and longitude.
    '''

    binned_df = (
        df.withColumn("lat_bin_ports", spark_round(col("lat_bin"), 1))
        .withColumn("lon_bin_ports", spark_round(col("lon_bin"), 1))
    )
    
    port_candidates_agg = (
        binned_df.groupBy("lat_bin_ports", "lon_bin_ports")
                .agg(_sum("vessel_count").alias("vessel_total_count"))
    )
    
    port_candidates_agg.orderBy(col("vessel_total_count").desc()).show(truncate=False)

    port_candidates_agg = port_candidates_agg.select(
        col("lat_bin_ports").alias("lat_bin"),
        col("lon_bin_ports").alias("lon_bin"),
        col("vessel_total_count").alias("vessel_count")
    )

    return port_candidates_agg

# Simple scikit-learn clustering is used, as we have only 144 port candidates and parallel processing would not improve computing speed
# Also DBSCAN is not included in a standart pyspark setup.
def cluster_dbscan(df, eps=0.1, min_samples=1):
    '''
    Create dbscan clusters based on coordinate bins, eps of 0.1 ~ 11km
    '''

    pandas_df = df.toPandas()
    coordinates = pandas_df[['lat_bin', 'lon_bin']].values

    dbscan = DBSCAN(eps=eps, min_samples=min_samples)
    cluster_labels = dbscan.fit_predict(coordinates)
    pandas_df['cluster'] = cluster_labels

    # Aggregate the average coordinates and sum of vessels
    ports_clusters = pandas_df.groupby('cluster').agg(
        lat_bin = ('lat_bin', 'mean'),
        lon_bin = ('lon_bin', 'mean'),
        vessel_count = ('vessel_count', 'sum')
    )

    # Print out the biggest ports
    print(ports_clusters.sort_values('vessel_count', ascending=False).head())

    return ports_clusters

def visualize_ports(port_candidates, map_name='port_map', to_pandas=True):
    # Transfer pyspark to pandas dataframe
    if to_pandas == True:
        port_candidates_pd = port_candidates.toPandas()
    else:
        port_candidates_pd = port_candidates

    # Zoom into Denmark
    mean_lat = port_candidates_pd["lat_bin"].mean()
    mean_lon = port_candidates_pd["lon_bin"].mean()
    m = folium.Map(location=[mean_lat, mean_lon], zoom_start=7)
    
    # Visualize the ports as circles
    for _, row in port_candidates_pd.iterrows():
        folium.CircleMarker(
            location=[row["lat_bin"], row["lon_bin"]],
            radius=min(row["vessel_count"], 20), 
            color="blue",
            fill=True,
            fill_opacity=0.6,
            popup=f"Vessels: {row['vessel_count']}"
        ).add_to(m)

    m.save(f"{map_name}.html")

### K-means functions
# Making as vector filtered data for knn analysis, where the features column is  made from the latitude and longitude
def make_vector(data):
    vec_assembler = VectorAssembler(inputCols=["Longitude", "Latitude"], outputCol="features")
    features_df = vec_assembler.transform(data)
    return features_df

# Making elbow graph, to check the most suitable port number for the data
def elbow(data):
# Elbow method: WSSSE unique K
    costs = []
    K_values = list(range(2, 10))
    for k in K_values:
        kmeans = KMeans().setK(k).setSeed(1).setFeaturesCol("features")
        model = kmeans.fit(data)
        wssse = model.summary.trainingCost
        costs.append(wssse)
    
    plt.figure(figsize=(8, 5))
    plt.plot(K_values, costs, marker='o')
    plt.xlabel("Number of Clusters (K)")
    plt.ylabel("WSSSE (Cost)")
    plt.title("Elbow Method for Optimal K")
    plt.grid(True)
    plt.show()

# Performing knn clustering with k={7,15,20} 
def perform_knn(k, feature_df):
    kmeans = KMeans().setK(k).setSeed(1)
    model = kmeans.fit(features_df)
    centers = model.clusterCenters()
    
    # Adding to each row cluster number 
    clustered = model.transform(features_df)
    return centers, clustered

# Printing cluster center coordinates
def print_cluster_centers(centers, k):
    print(f"Clusters (k={k}) centers")
    for idx, center in enumerate(centers):
        print(f"Cluster {idx}: Longitude = {center[0]}, Latitude = {center[1]}")

# Counting vessel number for each cluster for each experiment (k={7,15,20})
def count_vessel_in_cluster(clustered):
    cluster_counts = clustered.groupBy("prediction").agg(countDistinct("MMSI").alias("ship_count"))
    cluster_counts.show()


def visualize_clusters_knn(clustered, k):
    # 1. Group data by cluster prediction and aggregate:
    #    - count distinct vessels (MMSI) per cluster as 'vessel_count'
    #    - average latitude and longitude per cluster for map positioning
    summary_df = clustered.groupBy("prediction").agg(
        countDistinct("MMSI").alias("vessel_count"),
        avg("latitude").alias("lat_bin"),
        avg("longitude").alias("lon_bin")
    ).withColumnRenamed("prediction", "cluster_id") \
     .filter(col("vessel_count") >= 20)  # Filter clusters with at least 20 vessels  to automatically distinguish 
    #ports or major gathering points from small, possibly transient groups of vessels like fishing boats.
    
    # 2. Convert Spark DataFrame to Pandas DataFrame for easier iteration
    port_candidates_pd = summary_df.toPandas()
    
    # 3. Calculate mean latitude and longitude to center the Folium map
    mean_lat = port_candidates_pd["lat_bin"].mean()
    mean_lon = port_candidates_pd["lon_bin"].mean()
    
    # 4. Create a Folium map centered at the average coordinates
    m = folium.Map(location=[mean_lat, mean_lon], zoom_start=6)
    
    # 5. Add circle markers ("bubbles") to the map for each cluster
    for _, row in port_candidates_pd.iterrows():
        vessel_count = int(row["vessel_count"])  # Explicitly convert vessel_count to int
        radius = builtins.min(vessel_count, 20)  # Limit radius size to max 20 for visualization
        
        folium.CircleMarker(
            location=[row["lat_bin"], row["lon_bin"]],
            radius=radius,
            color="blue",
            fill=True,
            fill_opacity=0.6,
            popup=f"Vessels: {vessel_count}, Cluster: {row['cluster_id']}"
        ).add_to(m)
    
    # 6. Save the generated map as an HTML file with dynamic filename including k
    m.save(f"port_clusters_map_knn_{k}.html")

# Filtering vessels
df_clean = clean_data(df)
df_filtered = filter_moving_ships(df_clean)

total_stationary_ships = df_filtered.select("MMSI").distinct().count()
print(f"Total stationary/drifting ships: {total_stationary_ships}")

df_filtered.show(5, truncate=False)

# Using binning algorithm for port detection
port_candidates = bin_ships(df_filtered)
# Clustering these port candidates to ports with DBSCAN
ports_clusters = cluster_dbscan(port_candidates)
# Another alternative is re-using the binning algorithm
port_candidates_agg = bin_ports(port_candidates)

# Visualization
visualize_ports(port_candidates, map_name='binning_ports', to_pandas=True)
visualize_ports(ports_clusters, map_name='dbscan_ports', to_pandas=False)
visualize_ports(port_candidates_agg, map_name='binning_ports_agg', to_pandas=True)

# Now we can also try pyspark k-means clustering
print('Start k-means clustering')
features_df=make_vector(df_filtered)

centers7, clustered7 = perform_knn(7, features_df)
centers15, clustered15 = perform_knn(15, features_df)
centers20, clustered20 = perform_knn(20, features_df)

visualize_clusters_knn(clustered7, 7)
visualize_clusters_knn(clustered15, 15)
visualize_clusters_knn(clustered20, 20)
