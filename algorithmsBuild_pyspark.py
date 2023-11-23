import csv
from pyspark.sql import SparkSession
from pyspark.sql.types import StringType
from pyspark.sql.functions import col, collect_set, size

from neo4j import GraphDatabase

URI = "neo4j://localhost:7687"
AUTH = ('neo4j', 'neo4j1234')

def read_TopKCities_w_MostAirlines_pySpark(spark, output):
    city_airlines = {}
    airport_to_city = {}

    with GraphDatabase.driver(URI, auth=AUTH) as driver:
        driver.verify_connectivity()
        records, summary, keys = driver.execute_query(
            """ 
            MATCH (p:Airport)
            RETURN p.airport_id as Airport_ID, p.city AS City
            """,
            database_="neo4j", )
        for record in records: 
            #print(record.data())
            airport_id = record.data()['Airport_ID']
            city = record.data()['City']
            airport_to_city[airport_id] = city

        records, summary, keys = driver.execute_query(
            """ 
            MATCH (p:Airport)-[r:Route]->(p2:Airport)
            RETURN r.airline_name as Airline_Name, p.airport_id as Source_airport_id, p2.airport_id as Dest_airport_id
            """,
            database_="neo4j", )
        for record in records: 
            #print(record.data())
            airline_name = record.data()['Airline_Name']
            source_airport_id = record.data()['Source_airport_id']
            dest_airport_id = record.data()['Dest_airport_id']

            source_city = airport_to_city.get(source_airport_id, "Unknown")
            dest_city = airport_to_city.get(dest_airport_id, "Unknown")

            if source_city != "Unknown":
                if source_city not in city_airlines:
                    city_airlines[source_city] = set()
                city_airlines[source_city].add(airline_name)

            if dest_city != "Unknown": 
                if dest_city not in city_airlines:
                    city_airlines[dest_city] = set()
                city_airlines[dest_city].add(airline_name)
    sorted_cities = sorted(city_airlines.items(), key=lambda item: len(item[1]), reverse=True)
    #print(sorted_cities)
    #for city, airlines in sorted_cities:
    #    airlines = list(airlines)
    #    sorted_airlines = sorted(airlines)
    #    print(f"{city}, {sorted_airlines}, {len(sorted_airlines)}")

    # Convert data to PySpark DataFrame
    city_airlines_df = spark.createDataFrame(
        [(city, list(airlines)) for city, airlines in sorted_cities],
        ["City", "Airlines"]
    )

    # Perform necessary transformations and aggregations
    city_airlines_df = (
        city_airlines_df
        .withColumn("numofAirlines", size("Airlines"))
        .orderBy("numofAirlines", ascending=False)
        .limit(10)
    )

    # Collect and print the results
    results = city_airlines_df.collect()

    #for row in results:
    #    city = row["City"]
    #    airlines = sorted(row["Airlines"])  # Convert back to list
    #    num_of_airlines = row["numofAirlines"]
    #    print(f"{city}, {airlines}, {num_of_airlines}")

    # Show the result on the screen
    city_airlines_df.show()

    # Save the results to a CSV file
    #city_airlines_df.write.csv(output, header=True, mode="overwrite")
    # Save to JSON format
    city_airlines_df.write.json(output, mode="overwrite")

    driver.close()

# Create a Spark session
spark = SparkSession.builder.appName("Scala").getOrCreate()

# Specify the output Json path
output = "output_topcities"

# Call the function
read_TopKCities_w_MostAirlines_pySpark(spark, output)

# Stop the Spark session
spark.stop()
