from pyspark.sql import SparkSession
from neo4j import GraphDatabase
from collections import defaultdict

URI = "neo4j://localhost:7687"
AUTH = ('neo4j', 'neo4j1234')

def read_TopCountry_w_HighestAirports_pySpark(spark, output):
    ll = defaultdict(list)
    # Connect to Neo4j
    with GraphDatabase.driver(URI, auth=AUTH) as driver:
        driver.verify_connectivity()
        records, _, _ = driver.execute_query(
            """ 
            MATCH (p:Airport)
            RETURN p.airport_id as Airport_ID, p.country AS Country
            """,
            database_="neo4j", )
        
        # Create a DataFrame from Neo4j data
        columns = ["Airport_ID", "Country"]
        neo4j_data = [record.data() for record in records]
        neo4j_df = spark.createDataFrame(neo4j_data, columns)
        
        #top_country = max(ll, key=lambda x: len(ll[x]))
        #highest_airports = len(ll[top_country])
        
        # Perform necessary transformations
        result_df = neo4j_df.groupBy("Country").count().orderBy("count", ascending=False).limit(1)

        # Show the result on the screen
        result_df.show()

        # Write the result to a CSV file
        result_df.write.csv(output, header=True, mode="overwrite")

    driver.close()


# Create a Spark session
spark = SparkSession.builder.appName("TopCountrywithHighestAirport").getOrCreate()

# Specify the output CSV path
output = "output_topcountries"

# Call the function
read_TopCountry_w_HighestAirports_pySpark(spark, output)

# Stop the Spark session
spark.stop()
