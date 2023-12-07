from neo4j import GraphDatabase

URI = "neo4j://localhost:7687"
AUTH = ('neo4j', 'neo4j1234')

#top k cities with most airlines
#MATCH (p:Airport)-[r:Route]->(p2:Airport)
#WITH p.city AS City, COLLECT(DISTINCT r.airline_name) AS Airlines
#WITH City, Airlines, SIZE(Airlines) AS numofAirlines
#ORDER BY numofAirlines DESC
#LIMIT 10
#RETURN City, Airlines, numofAirlines*/
def test():
    return "Hello World"

def read_TopCountry_w_HighestAirports():
    ll = {}
    with GraphDatabase.driver(URI, auth=AUTH) as driver:
        driver.verify_connectivity()
        records, summary, keys = driver.execute_query(
            """ 
            MATCH (p:Airport)
            RETURN p.airport_id as Airport_ID, p.country AS Country
            """,
            database_="neo4j", )
        
        for record in records: 
            #print(record.data())
            airport_id = record.data()['Airport_ID']
            country = record.data()['Country']

            if country not in ll.keys():
                ll[country] = [airport_id]
            elif airport_id not in ll[country]:
                ll[country].append(id)   
    top_country = max(ll, key=lambda x: len(ll[x]))
    highest_airports = len(ll[top_country])
    print(f"{top_country}: {highest_airports}")
         
    driver.close() 

def read_TopKCities_w_MostAirlines2():
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
    for city, airlines in sorted_cities:
        airlines = list(airlines)
        sorted_airlines = sorted(airlines)
        print(f"{city}, {sorted_airlines}, {len(sorted_airlines)}")

    driver.close()


