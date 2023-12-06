from fastapi import FastAPI
import os
from neo4j import GraphDatabase
from dotenv import load_dotenv

load_dotenv()
URI = "neo4j://localhost:7687"
AUTH = ('neo4j', 'neo4j1234')

uri=os.getenv("neo4j://localhost:7687")
user=os.getenv("neo4j")
pwd=os.getenv("neo4j1234")
auth=(user, pwd)

def connection():
    driver=GraphDatabase.driver(uri=URI,auth=AUTH)
    return driver

app=FastAPI()
@app.get("/")
def default():
    return {"reponse": "you are in the root path"}

@app.get("/read_TopCountry_w_HighestAirports")
def read_TopCountry_w_HighestAirports():
    driver_neo4j=connection()
    session=driver_neo4j.session()
    ll = {}
    q1="""
    MATCH (p:Airport)
    RETURN p.airport_id as Airport_ID, p.country AS Country
    """
    #x={"Airport_ID":airport_id, "Country": country}
    records=session.run(q1)

    for record in records: 
        #print(record.data())
        #airport_id = record.data()['Airport_ID']
        airport_id = record['Airport_ID']
        #country = record.data()['Country']
        country = record['Country']

        if country not in ll.keys():
            ll[country] = [airport_id]
        elif airport_id not in ll[country]:
            ll[country].append(id)   
    top_country = max(ll, key=lambda x: len(ll[x]))
    highest_airports = len(ll[top_country])
    print(f"{top_country}: {highest_airports}")  
    
    data=[{"Top_Country":top_country, "Num":highest_airports}]
    return data


@app.get("/read_TopKCities_w_MostAirlines2")
def read_TopKCities_w_MostAirlines2():

    driver_neo4j=connection()
    session=driver_neo4j.session()
    
    city_airlines = {}
    airport_to_city = {}

    airport_query="""
    MATCH (p:Airport)
    RETURN p.airport_id as Airport_ID, p.city AS City
    """
    city_query_records=session.run(airport_query)
    for record in city_query_records: 
        #print(record.data())
        airport_id = record['Airport_ID']
        city = record['City']
        airport_to_city[airport_id] = city

    airline_query="""
    MATCH (p:Airport)-[r:Route]->(p2:Airport)
    RETURN r.airline_name as Airline_Name, p.airport_id as Source_airport_id, p2.airport_id as Dest_airport_id
    """
    airline_query_records=session.run(airline_query)
    for record2 in airline_query_records: 
        airline_name = record2['Airline_Name']
        source_airport_id = record2['Source_airport_id']
        dest_airport_id = record2['Dest_airport_id']

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
    
    data=[]
    for city, airlines in sorted_cities:
        airlines = list(airlines)
        sorted_airlines = sorted(airlines)
        print(f"{city}, {sorted_airlines}, {len(sorted_airlines)}")
        
        row = {"City":city, "List_of_Airlines":sorted_airlines, "Num": len(sorted_airlines)}
        data.append(row)

    return data



 