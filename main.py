#source: https://github.com/ronidas39/NEO4J_PYTHON/blob/main/tutorial106/main.py
#how to run:
#python main.py
#uvicorn main:app --port 8081
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

@app.get("/airportSearch/read_TopCountry_w_HighestAirports")
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


@app.get("/airlineSearch/read_TopKCities_w_MostAirlines2")
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


@app.get("/airportSearch/read_TopCountry_w_HighestAirports")
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

#example: http://127.0.0.1:8081/tripRecommendation/findATripWithDHops?srcCity=Seattle&dstCity=Las%20Vegas&dHops=10
@app.post("/tripRecommendation/findATripWithDHops")
def findATripWithDHops():
    srcCity = "Seattle"
    dstCity = "Las Vegas"
    dHops = 10
    driver_neo4j=connection()
    session=driver_neo4j.session()

    trip_query="MATCH p=(s:Airport)-->{1," + str(dHops) + "}(d:Airport) WHERE s.city = $src and d.city = $dst RETURN relationships(p), nodes(p) limit 1"
    search={"src":srcCity,"dst":dstCity}
    #search={"dHopsRelation":dHops_Value,"src":srcCity,"dst":dstCity}
    records=session.run(trip_query, search)

    trips = []
    for record in records:
        routes = record['relationships(p)']
        nodes = record['nodes(p)']
        
        path = []
        for i in range(len(routes)):
            src, dst, route = nodes[i], nodes[i + 1], routes[i]
            airline = route.get("airline_name")
            dept = src.get("IATA") + ", " + src.get("city")
            dest = dst.get("IATA") + ", " + dst.get("city")
            path.append(f"{airline} airline: {dept}  to  {dest}")
        
        trips.append(path)
    
    for t in trips:
        print('Trip: ', t)  
    
    data=[{"Trip": trips}]
    return data

#example: http://127.0.0.1:8081/tripRecommendation/findCitiesWithDHops?srcCity=Seattle&dHops=2
@app.post("/tripRecommendation/findCitiesWithDHops")
def findCitiesWithDHops(srcCity, dHops):
    #srcCity = "Seattle"
    #dHops = 2
    driver_neo4j=connection()
    session=driver_neo4j.session()

    city_query = "match p=(s:Airport)-->{1," + str(dHops) + "}(dst: Airport) where s.city = $src return distinct dst.city order by dst.city"
    search={"src":srcCity}
    records=session.run(city_query, search)

    cities = list(map(lambda r: r.get('dst.city'), records))
    
    print(f"{len(cities)} cities found: ")
    print(cities)
    
    data=[{"Total_cities_found": len(cities), "Cities": cities}]
    return data

######################################
#FIND A TRIP THAT CONNECTS TWO CITIES#
######################################
def findAdjacentAirportNodesOfCity(city):
    driver_neo4j=connection()
    session=driver_neo4j.session()

    query = 'match (s:Airport)-->(d: Airport) where s.city = $city return d'
    search={"city":city}
    records=session.run(query, search)
    return list(map(lambda r: r['d'], records))

def findAdjacentAirportNodes(airportNode):
    driver_neo4j=connection()
    session=driver_neo4j.session()

    query = 'match (s:Airport)-->(d: Airport) where s.airport_id = $airport_id return d'
    search={"airport_id":airportNode.get("airport_id")}
    records=session.run(query, search)
    return list(map(lambda r: r['d'], records))

def findAirportNodeOfCity(city, airportNode):
    driver_neo4j=connection()
    session=driver_neo4j.session()
    
    query = 'match p=(s:Airport)-->(d: Airport) where s.city = $city and d.airport_id = $airport_id return s'
    search={"city":city, "airport_id":airportNode["airport_id"]}
    records=session.run(query, search)
    
    # Check if there are any records
    if records:
        # Access the first record, if available
        for record in records:
            result = record['s']
            return result

    return None  # Return None if no matching record is found


def findRouteBetweenAirports(src, dst):
    driver_neo4j=connection()
    session=driver_neo4j.session()
    
    query = 'match p=(s:Airport)-->(d: Airport) where s.airport_id = $src and d.airport_id = $dst return relationships(p)'
    search={"src":src.get("airport_id"), "dst":dst.get("airport_id")}
    records=session.run(query, search)
    #return records[0]['relationships(p)'][0]

    # Check if there are any records
    for record in records:
        subrecord = record['relationships(p)']
        for result in subrecord:
            return result

    return None  # Return None if no matching record is found

def bfs(src, dst):
    queue = findAdjacentAirportNodesOfCity(src)
    firstChecks = [d for d in queue if d["city"] == dst]
    if len(firstChecks) > 0:
        lastNode = {"cur": firstChecks[0], "prev": None}
    else:
        queue = list(map(lambda airport: {"cur": airport, "prev": None}, queue))
        visited = set()
        
        lastNode = None
        while len(queue) > 0:
            top = queue.pop(0)
            curNode = top.get("cur")
            if curNode.get("airport_id") not in visited:
                if curNode.get("city") == dst:
                    lastNode = top
                    break
                
                visited.add(curNode.get("airport_id"))
                for adj in findAdjacentAirportNodes(curNode):
                    if curNode.get("city") == src:
                        continue
                    if adj.get("airport_id") not in visited:
                        queue.append({"cur": adj, "prev": top})
        
    nodes = []
    while lastNode is not None:
        nodes.insert(0, lastNode["cur"])
        lastNode = lastNode["prev"]
    
    if len(nodes) > 0:
        nodes.insert(0, findAirportNodeOfCity(src, nodes[0]))
    # print(nodes)
    
    path = []
    for i in range(len(nodes) - 1):
        src, dst = nodes[i], nodes[i + 1]
        route = findRouteBetweenAirports(src, dst)
        airline = route.get("airline_name")
        dept = src.get("IATA") + ", " + src.get("city")
        dest = dst.get("IATA") + ", " + dst.get("city")
        path.append(f"{airline} airline: {dept}  to  {dest}")
        #print(f"{airline} airline: {dept}  to  {dest}")
    return path

#print(bfs("Seattle", "Masset"))

#example: http://127.0.0.1:8081/tripRecommendation/findATrip?srcCity=Seattle&dstCity=Masset
@app.post("/tripRecommendation/findATrip")
def findATrip(srcCity, dstCity):
    result = bfs(srcCity, dstCity)
    data=[{"Trip_Search": result}]
    return data



        