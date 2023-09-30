import csv

def readAirport():
    with open('airports_example.csv') as f:
        reader = csv.reader(f)
        
        for row in reader:
            print(row)
    f.close()

