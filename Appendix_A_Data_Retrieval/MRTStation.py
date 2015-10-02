from bs4 import BeautifulSoup
import unicodecsv

    

with open('G:\Data\MRT_Station.csv','wb') as f1:
    writer=unicodecsv.writer(f1, delimiter=',', lineterminator = '\n')
    #br = webdriver.Chrome()
    #header row
    writer.writerow(["NODE_TYPE","STATION_NAME","NETWORK_TYPE","LONGITUDE","LATITUDE"])
    
    handler = open("G:\\Dropbox\MSBA\\Digital and Social Media Analytics\\Project\\singapore.osm", "r").read()
    soup = BeautifulSoup(handler)
    stations = soup.findAll('node')
    
    
    
    for station in stations:
        longitude = str(station['lon'])
        latitude = str(station['lat'])
        another_tag = station('tag')
        
        #get station name
        try: 
            station_nametag = station.find('tag', k='name')
            station_name = str(station_nametag['v'])
        except:
            station_name = 'NA'
        
        try: 
            network_typetag = station.find('tag', k='network')
            network_type = str(network_typetag['v'])
        except:
            network_type = 'NA'
        
        for tag_attrs in another_tag:
            if str(tag_attrs['k']) == 'railway':
                railway_node_type = str(tag_attrs['v'])
                row = [(railway_node_type), (station_name), (network_type), (longitude), (latitude)]  
                print row
                writer.writerow(row)

f1.close()
            
