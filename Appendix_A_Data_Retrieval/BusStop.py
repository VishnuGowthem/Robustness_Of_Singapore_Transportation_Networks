from bs4 import BeautifulSoup
import unicodecsv
    

with open('G:\Data\Bus_Station_Old.csv','wb') as f1:
    writer=unicodecsv.writer(f1, delimiter=',', lineterminator = '\n')
    #br = webdriver.Chrome()
    #header row
    writer.writerow(["BUSSTOP_CODE","STATION_NAME","BUS_ROUTE","LONGITUDE","LATITUDE"])
    
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
            route_tag = station.find('tag', k='route_ref')
            bus_route = str(route_tag['v'])
        except:
            bus_route = 'NA'
        
        try: 
            busstop_codetag = station.find('tag', k='asset_ref')
            busstop_code = str(busstop_codetag['v'])
        except:
            busstop_code = 'NA'
        
        for tag_attrs in another_tag:
            #if 'bus' in tag_attrs['v'] :            
            if str(tag_attrs['k']) == 'highway' and str(tag_attrs['v']) == 'bus_stop' :
                row = [(busstop_code), (station_name), (bus_route), (longitude), (latitude)]  
                print row
                writer.writerow(row)

f1.close()
            
