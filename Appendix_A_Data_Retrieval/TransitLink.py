#importing urllib2, beautifulsoup and csv
import csv
from bs4 import BeautifulSoup
import httplib
import unicodecsv
from selenium import webdriver


def patch_http_response_read(func):
    def inner(*args):
        try:
            return func(*args)
        except httplib.IncompleteRead, e:
            return e.partial
    return inner

httplib.HTTPResponse.read = patch_http_response_read(httplib.HTTPResponse.read)

with open('E:\Dropbox\MSBA\Digital and Social Media Analytics\Project\BusNumber.csv', 'rb') as f:
    reader = csv.reader(f)
    busno_list = list(reader)

#open a cursor to csv
with open('D:\Data\TransitLink.csv','wb') as f1:
    writer=unicodecsv.writer(f1, delimiter=',', lineterminator = '\n')
    #br = webdriver.Chrome()
    #header row
    writer.writerow(["BUS","DIRECTION","DISTANCE(KM)","BUSSTOP_CODE","BUSSTOP_DESC"])
    #starting Url to grab all the links
    #outerhtml = urllib2.urlopen("http://www.transitlink.com.sg/eservice/eguide/service_route.php?service=10").read()
    #br.get("http://www.ebay.com.sg/sch/Mobile-Phones-/9355/i.html?LH_PrefLoc=1&_pgn=2&_skc=200&rt=nc")    
    #html_source = br.page_source
    #br = mechanize.Browser()
    #outerhtml = br.open("http://www.transitlink.com.sg/eservice/eguide/service_route.php?service=10").get_data()
    
    for i in range(len(busno_list)):
        busno = busno_list[i][0]
        html_link = "http://www.transitlink.com.sg/eservice/eguide/service_route.php?service=" + busno
    
            
        #Remember to copy phantomjs.exe into path    
        browser = webdriver.PhantomJS()        
        browser.get(html_link)        
        html_source = browser.page_source    
        browser.quit()
        
        #scrape1 = BeautifulSoup(html_source,'html.parser')
        scrape1 = BeautifulSoup(html_source)
                
        #grab all sections of html that are repeated. E.g. forum posts, listings of products
        htmlsection = scrape1.find_all('section', {"class":"eguide-table"})
        
        for item in htmlsection:
          
            try:
                direction = item.find('td', {"class":"subhead"}).text.strip()
            #, encoding='utf-8', errors = 'ignore')
            except:
                direction = "NA"
                    
            if direction == 'DIRECTION 1':
                for tab in item.find_all('table'):
                    for tr in tab.find_all('tr')[2:]:
                        tds = tr.find_all('td')
                        
                        if len(tds) == 3:
                            try:            
                                distance = tds[0].text.strip().encode('ascii', 'ignore')
                                busstop_code = tds[1].text.strip().encode('ascii', 'ignore')
                                busstop_desc = tds[2].text.strip().encode('ascii', 'ignore')
                                if distance != '':                             
                                    row = [(busno), (direction), (distance), (busstop_code), (busstop_desc)]
                                    print row
                                    writer.writerow(row)
                            except:
                                pass
            elif direction == 'DIRECTION 2':
                for tab in item.find_all('table'):
                    for tr in tab.find_all('tr')[2:]:
                        tds = tr.find_all('td')
                        
                        if len(tds) == 3:
                            try:            
                                distance = tds[0].text.strip().encode('ascii', 'ignore')
                                busstop_code = tds[1].text.strip().encode('ascii', 'ignore')
                                busstop_desc = tds[2].text.strip().encode('ascii', 'ignore')
                                if distance != '':                             
                                    row = [(busno), (direction), (distance), (busstop_code), (busstop_desc)]
                                    print row
                                    writer.writerow(row)
                            except:
                                pass
            elif direction == 'LOOP SERVICE':
                        for tab in item.find_all('table'):
                            for tr in tab.find_all('tr')[2:]:
                                tds = tr.find_all('td')
                                
                                if len(tds) == 3:
                                    try:            
                                        distance = tds[0].text.strip().encode('ascii', 'ignore')
                                        busstop_code = tds[1].text.strip().encode('ascii', 'ignore')
                                        busstop_desc = tds[2].text.strip().encode('ascii', 'ignore')
                                        if distance != '':                             
                                            row = [(busno), (direction), (distance), (busstop_code), (busstop_desc)]
                                            print row
                                            writer.writerow(row)
                                    except:
                                        pass
        
f1.close()

import pandas as pd

clean_data = pd.read_csv('G:\\Dropbox\\MSBA\\Digital and Social Media Analytics\\Project\\TransitLink.csv')

clean_data['BUSSTOP_FROM'] = clean_data.sort('DISTANCE_KM').groupby(['BUS','DIRECTION'])['BUSSTOP_CODE'].shift(1)

clean_data['DISTANCE_LAG'] = clean_data.sort('DISTANCE_KM').groupby(['BUS','DIRECTION'])['DISTANCE_KM'].shift(1)

clean_data['DISTANCE_BETWEEN'] = clean_data['DISTANCE_KM'] - clean_data['DISTANCE_LAG']

final_data = clean_data.sort(['BUS','DIRECTION','DISTANCE_KM']).dropna()

final_data.rename(columns={'BUSSTOP_CODE':'BUSSTOP_TO'}, inplace=True)

final_data.to_csv('G:\\Dropbox\\MSBA\\Digital and Social Media Analytics\\Project\\BusNodesEdges_v2.csv', index = False)
