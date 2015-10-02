import networkx as nx
import pandas as pd
import csv

# Do not include new MRT lines
peak_full_nodesedges = pd.read_csv("H:\\Projects\\SMDA\\data\\FinalPeakTransportNodesEdges.csv")

del peak_full_nodesedges['FROM_GEOSPATIAL_KEY']
del peak_full_nodesedges['TO_GEOSPATIAL_KEY']

#This is for calculating current nodes edges without considering future lines
peak_full_nodesedges = peak_full_nodesedges[peak_full_nodesedges.COMPLETED == "YES"]
del peak_full_nodesedges['COMPLETED']

#Peak complete graph modelling
peak_complete_graph = nx.DiGraph()
peak_complete_graph.add_weighted_edges_from([tuple(x) for x in peak_full_nodesedges.values])

filtered = peak_full_nodesedges[~peak_full_nodesedges["FROM"].str.contains("_")]
origin = list(filtered["FROM"])
#test = origin[:5]

with open("H:\\Projects\\SMDA\\data\\PeakComplete_ShortestPath.csv",'wb') as f1:
    writer=csv.writer(f1, delimiter=',', lineterminator = '\n')
    writer.writerow(["ORIGIN","DESTINATION","TIME_MINS"])
    for i in origin:
        length = nx.single_source_dijkstra_path_length(peak_complete_graph, i)
        docs = []        
        for k, v in length.iteritems():
            if "W" not in k:  
                dest = k.split("_")[0]
                row = [(i), (dest), (v)]
                docs.append(row)
                #writer.writerow(row)
        df = pd.DataFrame(docs, columns = ['ORIGIN','DESTINATION','TIME'])
        min_df = df.groupby(['ORIGIN','DESTINATION'])['TIME'].min()
        min_df.to_csv(f1, header = False)
f1.close()            

#####################################################
#Include new MRT lines
peak_full_nodesedges = pd.read_csv("H:\\Projects\\SMDA\\data\\FinalPeakTransportNodesEdges.csv")

del peak_full_nodesedges['FROM_GEOSPATIAL_KEY']
del peak_full_nodesedges['TO_GEOSPATIAL_KEY']

#This is for calculating current nodes edges without considering future lines
#peak_full_nodesedges = peak_full_nodesedges[peak_full_nodesedges.COMPLETED == "YES"]
del peak_full_nodesedges['COMPLETED']

#Peak complete graph modelling
peak_complete_graph = nx.DiGraph()
peak_complete_graph.add_weighted_edges_from([tuple(x) for x in peak_full_nodesedges.values])

filtered = peak_full_nodesedges[~peak_full_nodesedges["FROM"].str.contains("_")]
origin = list(filtered["FROM"])
#test = origin[:5]

with open("H:\\Projects\\SMDA\\data\\Peak_IncludeNewMRT_ShortestPath.csv",'wb') as f1:
    writer=csv.writer(f1, delimiter=',', lineterminator = '\n')
    writer.writerow(["ORIGIN","DESTINATION","TIME_MINS"])
    for i in origin:
        length = nx.single_source_dijkstra_path_length(peak_complete_graph, i)
        docs = []        
        for k, v in length.iteritems():
            if "W" not in k:  
                dest = k.split("_")[0]
                row = [(i), (dest), (v)]
                docs.append(row)
                #writer.writerow(row)
        df = pd.DataFrame(docs, columns = ['ORIGIN','DESTINATION','TIME'])
        min_df = df.groupby(['ORIGIN','DESTINATION'])['TIME'].min()
        min_df.to_csv(f1, header = False)
f1.close()            

#####################################################

#Calculating breakdown from Bugis to JE

peak_full_nodesedges = pd.read_csv("H:\\Projects\\SMDA\\data\\FinalPeak_JE_Bugis_TransportNodesEdges.csv")

del peak_full_nodesedges['FROM_GEOSPATIAL_KEY']
del peak_full_nodesedges['TO_GEOSPATIAL_KEY']

#This is for calculating current nodes edges without considering future lines
peak_full_nodesedges = peak_full_nodesedges[peak_full_nodesedges.COMPLETED == "YES"]
del peak_full_nodesedges['COMPLETED']

#Peak complete graph modelling
peak_complete_graph = nx.DiGraph()
peak_complete_graph.add_weighted_edges_from([tuple(x) for x in peak_full_nodesedges.values])

filtered = peak_full_nodesedges[~peak_full_nodesedges["FROM"].str.contains("_")]
origin = list(filtered["FROM"])
#test = origin[:5]

with open("H:\\Projects\\SMDA\\data\\Peak_Bugis_JE_Breakdown_ShortestPath.csv",'wb') as f1:
    writer=csv.writer(f1, delimiter=',', lineterminator = '\n')
    writer.writerow(["ORIGIN","DESTINATION","TIME_MINS"])
    for i in origin:
        length = nx.single_source_dijkstra_path_length(peak_complete_graph, i)
        docs = []        
        for k, v in length.iteritems():
            if "W" not in k:  
                dest = k.split("_")[0]
                row = [(i), (dest), (v)]
                docs.append(row)
                #writer.writerow(row)
        df = pd.DataFrame(docs, columns = ['ORIGIN','DESTINATION','TIME'])
        min_df = df.groupby(['ORIGIN','DESTINATION'])['TIME'].min()
        min_df.to_csv(f1, header = False)
f1.close()    


#####################################################
#Calculating breakdown from Bishan to Marina Bay

peak_full_nodesedges = pd.read_csv("H:\\Projects\\SMDA\\data\\FinalPeak_Bishan_MarinaBay_TransportNodesEdges.csv")

del peak_full_nodesedges['FROM_GEOSPATIAL_KEY']
del peak_full_nodesedges['TO_GEOSPATIAL_KEY']

#This is for calculating current nodes edges without considering future lines
peak_full_nodesedges = peak_full_nodesedges[peak_full_nodesedges.COMPLETED == "YES"]
del peak_full_nodesedges['COMPLETED']

#Peak complete graph modelling
peak_complete_graph = nx.DiGraph()
peak_complete_graph.add_weighted_edges_from([tuple(x) for x in peak_full_nodesedges.values])

filtered = peak_full_nodesedges[~peak_full_nodesedges["FROM"].str.contains("_")]
origin = list(filtered["FROM"])
#test = origin[:5]

with open("H:\\Projects\\SMDA\\data\\Peak_Bishan_MB_Breakdown_ShortestPath.csv",'wb') as f1:
    writer=csv.writer(f1, delimiter=',', lineterminator = '\n')
    writer.writerow(["ORIGIN","DESTINATION","TIME_MINS"])
    for i in origin:
        length = nx.single_source_dijkstra_path_length(peak_complete_graph, i)
        docs = []        
        for k, v in length.iteritems():
            if "W" not in k:  
                dest = k.split("_")[0]
                row = [(i), (dest), (v)]
                docs.append(row)
                #writer.writerow(row)
        df = pd.DataFrame(docs, columns = ['ORIGIN','DESTINATION','TIME'])
        min_df = df.groupby(['ORIGIN','DESTINATION'])['TIME'].min()
        min_df.to_csv(f1, header = False)
f1.close()    

#####################################################
#Calculating breakdown from Woodleigh to Punggol

peak_full_nodesedges = pd.read_csv("H:\\Projects\\SMDA\\data\\FinalPeak_Woodleigh_Punggol_TransportNodesEdges.csv")

del peak_full_nodesedges['FROM_GEOSPATIAL_KEY']
del peak_full_nodesedges['TO_GEOSPATIAL_KEY']

#This is for calculating current nodes edges without considering future lines
peak_full_nodesedges = peak_full_nodesedges[peak_full_nodesedges.COMPLETED == "YES"]
del peak_full_nodesedges['COMPLETED']

#Peak complete graph modelling
peak_complete_graph = nx.DiGraph()
peak_complete_graph.add_weighted_edges_from([tuple(x) for x in peak_full_nodesedges.values])

filtered = peak_full_nodesedges[~peak_full_nodesedges["FROM"].str.contains("_")]
origin = list(filtered["FROM"])
#test = origin[:5]

with open("H:\\Projects\\SMDA\\data\\Peak_Punggol_Woodleigh_Breakdown_ShortestPath.csv",'wb') as f1:
    writer=csv.writer(f1, delimiter=',', lineterminator = '\n')
    writer.writerow(["ORIGIN","DESTINATION","TIME_MINS"])
    for i in origin:
        length = nx.single_source_dijkstra_path_length(peak_complete_graph, i)
        docs = []        
        for k, v in length.iteritems():
            if "W" not in k:  
                dest = k.split("_")[0]
                row = [(i), (dest), (v)]
                docs.append(row)
                #writer.writerow(row)
        df = pd.DataFrame(docs, columns = ['ORIGIN','DESTINATION','TIME'])
        min_df = df.groupby(['ORIGIN','DESTINATION'])['TIME'].min()
        min_df.to_csv(f1, header = False)
f1.close()    

################################################################
#Non-Peak calculations
nonpeak_full_nodesedges = pd.read_csv("H:\\Projects\\SMDA\\data\\FinalNonPeakTransportNodesEdges.csv")

del nonpeak_full_nodesedges['FROM_GEOSPATIAL_KEY']
del nonpeak_full_nodesedges['TO_GEOSPATIAL_KEY']

#This is for calculating current nodes edges without considering future lines
nonpeak_full_nodesedges = nonpeak_full_nodesedges[nonpeak_full_nodesedges.COMPLETED == "YES"]
del nonpeak_full_nodesedges['COMPLETED']

#Non-peak complete graph modelling
nonpeak_complete_graph = nx.DiGraph()
nonpeak_complete_graph.add_weighted_edges_from([tuple(x) for x in nonpeak_full_nodesedges.values])

filtered = nonpeak_full_nodesedges[~nonpeak_full_nodesedges["FROM"].str.contains("_")]
origin = list(filtered["FROM"])
#test = origin[:5]

with open("H:\\Projects\\SMDA\\data\\NonPeakComplete_ShortestPath.csv",'wb') as f1:
    writer=csv.writer(f1, delimiter=',', lineterminator = '\n')
    writer.writerow(["ORIGIN","DESTINATION","TIME_MINS"])
    for i in origin:
        length = nx.single_source_dijkstra_path_length(nonpeak_complete_graph, i)
        docs = []        
        for k, v in length.iteritems():
            if "W" not in k:  
                dest = k.split("_")[0]
                row = [(i), (dest), (v)]
                docs.append(row)
                #writer.writerow(row)
        df = pd.DataFrame(docs, columns = ['ORIGIN','DESTINATION','TIME'])
        min_df = df.groupby(['ORIGIN','DESTINATION'])['TIME'].min()
        min_df.to_csv(f1, header = False)
f1.close()            

#########################################################

        
"""
Test Scripts
"""
length = nx.single_source_dijkstra_path_length(peak_complete_graph, "70361")

path = nx.single_source_dijkstra_path(peak_complete_graph, origin)

path_length=nx.all_pairs_dijkstra_path_length(peak_complete_graph, weight='weight');

peak_complete_graph.get_edge_data('60141_66', '60141_140')

