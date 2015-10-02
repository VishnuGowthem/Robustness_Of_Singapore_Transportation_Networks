library(geosphere)
library(data.table)
library(foreach)
library(sqldf)
library(doParallel)
library(gsubfn)
library(plyr)

cl<-makeCluster(3)
registerDoParallel(cl)

nodes <- read.csv('./BusMRTLRTNodes.csv')
nodes$dummy <- "k"
nodes_crossjoin <- merge(nodes, nodes, by="dummy")
nodes_clean <- subset(nodes_crossjoin, NODE_KEY.x != NODE_KEY.y)

#Using Haversine Distance for calculation
ptm <- proc.time()
nodes_clean$DistHaversine <- apply(nodes_clean[,c('LONGITUDE.x','LATITUDE.x','LONGITUDE.y','LATITUDE.y')], 1, function(y) distHaversine(c(y['LONGITUDE.x'],y['LATITUDE.x']),c(y['LONGITUDE.y'],y['LATITUDE.y'])))
proc.time() - ptm

###################################
#nodes_100m <- subset(nodes_clean, DistHaversine < 1000) 
#Model walking distance
nodes_walk_400m <- subset(nodes_clean, DistHaversine <= 400) 
nodes_walk_400m$timetravelled <- round(nodes_walk_400m$DistHaversine / 80, 2)
nodes_walk_edges <- subset(nodes_walk_400m, select = c('NODE_KEY.x','NODE_KEY.y','timetravelled'))
setnames(nodes_walk_edges, old=c('NODE_KEY.x','NODE_KEY.y','timetravelled'), new = c('FROM','TO','TIME_MINS'))
nodes_walk_edges$FROM <- paste(nodes_walk_edges$FROM, "_", "W", sep = "")
nodes_walk_edges$TO <- paste(nodes_walk_edges$TO, "_", "W", sep = "")

rm(nodes_walk_400m)

#Model geospatial node to walking node
geonode_walknode <- subset(nodes, select = c('NODE_KEY'))
geonode_walknode$TO <- paste(geonode_walknode$NODE_KEY, "_", "W", sep = "")
geonode_walknode$TIME_MINS <- 0.0
setnames(geonode_walknode, old=c('NODE_KEY'), new = c('FROM'))
geonode_walknode$LINE <- 'WALK'
geonode_walknode$COMPLETED <- 'YES'
geonode_walknode$FROM_GEOSPATIAL_KEY <- geonode_walknode$FROM
geonode_walknode$TO_GEOSPATIAL_KEY <- geonode_walknode$FROM

####################################
#Modelling bus travelling time (nodes are remodelled)
buspeaktravelspeed <- 25
busnonpeaktravelspeed <- 30

nodes_bus <- read.csv('./BusNodesEdges_v2.csv')
nodes_bus$STOPCODE_BUS_FROM <- paste(nodes_bus$BUSSTOP_FROM, nodes_bus$BUS, sep="_") 
nodes_bus$STOPCODE_BUS_TO <- paste(nodes_bus$BUSSTOP_TO, nodes_bus$BUS, sep="_") 

nodes_bus$timetravelled_peak <- nodes_bus$DISTANCE_BETWEEN / buspeaktravelspeed * 60
nodes_bus$timetravelled_nonpeak <- nodes_bus$DISTANCE_BETWEEN / busnonpeaktravelspeed * 60 
nodes_bus$LINE <- 'BUS'
nodes_bus$COMPLETED <- 'YES'
nodes_bus_peak_edges <- subset(nodes_bus, select = c('STOPCODE_BUS_FROM','STOPCODE_BUS_TO','LINE','COMPLETED','timetravelled_peak'))
nodes_bus_nonpeak_edges <- subset(nodes_bus, select = c('STOPCODE_BUS_FROM','STOPCODE_BUS_TO','LINE','COMPLETED','timetravelled_nonpeak'))
setnames(nodes_bus_peak_edges, old= c('STOPCODE_BUS_FROM','STOPCODE_BUS_TO','timetravelled_peak'), new = c('FROM','TO','TIME_MINS'))
setnames(nodes_bus_nonpeak_edges, old= c('STOPCODE_BUS_FROM','STOPCODE_BUS_TO','timetravelled_nonpeak'), new = c('FROM','TO','TIME_MINS'))

#####################################
#Adding in the bus to bus waiting time (nodes are remodelled)
nodes_bus_interchg <- sqldf('select distinct STOPCODE_BUS_FROM, BUSSTOP_FROM from nodes_bus where BUSSTOP_FROM in (select BUSSTOP_FROM from nodes_bus group by BUSSTOP_FROM having count(distinct(BUS)) > 1) order by BUSSTOP_FROM')

nodes_bus_crossjoin <- merge(nodes_bus_interchg, nodes_bus_interchg, by="BUSSTOP_FROM")
nodes_bus_interchg_clean <- subset(nodes_bus_crossjoin, STOPCODE_BUS_FROM.x != STOPCODE_BUS_FROM.y)
nodes_bus_interchg_clean$TIME_MINS <- 10
nodes_bus_inter_edges <- subset(nodes_bus_interchg_clean, select = c('STOPCODE_BUS_FROM.x','STOPCODE_BUS_FROM.y','TIME_MINS'))
setnames(nodes_bus_inter_edges, old= c('STOPCODE_BUS_FROM.x','STOPCODE_BUS_FROM.y'), new = c('FROM','TO'))
nodes_bus_inter_edges$LINE <- 'BUS CHANGE'
nodes_bus_inter_edges$COMPLETED <- 'YES'


rm(nodes_bus_interchg, nodes_bus_crossjoin, nodes_bus_interchg_clean)

######################################
#Modelling MRT/LRT travelling time (nodes are remodelled)

nodes_mrtlrt <- read.csv('./MRTLRTNodesEdges.csv')
nodes_mrtlrt$MRT_LRT_FROM <- as.factor(nodes_mrtlrt$MRT_LRT_FROM)
nodes_mrtlrt$MRT_LRT_TO <- as.factor(nodes_mrtlrt$MRT_LRT_TO)
nodes_mrtlrt$MRT_LRT_LINE_FROM <- paste(nodes_mrtlrt$MRT_LRT_FROM, nodes_mrtlrt$LINE, sep="_")
##
nodes_mrtlrt$MRT_LRT_LINE_TO <- paste(nodes_mrtlrt$MRT_LRT_TO, nodes_mrtlrt$LINE, sep="_")
nodes_mrtlrt_edges <- subset(nodes_mrtlrt, select = c('MRT_LRT_LINE_FROM','MRT_LRT_LINE_TO','LINE','COMPLETED','TRAVEL_TIME'))
setnames(nodes_mrtlrt_edges, old= c('MRT_LRT_LINE_FROM','MRT_LRT_LINE_TO','TRAVEL_TIME'), new = c('FROM','TO','TIME_MINS'))

######################################
#Adding in the interchange waiting time (nodes are remodelled)

nodes_mrtlrt_interchg <- sqldf('select distinct MRT_LRT_FROM, MRT_LRT_LINE_FROM from nodes_mrtlrt where MRT_LRT_FROM in (select MRT_LRT_FROM from nodes_mrtlrt group by MRT_LRT_FROM having count(distinct(LINE)) > 1)')

nodes_mrtlrt_interchg$LINE <- strapplyc(nodes_mrtlrt_interchg$MRT_LRT_LINE_FROM, "_(.*)", simplify = TRUE)

nodes_mrtlrt_crossjoin <- merge(nodes_mrtlrt_interchg, nodes_mrtlrt_interchg, by="MRT_LRT_FROM")
nodes_mrtlrt_interchg_clean <- subset(nodes_mrtlrt_crossjoin, MRT_LRT_LINE_FROM.x != MRT_LRT_LINE_FROM.y)
nodes_mrtlrt_interchg_clean$TIME_MINS <- 5

#Remove TWE lines because it is an extension
nodes_mrtlrt_interchg_clean <- subset(nodes_mrtlrt_interchg_clean, LINE.x != 'TWE' & LINE.y != 'TWE')

nodes_mrtlrt_inter_edges <- subset(nodes_mrtlrt_interchg_clean, select = c('MRT_LRT_LINE_FROM.x','MRT_LRT_LINE_FROM.y','LINE.x','LINE.y','TIME_MINS'))

#Checking lines that are not completed for interchanges.
nodes_mrtlrt_inter_edges$COMPLETED <- ifelse((nodes_mrtlrt_inter_edges$LINE.x == 'DTL' & !(nodes_mrtlrt_inter_edges$MRT_LRT_LINE_FROM.x %in% c('45_DTL','75_DTL','92_DTL','67_DTL'))) | (nodes_mrtlrt_inter_edges$LINE.y == 'DTL' & !(nodes_mrtlrt_inter_edges$MRT_LRT_LINE_FROM.y %in% c('45_DTL','75_DTL','92_DTL','67_DTL'))) | nodes_mrtlrt_inter_edges$LINE.x == 'TEL' | nodes_mrtlrt_inter_edges$LINE.y == 'TEL', 'NO','YES')
nodes_mrtlrt_inter_edges$LINE <- paste(nodes_mrtlrt_inter_edges$LINE.x, nodes_mrtlrt_inter_edges$LINE.y, sep = "_")

setnames(nodes_mrtlrt_inter_edges, old= c('MRT_LRT_LINE_FROM.x','MRT_LRT_LINE_FROM.y'), new = c('FROM','TO'))
nodes_mrtlrt_inter_edges <- subset(nodes_mrtlrt_inter_edges, select = c('FROM','TO','LINE','COMPLETED','TIME_MINS'))

rm(nodes_mrtlrt_interchg, nodes_mrtlrt_crossjoin, nodes_mrtlrt_interchg_clean)

######################################
#Calculate edges of walking node to bus stop / MRT

buspeak_waittime <- 8
mrtpeak_waittime <- 3
busnonpeak_waittime <- 10
mrtnonpeak_waittime <- 5

unique_busstop_code <- sqldf('select distinct STOPCODE_BUS_FROM from nodes_bus union select distinct STOPCODE_BUS_TO from nodes_bus ')
unique_busstop_code$STATION_KEY <- strapplyc(unique_busstop_code$STOPCODE_BUS_FROM, "(.*)_", simplify = TRUE)
setnames(unique_busstop_code, old= c('STOPCODE_BUS_FROM'), new = c('STATION_CODE'))

unique_mrtline_code <- sqldf('select distinct MRT_LRT_LINE_FROM from nodes_mrtlrt union select distinct MRT_LRT_LINE_TO from nodes_mrtlrt ')
unique_mrtline_code$STATION_KEY <- strapplyc(unique_mrtline_code$MRT_LRT_LINE_FROM, "(.*)_", simplify = TRUE)
setnames(unique_mrtline_code, old= c('MRT_LRT_LINE_FROM'), new = c('STATION_CODE'))
unique_station_codes <- rbind(unique_mrtline_code, unique_busstop_code)

peak_walknodes_busMRTLRT <- unique_station_codes
nonpeak_walknodes_busMRTLRT <- unique_station_codes
busMRTLRT_walknodes <- unique_station_codes

#Model peak wait times
peak_walknodes_busMRTLRT <- rename(peak_walknodes_busMRTLRT, c("STATION_KEY"="FROM", "STATION_CODE"="TO"))
peak_walknodes_busMRTLRT$FROM <- as.integer(peak_walknodes_busMRTLRT$FROM)
peak_walknodes_busMRTLRT$TIME_MINS <- ifelse(peak_walknodes_busMRTLRT$FROM < 1000, mrtpeak_waittime, buspeak_waittime)
peak_walknodes_busMRTLRT$FROM <- paste(peak_walknodes_busMRTLRT$FROM, "_", "W", sep = "")

#Model nonpeak wait times
nonpeak_walknodes_busMRTLRT <- rename(nonpeak_walknodes_busMRTLRT, c("STATION_KEY"="FROM", "STATION_CODE"="TO"))
nonpeak_walknodes_busMRTLRT$FROM <- as.integer(nonpeak_walknodes_busMRTLRT$FROM)
nonpeak_walknodes_busMRTLRT$TIME_MINS <- ifelse(nonpeak_walknodes_busMRTLRT$FROM < 1000, mrtnonpeak_waittime, busnonpeak_waittime)
nonpeak_walknodes_busMRTLRT$FROM <- paste(nonpeak_walknodes_busMRTLRT$FROM, "_", "W", sep = "")

#Model bus mrt lrt node to walking node
busMRTLRT_walknodes <- rename(busMRTLRT_walknodes, c("STATION_KEY"="TO", "STATION_CODE"="FROM"))
busMRTLRT_walknodes$TIME_MINS <- 0
busMRTLRT_walknodes$TO <- paste(busMRTLRT_walknodes$TO, "_", "W", sep = "")

#Final results for walk edges
peak_walk_edges_final <- do.call(rbind, list(nodes_walk_edges, peak_walknodes_busMRTLRT, busMRTLRT_walknodes))
peak_walk_edges_final$LINE <- 'WALK'
peak_walk_edges_final$COMPLETED <- 'YES'
nonpeak_walk_edges_final <- do.call(rbind, list(nodes_walk_edges, nonpeak_walknodes_busMRTLRT, busMRTLRT_walknodes))
nonpeak_walk_edges_final$LINE <- 'WALK'
nonpeak_walk_edges_final$COMPLETED <- 'YES'

######################################
#Aggregate all peak edges value

final_nodes_peak_edges_detail <- do.call(rbind, list(peak_walk_edges_final, nodes_bus_peak_edges, nodes_bus_inter_edges, nodes_mrtlrt_edges, nodes_mrtlrt_inter_edges))

final_nodes_peak_edges_detail$FROM_GEOSPATIAL_KEY <- strapplyc(final_nodes_peak_edges_detail$FROM, "(.*)_", simplify = TRUE)
final_nodes_peak_edges_detail$TO_GEOSPATIAL_KEY <- strapplyc(final_nodes_peak_edges_detail$TO, "(.*)_", simplify = TRUE)

#geonode_walknode is a bit unique. the starting node does not have underscore....

final_nodes_peak_edges_detail <- rbind(final_nodes_peak_edges_detail, geonode_walknode)

write.csv(final_nodes_peak_edges_detail,'.//data//DetailedPeakTransportNodesEdges.csv', row.names = FALSE)

final_nodes_peak_edges <-aggregate(TIME_MINS ~ FROM_GEOSPATIAL_KEY + FROM + TO_GEOSPATIAL_KEY + TO + COMPLETED, data=final_nodes_peak_edges_detail, FUN=sum, na.rm=TRUE)

write.csv(final_nodes_peak_edges,'.//data//FinalPeakTransportNodesEdges.csv', row.names = FALSE)

######################################
#Aggregate all non-peak edges value

final_nodes_nonpeak_edges_detail <- do.call(rbind, list(nonpeak_walk_edges_final, nodes_bus_nonpeak_edges, nodes_bus_inter_edges, nodes_mrtlrt_edges, nodes_mrtlrt_inter_edges))

final_nodes_nonpeak_edges_detail$FROM_GEOSPATIAL_KEY <- strapplyc(final_nodes_nonpeak_edges_detail$FROM, "(.*)_", simplify = TRUE)
final_nodes_nonpeak_edges_detail$TO_GEOSPATIAL_KEY <- strapplyc(final_nodes_nonpeak_edges_detail$TO, "(.*)_", simplify = TRUE)

#geonode_walknode is a bit unique. the starting node does not have underscore....
final_nodes_nonpeak_edges_detail <- rbind(final_nodes_nonpeak_edges_detail, geonode_walknode)

write.csv(final_nodes_nonpeak_edges_detail,'.//data//DetailedNonPeakTransportNodesEdges.csv', row.names = FALSE)

final_nodes_nonpeak_edges <-aggregate(TIME_MINS ~ FROM_GEOSPATIAL_KEY + FROM + TO_GEOSPATIAL_KEY + TO + COMPLETED, data=final_nodes_nonpeak_edges_detail, FUN=sum, na.rm=TRUE)

write.csv(final_nodes_nonpeak_edges,'.//data//FinalNonPeakTransportNodesEdges.csv', row.names = FALSE)


#For testing
final_nodes_peak_edges$FROM[!grepl("_", final_nodes_peak_edges$FROM)]

#######################################
#Others (Codes not in use)


sqldf('select "NODE_DESC.x" from nodes_100m where "NODE_TRANSPORT.x"="MRT"  except select "NODE_DESC.x" from nodes_200m where "NODE_TRANSPORT.x"="MRT"')

sqldf('select "NODE_DESC.x", count(*) from nodes_200m where "NODE_TRANSPORT.x"="MRT" group by "NODE_KEY.x" order by "NODE_DESC.x"')

sqldf('select * from nodes_200m where "NODE_DESC.x" = "Jurong East"')

sqldf('select * from nodes_100m where "NODE_DESC.x" = "Jurong East"')

ptm <- proc.time()
nodes_simple_clean$DistHaversine <- apply(nodes_simple_clean[,c('LONGITUDE.x','LATITUDE.x','LONGITUDE.y','LATITUDE.y')], 1, function(y) distHaversine(c(y['LONGITUDE.x'],y['LATITUDE.x']),c(y['LONGITUDE.y'],y['LATITUDE.y'])))
proc.time() - ptm

ptm <- proc.time()
for(i in 1:nrow(nodes_simple_clean)) {
  nodes_simple_clean$distance[i] <- distHaversine(c(nodes_simple_clean$LONGITUDE.x[i], nodes_simple_clean$LATITUDE.x[i]), c(nodes_simple_clean$LONGITUDE.y[i], nodes_simple_clean$LATITUDE.y[i]))
}
proc.time() - ptm

pb = txtProgressBar(min = 0, max = 200, initial = 0) 

ptm <- proc.time()

for(i in 1:200) {
  nodes_clean$distance[i] <- distHaversine(c(nodes_clean$LONGITUDE.x[i], nodes_clean$LATITUDE.x[i]), c(nodes_clean$LONGITUDE.y[i], nodes_clean$LATITUDE.y[i]) )
  setTxtProgressBar(pb,i)
}

nodes_clean$distance <- NULL


distHaversine(c(nodes_simple_clean$LONGITUDE.x,nodes_simple_clean),c(nodes_simple_clean,nodes_simple_clean$LATITUDE.y))


nodes_simple_clean$distance_2 <- gcd.hf(nodes_simple_clean$LONGITUDE.x, nodes_simple_clean$LATITUDE.x, nodes_simple_clean$LONGITUDE.y, nodes_simple_clean$LATITUDE.y)


write.csv(nodes_simple_clean,'./Test.csv')


#nodes_simple <- nodes[1:200,]
#nodes_simple_crossjoin <- merge(nodes_simple, nodes_simple, by="dummy")
#nodes_simple_clean <- subset(nodes_simple_crossjoin, NODE_KEY.x != NODE_KEY.y)
