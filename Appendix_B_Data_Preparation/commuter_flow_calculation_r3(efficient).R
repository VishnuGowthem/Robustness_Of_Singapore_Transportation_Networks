#install.packages("dplyr")
library(dplyr)

#read data
SGPopulationDist <- read.csv("H://Projects//SMDA//data//Dist//SGPopulationDist.csv")
Node_Subzone <- read.csv("H://Projects//SMDA//data//Dist//Node_Subzone_Dummy.csv")

#define proportions
ComCBDPct <- 0.75
ComNCBDPct <- 1-ComCBDPct

#generate counts of nodes in each subzone
Subzone_node_count <- table(Node_Subzone["Subzone"])
Subzone_node_count <- as.data.frame(Subzone_node_count)
colnames(Subzone_node_count) <- c("Subzone", "Subzone_node_count")


#counts numbers of nodes in CBD and out of CBD
CBD_count <- table(Node_Subzone["ZoneType"])
CBD_count <- as.data.frame(CBD_count)
CBD_node_count <- CBD_count$Freq[CBD_count$Var1=="CBD"]
NCBD_node_count <- CBD_count$Freq[CBD_count$Var1=="NCBD"]
rm(CBD_count)

#populate node pairs
paired_nodes <- expand.grid (x=Node_Subzone$Node_ID, y=Node_Subzone$Node_ID)
colnames(paired_nodes) <- c("StartNode", "EndNode")
temp <- Node_Subzone
colnames(temp)[which(colnames(temp)=="Node_ID")] <- "StartNode"
colnames(temp)[which(colnames(temp)=="Subzone")] <- "StartSubzone"
colnames(temp)[which(colnames(temp)=="ZoneType")] <- "StartZoneType"
paired_nodes <- merge(paired_nodes, temp, by = "StartNode")
rm(temp)
temp <- Node_Subzone
colnames(temp)[which(colnames(temp)=="Node_ID")] <- "EndNode"
colnames(temp)[which(colnames(temp)=="Subzone")] <- "EndSubzone"
colnames(temp)[which(colnames(temp)=="ZoneType")] <- "EndZoneType"
paired_nodes <- merge(paired_nodes, temp, by = "EndNode")
rm(temp)

#remove StartNode in CBD, and StartNode=EndNode
paired_nodes <- paired_nodes[!(paired_nodes$StartNode==paired_nodes$EndNode),]
paired_nodes <- paired_nodes[(paired_nodes$StartZoneType=="NCBD"),]

#create nodes template
paired_nodes2 <- subset(paired_nodes, select=c(StartSubzone,StartZoneType,EndZoneType))
paired_nodes2 <- distinct(paired_nodes2[c("StartSubzone","StartZoneType","EndZoneType")])

#calculating commuter count via loops
paired_nodes2$CommuterCount <- NA
x <- grep("EndZoneType", colnames(paired_nodes2))
y <- grep("CommuterCount", colnames(paired_nodes2))
z <- grep("StartSubzone", colnames(paired_nodes2))

for (i in 1:nrow(paired_nodes2)) {
if (paired_nodes2[i,x] =="NCBD") {
  paired_nodes2[i,y] = ComNCBDPct * 
    (SGPopulationDist$WorkingPopulation[SGPopulationDist$SubzoneRenamed==as.character(paired_nodes2[i,z])])/
    Subzone_node_count$Subzone_node_count[Subzone_node_count$Subzone==as.character(paired_nodes2[i,z])]/
    NCBD_node_count
} else {
  paired_nodes2[i,y] = ComCBDPct *
    (SGPopulationDist$WorkingPopulation[SGPopulationDist$SubzoneRenamed==as.character(paired_nodes2[i,z])])/
    Subzone_node_count$Subzone_node_count[Subzone_node_count$Subzone==as.character(paired_nodes2[i,z])]/
    CBD_node_count
}
}
paired_nodes3 <- merge(paired_nodes,paired_nodes2)
rm(paired_nodes,paired_nodes2)

#exports results
write.csv(paired_nodes3,file="H://Projects//SMDA//data//Dist//commuter_flow_count.csv")
