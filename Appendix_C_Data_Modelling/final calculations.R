library(data.table)

peak_shortestpath <- read.csv('H://Projects//SMDA//data//PeakComplete_ShortestPath.csv')

peak_baseline_calc <- merge(paired_nodes3, peak_shortestpath, by.x = c("StartNode","EndNode"), by.y = c("ORIGIN","DESTINATION")) 

peak_baseline_calc$BaseTotalTime <- peak_baseline_calc$CommuterCount * peak_baseline_calc$TIME_MINS

peak_baseline_t <- data.table(peak_baseline_calc)

agg_baseline <- peak_baseline_t[, list(BaseTotalTime = sum(BaseTotalTime)), by = c('StartSubzone')]

rm(peak_baseline_calc, peak_baseline_t, peak_shortestpath)
########################
#Inclusion of future MRT lines

peak_allmrt_shortestpath <- read.csv('H://Projects//SMDA//data//Peak_IncludeNewMRT_ShortestPath.csv')

peak_allmrt_calc <- merge(paired_nodes3, peak_allmrt_shortestpath, by.x = c("StartNode","EndNode"), by.y = c("ORIGIN","DESTINATION")) 

peak_allmrt_calc$TotalTime <- peak_allmrt_calc$CommuterCount * peak_allmrt_calc$TIME_MINS

peak_allmrt_t <- data.table(peak_allmrt_calc)

agg_allmrt <- peak_allmrt_t[, list(TotalTimeIncludeNewMRT = sum(TotalTime)), by = c('StartSubzone')]

agg_baseline_allmrt <- merge(agg_baseline, agg_allmrt, by = 'StartSubzone')

setnames(agg_baseline_allmrt, old= c('StartSubzone'), new = c('SUBZONE'))

agg_baseline_allmrt$TravelTimeDiff <- agg_baseline_allmrt$TotalTimeIncludeNewMRT - agg_baseline_allmrt$BaseTotalTime
agg_baseline_allmrt$PercentageDiff <- agg_baseline_allmrt$TravelTimeDiff / agg_baseline_allmrt$BaseTotalTime * 100

agg_baseline_allmrt$PercentageDiff[is.nan(agg_baseline_allmrt$PercentageDiff)] <- 0

write.csv(agg_baseline_allmrt, 'H://Projects//SMDA//data//3_IncludeNewMRT_TravelTime.csv', row.names=FALSE)

rm(peak_allmrt_calc, peak_allmrt_shortestpath, peak_allmrt_t)

########################
#Removing Bishan to MB Breakdown

bishan_shortestpath <- read.csv('H://Projects//SMDA//data//Peak_Bishan_MB_Breakdown_ShortestPath.csv')

bishan_calc <- merge(paired_nodes3, bishan_shortestpath, by.x = c("StartNode","EndNode"), by.y = c("ORIGIN","DESTINATION")) 

bishan_calc$TotalTime <- bishan_calc$CommuterCount * bishan_calc$TIME_MINS

bishan_t <- data.table(bishan_calc)

agg_bishan <- bishan_t[, list(TotalTimeMinusBishan = sum(TotalTime)), by = c('StartSubzone')]

agg_baseline_bishan <- merge(agg_baseline, agg_bishan, by = 'StartSubzone')

setnames(agg_baseline_bishan, old= c('StartSubzone'), new = c('SUBZONE'))

agg_baseline_bishan$TravelTimeDiff <- agg_baseline_bishan$TotalTimeMinusBishan - agg_baseline_bishan$BaseTotalTime
agg_baseline_bishan$PercentageDiff <- agg_baseline_bishan$TravelTimeDiff / agg_baseline_bishan$BaseTotalTime * 100

agg_baseline_bishan$PercentageDiff[is.nan(agg_baseline_bishan$PercentageDiff)] <- 0

write.csv(agg_baseline_bishan, 'H://Projects//SMDA//data//2_Bishan_MB_Breakdown_TravelTime.csv', row.names=FALSE)

rm(bishan_calc, bishan_shortestpath, bishan_t)
########################
#Removing Bugis to JE Breakdown

bugis_shortestpath <- read.csv('H://Projects//SMDA//data//Peak_Bugis_JE_Breakdown_ShortestPath.csv')

bugis_calc <- merge(paired_nodes3, bugis_shortestpath, by.x = c("StartNode","EndNode"), by.y = c("ORIGIN","DESTINATION")) 

bugis_calc$TotalTime <- bugis_calc$CommuterCount * bugis_calc$TIME_MINS

bugis_t <- data.table(bugis_calc)

agg_bugis <- bugis_t[, list(TotalTimeMinusBugis = sum(TotalTime)), by = c('StartSubzone')]

agg_baseline_bugis <- merge(agg_baseline, agg_bugis, by = 'StartSubzone')

setnames(agg_baseline_bugis, old= c('StartSubzone'), new = c('SUBZONE'))

agg_baseline_bugis$TravelTimeDiff <- agg_baseline_bugis$TotalTimeMinusBugis - agg_baseline_bugis$BaseTotalTime
agg_baseline_bugis$PercentageDiff <- agg_baseline_bugis$TravelTimeDiff / agg_baseline_bugis$BaseTotalTime * 100

agg_baseline_bugis$PercentageDiff[is.nan(agg_baseline_bugis$PercentageDiff)] <- 0

write.csv(agg_baseline_bugis, 'H://Projects//SMDA//data//2_Bugis_JE_Breakdown_TravelTime.csv', row.names=FALSE)

rm(bugis_calc, bugis_shortestpath, bugis_t)

########################
#Removing Woodleigh to Punggol Breakdown

woodleigh_shortestpath <- read.csv('H://Projects//SMDA//data//Peak_Punggol_Woodleigh_Breakdown_ShortestPath.csv')

woodleigh_calc <- merge(paired_nodes3, woodleigh_shortestpath, by.x = c("StartNode","EndNode"), by.y = c("ORIGIN","DESTINATION")) 

woodleigh_calc$TotalTime <- woodleigh_calc$CommuterCount * woodleigh_calc$TIME_MINS

woodleigh_t <- data.table(woodleigh_calc)

agg_woodleigh <- woodleigh_t[, list(TotalTimeMinusWoodleigh = sum(TotalTime)), by = c('StartSubzone')]

agg_baseline_woodleigh <- merge(agg_baseline, agg_woodleigh, by = 'StartSubzone')

setnames(agg_baseline_woodleigh, old= c('StartSubzone'), new = c('SUBZONE'))

agg_baseline_woodleigh$TravelTimeDiff <- agg_baseline_woodleigh$TotalTimeMinusWoodleigh - agg_baseline_woodleigh$BaseTotalTime
agg_baseline_woodleigh$PercentageDiff <- agg_baseline_woodleigh$TravelTimeDiff / agg_baseline_woodleigh$BaseTotalTime * 100

agg_baseline_woodleigh$PercentageDiff[is.nan(agg_baseline_woodleigh$PercentageDiff)] <- 0

write.csv(agg_baseline_woodleigh, 'H://Projects//SMDA//data//2_Woodleigh_Pgl_Breakdown_TravelTime.csv', row.names=FALSE)

rm(woodleigh_calc, woodleigh_shortestpath, woodleigh_t)
