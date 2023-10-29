# https://gking.harvard.edu/files/gking/files/ei.pdf
library(ei)
source("/Users/zach/UCLA Anderson Dropbox/Zach Siegel/UCLA_classes/research/Polling/src/eco_inference/misc/eir-master/R/bounds.R")
source("/Users/zach/UCLA Anderson Dropbox/Zach Siegel/UCLA_classes/research/Polling/src/eco_inference/misc/eir-master/R/ei.R")
source("/Users/zach/UCLA Anderson Dropbox/Zach Siegel/UCLA_classes/research/Polling/src/eco_inference/misc/eir-master/R/eiRxCplot.R")
source("/Users/zach/UCLA Anderson Dropbox/Zach Siegel/UCLA_classes/research/Polling/src/eco_inference/misc/eir-master/R/eiread.R")
source("/Users/zach/UCLA Anderson Dropbox/Zach Siegel/UCLA_classes/research/Polling/src/eco_inference/misc/eir-master/R/likelihoodfn.R")
source("/Users/zach/UCLA Anderson Dropbox/Zach Siegel/UCLA_classes/research/Polling/src/eco_inference/misc/eir-master/R/plot.R")
source("/Users/zach/UCLA Anderson Dropbox/Zach Siegel/UCLA_classes/research/Polling/src/eco_inference/misc/eir-master/R/summary.R")
source("/Users/zach/UCLA Anderson Dropbox/Zach Siegel/UCLA_classes/research/Polling/src/eco_inference/misc/eir-master/R/summary.R")
source("/Users/zach/UCLA Anderson Dropbox/Zach Siegel/UCLA_classes/research/Polling/src/eco_inference/misc/eir-master/R/zzz.R")


data = read.csv("/Users/zach/UCLA Anderson Dropbox/Zach Siegel/UCLA_classes/research/Polling/data/census_pre_fetch/Georgia_tract_ACS5.csv")

frac_black = data["Total_Black_or_African_American_alone"]/data["Total"]
frac_vehicle_owner = 1- data["Total_No_vehicle_available"]/data['Total']
total = data['Total']

data['x'] = data['Total_Black_or_African_American_alone']/data['Total']
data['t'] = 1- data['Total_No_vehicle_available']/data['Total']
data['n'] = data['Total']

df = data[c('t','x','n')]
df$n[df$n <=0] = NA
df$t[df$t <=0] = NA
df$t[df$t >= 1] = NA
df$x[df$x <=0] = NA
df$x[df$x >= 1] = NA
df = na.omit(df)

formula= t ~ x
dbuf = ei(formula=formula, total="n", data=df)
summary(dbuf)
bb.out <- eiread(dbuf, "betab", "betaw", "sbetab", "sbetaw") #,"sigmaW","sigmaB"



b_i_B = bb.out$betab



# Necessary to avoid errors... For some reason the eir package does not load these files correctly
# source("/Users/zach/UCLA Anderson Dropbox/Zach Siegel/UCLA_classes/research/Polling/src/eco_inference/misc/eir-master/R/plot.R")
# source("/Users/zach/UCLA Anderson Dropbox/Zach Siegel/UCLA_classes/research/Polling/src/eco_inference/misc/eir-master/R/zzz.R")






# plot(dbuf,"tomog")
# plot(dbuf, "tomogE")
# plot(dbuf,"tomogCI")
plot(dbuf, "tomog", "betab", "betaw", "xtfit")

# EI SAMPLE
# library(ei)
# data(sample)
# formula = t ~ x
# dbuf = ei(formula=formula,total="n",data=sample)
# summary(dbuf)
eiread(dbuf, "betab", "betaw")
# plot(dbuf, "tomog", "betab", "betaw", "xtfit")
# EI SAMPLE END

# tomography plot source
# https://github.com/iqss-research/eir/blob/master/R/zzz.R

# data  = read.csv("~/Downloads/eir-master/data/matproii.txt", sep=" ")
# formula= t ~ x
# dbuf = ei(formula=formula, total="n", data=data)
# plot(dbuf, "tomog")

# "Total"
# "Total_Black_or_African_American_alone"
# "Total_No_vehicle_available"
# "Total_1_vehicle_available"
# "Total_2_vehicles_available"
# "Total_3_vehicles_available"
# "Total_4_vehicles_available"
# "Total_5_or_more_vehicles_available"
# "Aggregate_number_of_vehicles_car,_truck,_o"
# "Total_Car,_truck,_or_van_"
# "Total_Public_transportation_excluding_taxi"
# "Total_Bicycle"
# "Total_Walked"
# "Total_Taxicab,_motorcycle,_or_other_means"
# "Total_Worked_from_home"
