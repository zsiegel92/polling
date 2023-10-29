# ~/UCLA Anderson Dropbox/Zach Siegel/UCLA_classes/research/Polling/src/eco_inference/vehicles_race.R

'''
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
'''

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
