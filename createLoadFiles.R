# R script for generating all files to load into wd (with fastload)
source("miopdbLoad.R")
selectedmodels <-c("HIRLAM1","PARLAM10","HIRLAM20KM1","HIRLAM4KM1","HIRLAM8KM1","HIRLAM12KM1","EC1","ECMWF1","UK1","UM4KM1","HIRLAM2","HIRLAM20KM2","HIRLAM12KM2","EC2","ECMWF2","UK2") 

for (i in 1:length(selectedmodels)){
  miopdbModelLoad(selectedmodels[i])
  gzipcommand <- paste("gzip ",datadir,selectedmodels[i],"_*;",sep="")
  system(gzipcommand)
}
