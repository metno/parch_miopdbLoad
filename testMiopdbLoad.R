library(DBI)
library(RPostgreSQL)
# miIO, internal met.no package for Input/output
library(miIO)

source("miopdbLoadConf.R")
source("miopdbLoad.R")
source("createSqlFile.R")

# wdb2R, download from  git://github.com/wdb/wdb2R.git
source("../wdb2R/readVerifWdb.R")
# set startyear and endyear
startyear <- 1996
endyear <- 1997
startedup <- FALSE
errorfile <- "testMiopdbLoad.out"
errorcount <-0
errorstrings <<-c()
miopdbtimes <<- c()
wdbtimes <- c()
# compare miVerifDB(miopdb reading) and readVerifDB(wdb reading)?
validate <- TRUE
# compare total number of entries in miopdb and wdb for each parameter
testTotal <- TRUE


# test migration of all tables in a model: f.ex. testMiopdbModelLoad("HIRLAM")
# test migration for one table: f.ex. testMiopdbTableLoad("HIRLAM_1010")

startup<-function(){
  cat("Startup\n")
 #startup; load driver, connect to wdb etc
  drv<<-dbDriver("PostgreSQL")
  con<<-dbConnect(drv,  dbname=dbname,  user=user, host=host)
  wciselect <- paste("select wci.begin('wdb',",namespace,")",sep="")
  rs<-suppressWarnings(dbSendQuery(con,wciselect))
  dbClearResult(rs)
  startedup <<- TRUE
}


testMiopdbModelLoad <- function(modelName){
  modelOK <- TRUE
  # list all tables for this model(one table for each station)
  modelOutputFile <- createModelSqlFile(modelName)
  system(sqlpluscommand)
 # Read list of all the table names
  tablenameList<-scan(modelOutputFile,what=list(tablename=character()))
 # vector with table names
  tablenameVector<-tablenameList$tablename
  #write to errorfile
  sink(errorfile,append=FALSE,split=TRUE)
  cat("Check model ",modelName,"\n")
  cat(length(tablenameVector), " tables found\n")
  # do not write to errorfile
  sink()

  for (i in 1:length(tablenameVector)){
  #for (i in 1:1){
    if (!testMiopdbTableLoad(tablenameVector[i],TRUE)){
      modelOK <- FALSE
    }
  }
  sink(errorfile,append=TRUE,split=TRUE)
  if (!modelOK)
    cat("Failure when checking model, ", modelName,"\n")
  else
    cat("Success when checking model, ", modelName,"\n")
  sink()
}


testMiopdbTableLoad <- function(tableName,append=FALSE){
 # log on to wdb
  if (!startedup){
    cat("Call startup\n")
    startup()
  }
  #
  sink(errorfile,append=append,split=TRUE)
  cat("Checking table ", tableName,"\n")
  sink()
  errorcount <<-0
  errorstrings <<-c()

  tableInfo <- getTableInfo(tableName)
  if(length(tableInfo)==0)
    return(FALSE)
  dataprovider <- tableInfo$dataprovider
  shortmodelname <- tableInfo$shortmodelname
  stationid <- tableInfo$stationid

#  if (bugFound(stationid)==TRUE)
#    return(FALSE)
    
  okToProceed <- TRUE
  if (testTotal){
    # check number of entries in wdb and miopdb
    if (testTotalCount(tableName,tableInfo,c(startyear,endyear))<0)
      okToProceed <- FALSE
  }

  if (okToProceed==TRUE){
    for (validyear in startyear:endyear)
      compareMiopdbWdb(tableName,tableInfo,validyear)
  }
  
  sink(errorfile,append=TRUE,split=TRUE)
  if (errorcount!=0){
    cat("Failure when checking table, ", tableName,".", errorcount,  "errors found\n")
    for(error in errorstrings)
      cat(error)
    sink()
    return(FALSE)
  }
  else{
    cat("Success when checking table, ", tableName, ",", errorcount, "errors found\n")
    sink()
    return(TRUE)
  }

}


compareMiopdbWdb <- function(tableName,tableInfo,validyear){

  dataprovider <- tableInfo$dataprovider
  shortmodelname <- tableInfo$shortmodelname
  stationid <- tableInfo$stationid

  # find parameters, progs and levels in miopdb table
  parameters <- getParameters(tableName)
  progs <- getProgs(tableName)
  levels <- c(NA)
  if ("NIVA" %in% parameters){
    levels <- getLevels(tableName)
  }
  
  #check total number of dataentries in wdb and miopdb
  if (testTotal){
    miopdbcount <- testTotalCount(tableName,tableInfo,validyear)
    if(miopdbcount<0)
      return(FALSE)
    cat("Resulting miopdbcount",miopdbcount,"\n")
  }


  
  for (parameter in parameters){
    print(parameter)
    if (validate){
      validateMiopdbWdb(validyear,stationid,shortmodelname,parameter,progs,levels)
    }
    
    if (testTotal){
      wdbcount <- getWdbCount(parameter,dataprovider,stationid,validyear,levels)
      if (!is.na(wdbcount)){
        cat("Result from wdb ",wdbcount,"\n")
        diff <- wdbcount-miopdbcount
        cat("Difference wdb-miopdb", diff,"\n")
        
        errorstring <- paste("table:",tableName,", dataprovider:",dataprovider,",station:",stationid,", year:",validyear, ", parameter:", parameter, ", valueparametername:",valueparametername,", result from wdb:", wdbcount,", result from miopdb:",miopdbcount ,", difference",diff,"\n")
        if (diff != 0) {
          handleError(errorstring)
       } else{
         errorstring <- paste("Success: ", errorstring)
       }
       cat(errorstring)
       }

     }


   }    



 }



 validateMiopdbWdb <- function(validyear,stationid,shortmodelname,parameter,progs,levels){
   cat("validateMiopdbWdb",parameter,"\n")
   pdef <- parameterDefinitions[parameterDefinitions$miopdb_par==parameter,]
   if (nrow(pdef)==0 || is.null(parameter))
     return()
   if (length(validyear)==1){
     statmodyearpar <- paste(stationid,shortmodelname,parameter,validyear)
     validyear=c(validyear,validyear)
   } else{
     statmodyearpar <- paste(stationid,shortmodelname,parameter,paste(validyear,collapse="-"))
   }

   errorstring <- NULL

   for (level in levels){ 

     lev1 <- level
     if (is.na(level)){
       lev1 <- NULL
     }

     startperiod <- as.numeric(paste(validyear[1],"0101",sep=""))
     endperiod <- as.numeric(paste(validyear[2],"0107",sep=""))
     cat("call miVerifDB\n")
     # miVerifDB reads from miopdb
     miopdbtime <- system.time(miopdb.data <- miVerifDB(wmo_no = stationid,period=c(startperiod,endperiod),model=shortmodelname,prm=parameter,prg=progs,lev=lev1,init.time=0))
     cat("Time in seconds, read data from miopdb, user:",  miopdbtime[1],"system:",miopdbtime[2], "elapsed:", miopdbtime[3],"\n")
     miopdbtimes <<- c(miopdbtimes,miopdbtime)
     cat("call readVerifWdb\n")
  # readVerifWdb reads from wdb
     wdbtime <- system.time(wdb.data <- readVerifWdb(wmo_no = stationid,period=c(startperiod,endperiod),model=shortmodelname,prm=parameter,prg=progs,lev=lev1,init.time=0))
     cat("Time in seconds, read data from wdb, user:",  wdbtime[1],"system:",wdbtime[2], "elapsed:", wdbtime[3],"\n")
     wdbtimes <<- c(wdbtimes,wdbtime)
     if (!all(dim(miopdb.data)==dim(wdb.data))){
       errorstring <- paste (statmodyearpar, ": Different dimensions, miopdb.data",paste(dim(miopdb.data),collapse=" "),"wdb.data",paste(dim(wdb.data),collapse=" "),"\n")
       handleError(errorstring)
     }else if (nrow(wdb.data)==0){
       return()
     } else{
     #sort wdb.data
       modpar <- paste(parameter,shortmodelname,sep=".")
       wdb.data <- wdb.data[with(wdb.data,order(time,prog)),]
       print(head(miopdb.data))
       print(head(wdb.data))
       meanDiff <- mean( abs(wdb.data[modpar] - miopdb.data[parameter])< 0.01)
       allOK <- all( abs(wdb.data[modpar] - miopdb.data[parameter]) < 0.01 )
       if (allOK & !(is.na(meanDiff))){
         cat("Success:",statmodyearpar, "Difference less than 0.01 on all data\n")
       }
       else{
         errorstring <- paste (statmodyearpar,"Difference less than 0.01 on ", meanDiff*100 ,"percent of data\n")
         handleError(errorstring)
       }
   }
     if (!is.null(errorstring)){
       print((miopdb.data))
       print((wdb.data))
     }
   }

 }


 getWdbCount <- function(parameter,dataprovider,stationid,validyear,levels){

   cat("getWdbCount", parameter,"\n")  

   #check if OK parameter
   pdef <- parameterDefinitions[parameterDefinitions$miopdb_par==parameter,]
   if (nrow(pdef)!=0 || is.null(parameter)) {
     if(is.null(parameter)){
       parameterstring <- "NULL"
     } else {
       valueparametername <<- as.character(pdef$valueparametername)
       parameterstring<-paste("ARRAY['",valueparametername,"']",sep="")

     }
     dataproviderstring<-paste("ARRAY['",dataprovider,"']",sep="")
     stationstring<-paste("'",stationid,"'",sep="")
     totalWdbcount <- 0
   } else {
     totalWdbcount <- NA
     return(totalWdbcount)
   }


   # validtime start and stop at new year
  if (length(validyear)==1) validyear=c(validyear,validyear)
  validtimestartstring <-paste(validyear[1],"-01-01 00:00:00Z",sep="") 
  validtimeendstring <- paste(validyear[2],"-12-31 23:00:00Z",sep="")
  #ref time subtract 300 hours(proglength max 240 hours)
  reftimestart<-strptime(validtimestartstring,"%Y-%m-%d %H:%M:%SZ")
  reftimestart <- reftimestart-300*3600
  reftimestartstring <- format(reftimestart,"%Y-%m-%d %H:%M:%S")
  reftimestring<-paste("'inside ",reftimestartstring," TO ",validtimeendstring,"'",sep="")
  validtimestring<-paste("'inside ",validtimestartstring," TO ",validtimeendstring,"'",sep="")
  
  for (level in levels){ 

    lev2 <- level*100

    if(is.null(parameter)){
      levelstring <- "NULL"      
    } else {
      if (is.na(level)){
        levelparametername <- as.character(pdef$groundlevelparametername)
        level <- as.character(pdef$groundlevel)
      } else {
        levelparametername <- as.character(pdef$levelparametername)
        level <- format(lev2,scientific=FALSE)
      }
      levelstring <- paste("'",level," ",levelparametername,"'",sep="")
    }
    
    query <- paste("select count(*) from wci.read(",dataproviderstring,",",stationstring,",",reftimestring,",",validtimestring,",",parameterstring,",",levelstring,",NULL,NULL::wci.returnfloat )")
    print(query)
    rs <- dbSendQuery(con, query)
    results<-fetch(rs,n=-1)
    wdbcount <- results[1,1]
    totalWdbcount <- totalWdbcount+wdbcount
    cat("Resulting wdbcount ", wdbcount, totalWdbcount,"level", level,"\n") 
  }

  #return sum of all levels
  return(totalWdbcount)
}


 createLevelTableSqlFile <- function(tableName){
  tableLevelFile <- paste(tableName,".levels",sep="")
  tableQuery <- createLevelTableQuery(tableName)
  createSqlFile(tableQuery,tableLevelFile,FALSE)
  return(tableLevelFile)
}

createProgTableSqlFile <- function(tableName){
  tableProgFile <- paste(tableName,".prog",sep="")
  tableQuery <- createProgTableQuery(tableName)
  createSqlFile(tableQuery,tableProgFile,FALSE)
  return(tableProgFile)
}


createColumnsTableSqlFile <- function(tableName){
  tableColumnsFile <- paste(tableName,".columns",sep="")
  tableQuery <- createColumnsTableQuery(tableName)
  createSqlFile(tableQuery,tableColumnsFile,FALSE)
  return(tableColumnsFile)
}

createCountTableSqlFile <- function(tableName,validyear){
  if (length(validyear)==1){
    tableCountFile <- paste(tableName,"_",validyear,".count",sep="")
  }else{
    tableCountFile <- paste(tableName,".count",sep="")
  }
  tableQuery <- createCountTableQuery(tableName,validyear)
  createSqlFile(tableQuery,tableCountFile,FALSE)
  return(tableCountFile)
}

createCountTableQuery <- function(tableName,validyear){
  if (missing(tableName)) 
        stop(" ==>  table name(s)  not specified.")
  if (length(validyear)==1){
    tableQuery <- paste("select count(*) from verifop.",tableName," where aar=",validyear,";\n",sep="")
  } else {
    tableQuery <- paste("select count(*) from verifop.",tableName," where aar>=",validyear[1]," and aar <=",validyear[2],";\n",sep="")

  }
      print(tableQuery)
  return(tableQuery)
}


createProgTableQuery <- function(tableName){
  if (missing(tableName)) 
        stop(" ==>  table name  not specified.")
  tableQuery <- paste("select distinct(prog) from verifop.",tableName,";\n",sep="")
   print(tableQuery)
  return(tableQuery)
}


createLevelTableQuery <- function(tableName){
  if (missing(tableName)) 
        stop(" ==>  table name  not specified.")
  tableQuery <- paste("select distinct(niva) from verifop.",tableName,";\n",sep="")
   print(tableQuery)
  return(tableQuery)
}


createColumnsTableQuery <- function(tableName){
if (missing(tableName)) 
        stop(" ==>  table name  not specified.")
  tableQuery <- paste("select column_name from all_tab_columns where table_name='",tableName,"';\n",sep="")
  print(tableQuery)
  return(tableQuery)
}

handleError <- function(errorstring){
  errorstring <- paste("Failure: ", errorstring)
  cat(errorstring)
  errorstrings <<- c(errorstrings,errorstring)
  errorcount <<- errorcount + 1
}



getTableInfo <- function(tableName){
  tableElements <- unlist(strsplit(tableName,"_"))
  lengthTableElements <- length(tableElements)
  # assume stationid is everything after the last underscore
  stationid <- tableElements[lengthTableElements]
  if (is.na(as.numeric(stationid))){
    cat("Non numeric stationid in table", tableName,"\n")
    return(list())
  }
  # find data provider and stationid from tablemodename
  tablemodelname <- paste(tableElements[-lengthTableElements],collapse="_")
  dataprovider<-dataproviderDefinitions[dataproviderDefinitions$tablemodelname==tablemodelname,]$dataprovider
  shortmodelname<-dataproviderDefinitions[dataproviderDefinitions$tablemodelname==tablemodelname,]$shortmodelname
  if (length(dataprovider)==0){
    cat("Data provider not found for ", tablemodelname,"\n")
    return(list())
  }
  
  cat("The dataprovider is", dataprovider,"\n")
  cat("The shortmodelname is", shortmodelname,"\n")
  cat("The station is", stationid,"\n")

  tableInfo <- list(dataprovider=dataprovider,shortmodelname=shortmodelname,stationid=stationid)
  return(tableInfo)
  
}


 bugFound <- function(stationid){
  # test for bug in wdb
  query <- paste("SELECT count(*) FROM wdb_int.placename where placename = '",stationid,"'",sep="")
  print(query)
  rs <- dbSendQuery(con, query)
  results<-fetch(rs,n=-1)
  print(results)
  if (results[1,1]>1){
    sink(errorfile,append=TRUE,split=TRUE)
    cat("Failure when checking table, ", stationid,".",results[1,1], "placename results \n")
    sink()
    return(TRUE)
  }
  return(FALSE)
}



testTotalCount <- function(tableName,tableInfo,validyear){
   # test total results from miopdb and wdb
  dataprovider <- tableInfo$dataprovider
  shortmodelname <- tableInfo$shortmodelname
  stationid <- tableInfo$stationid
  years <- paste(validyear,collapse="-")
  
  tableCountFile <- createCountTableSqlFile(tableName,validyear)
  system(sqlpluscommand)
  tablecountList<-scan(tableCountFile,what=list(count=integer()))
  miopdbcount <- tablecountList$count[1]
  cat("Result from miopdb, year",years,"count:",miopdbcount,"\n")
  removecommand <- paste("rm ", tableCountFile)
  system(removecommand)

  levels <- c(NA)
  wdbcount <- getWdbCount(NULL,dataprovider,stationid,validyear,levels)
  if (!is.na(wdbcount)){
    cat("Result from wdb, year:",years,"count",wdbcount,"\n")
    if (wdbcount==0 && miopdbcount !=0){
      errorstring <- paste("table:",tableName,", dataprovider:",dataprovider,",station :",stationid,", year:", years, ", result from wdb: ", wdbcount,", result from miopdb:",miopdbcount ,"\n")
      handleError( errorstring)
      return(-1)
    }
  }
  return(miopdbcount)
}
  


getParameters <- function(tableName){
  #find out which columns table has
  tableColumnsFile <- createColumnsTableSqlFile(tableName);
  system(sqlpluscommand)
  parameterlist<-scan(tableColumnsFile,what=list(parameter=character()))
  parameters<-parameterlist$parameter
}

getProgs <- function(tableName){
  #find out which prog length table has
  tableProgFile <- createProgTableSqlFile(tableName);
  system(sqlpluscommand)
  proglist<-scan(tableProgFile,what=list(prog=integer()))
  progs<-proglist$prog
  progs<-progs[progs!=0]
# only progs that are multiples of 3
  progs<-progs[progs%%3==0]	
}

  
getLevels <- function(tableName){
  #find out which levels table has
  tableLevelFile <- createLevelTableSqlFile(tableName);
  system(sqlpluscommand)
  levellist<-scan(tableLevelFile,what=list(level=integer()))
  levels<-levellist$level
}
