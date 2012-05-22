library(udunits2)
source("miopdbLoadConf.R")
source("createSqlFile.R")

miopdbModelLoad <- function(modelName,dryRun=TRUE){
# list all tables for this model(one table for each station)
  modelOutputFile <- createModelSqlFile(modelName)
  system(sqlpluscommand)
  # Read list of all the table names
  tablenameList<-scan(modelOutputFile,what=list(tablename=character()))
 # vector with table names
 tablenameVector<-tablenameList$tablename
  for (i in 1:length(tablenameVector)){
   if (miopdbTableLoad(tablenameVector[i],dryRun)){
      if (dryRun)
        cat("dry run,", tablenameVector[i], "not loaded\n")
      else
        cat(tablenameVector[i], "loaded\n")
    }
    else{
      cat("error,",tablenameVector[i], "not loaded\n")
    }
  }
  removeFileCommand <- paste("rm ",modelOutputFile,sep="")
  system(removeFileCommand)
  system("rm sql.ctl")

}


miopdbTableLoad <- function(tableName,dryRun=TRUE){
  tableElements <- unlist(strsplit(tableName,"_"))
  lengthTableElements <- length(tableElements)
  # assume stationid is everything after the last underscore
  stationid <- tableElements[lengthTableElements]
  if (is.na(as.numeric(stationid))){
    cat("Non numeric stationid in table ", tableName,"\n")
    return(FALSE)
  }
 # find data provider and stationid from 
  model <- paste(tableElements[-lengthTableElements],collapse="_")
  dataprovider<-dataproviderDefinitions[dataproviderDefinitions$model==model,]$dataprovider
  if (length(dataprovider)==0){
      cat("Data provider not found for ", model,"\n")
      return(FALSE)
    }
  cat("The dataprovider is", dataprovider,"\n")
  cat("The station is", stationid,"\n")

  tableOutputFile <- createTableSqlFile(tableName)
 # create an sql file, sql.ctl for downloading table to file tableOutputFile
  headerFile<- paste(tableName,".head",sep="")
  dataFile <- paste(tableName,".dat",sep="")
  # log onto miopdb and execute the command
  result <- system(sqlpluscommand)
  if (result !=0){
    cat("system command",sqlplus,"failed\n")
    return(FALSE)
  }
  # sqlplus problem, we get several headers in the output file
  grepheaders <- paste("grep \"AAR\"",tableOutputFile," >", headerFile)
  result <- system(grepheaders)
  if (result !=0){
    cat("system command",grepheaders,"failed\n")
    return(FALSE)
  }
  grepnoheaders <- paste("grep -v \"AAR\"",tableOutputFile," >", dataFile)
  resut <- system(grepnoheaders)
  if (result !=0){
    cat("system command",grepnoheaders,"failed\n")
    return(FALSE)
  }

 # write to file for fastload, names tableName.load
  fastloadFile <- paste(tableName,".load",sep="")
  cat(paste(dataprovider,namespace,"\n",sep="\t"), file=fastloadFile)
 # todo deal with NA values, not factor
  headers<-read.table(headerFile,sep="",header=TRUE)
  df<-read.table(dataFile,sep="",header=FALSE)
  names(df)<-names(headers)
  if (!identical(names(df)[1:5],c("AAR","MND","DAG","TIM","PROG"))){
    cat("Wrong headers in sql file")
    return(FALSE)
  }
  for (i in 1:nrow(df)){
    row <- df[i,]
    aar <- row$AAR
    mnd <- row$MND
    dag <- row$DAG
    tim <- row$TIM
    prog <- row$PROG
    validtime <- ISOdatetime(aar,mnd,dag,tim,0,0)
    if (is.na(validtime)){
      cat("Error, in validtime ",paste(unlist(row),collapse=" "), "row", i,"\n")
      return(FALSE)
    }
    timediff <- as.difftime(prog,units="hours")
    referencetime <- validtime-timediff
    firstDataColumn <- 6
    for (j in firstDataColumn:ncol(row)){
     # get parameter name and defintions
     par <- names(row)[j]
     #print(par)
     pdef <- parameterDefinitions[parameterDefinitions$miopdb_par==par,]
     #print(pdef)     
     # exit loop if unknown parameter
     if (nrow(pdef)!=0) {
       valueparametername <- as.character(pdef$valueparametername)
       levelparametername <- as.character(pdef$levelparametername)
       defaultlevel <- as.character(pdef$defaultlevel)
       #TODO, check if defaultlevel to be used
       level<-defaultlevel
       value <- as.character(row[j])
       cat(value, stationid,format(referencetime,"%Y-%m-%dT%H:%M:%S+00"),format(validtime,"%Y-%m-%dT%H:%M:%S+00"),format(validtime,"%Y-%m-%dT%H:%M:%S+00"), valueparametername, levelparametername, level,level,"\n",sep="\t",file=fastloadFile,append=T)
     }
   }
 }

 # fastload - load into wdb
  if (!dryRun){
    fastloadCommand <- paste("wdb-fastload -d",dbname,"-u",user,"-h",host, "--logfile fastload.log  <", fastloadFile)
    fastloadCommand <- paste("wdb-fastload -d",dbname,"-u",user,"-h",host, "<", fastloadFile)
    cat("system command",fastloadCommand,"\n")
    result <- system(fastloadCommand)
    if (result !=0){
      cat("system command",fastloadCommand,"failed.\n")
      return(FALSE)
    }
    # remove all files no longer needed
    removeFileCommand <- paste("rm ",tableName,".*",sep="")
    system(removeFileCommand)
  }
  return(TRUE)
}


createModelSqlFile <- function(modelName){
  modelOutputFile <- paste(modelName,".out",sep="")
  modelQuery <- createModelQuery(modelName)
  #print(modelOutputFile)
  createSqlFile(modelQuery,modelOutputFile,FALSE)
  return(modelOutputFile)
}

createTableSqlFile <- function(tableName){
  tableOutputFile <- paste(tableName,".out",sep="")
  tableQuery <- createTableQuery(tableName)
  #print(tableOutputFile)
  createSqlFile(tableQuery,tableOutputFile,TRUE)
  return(tableOutputFile)
}


createTableQuery <- function(tableName){
  if (missing(tableName)) 
        stop(" ==>  table name(s)  not specified.")
  tableQuery <- paste("select * from verifop.",tableName,";\n",sep="")
  #print(tableQuery)
  return(tableQuery)
}


createModelQuery <- function(modelName){
  if (missing(modelName)) 
        stop(" ==>  model name(s)  not specified.")
  modelQuery <- "select table_name from all_tables where table_name LIKE 'HIRLAM1\\_%' ESCAPE '\\';\n"
  modelQuery <- paste("select table_name from all_tables where table_name LIKE '",modelName,"\\_%' ESCAPE '\\';\n",sep="")
  #print(modelQuery)
  return(modelQuery)
}
