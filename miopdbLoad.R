library(udunits2)
source("miopdbLoadConf.R")
source("createSqlFile.R")

readExportStations <- function(filename){
  exportStations<<-read.table(filename, sep=",",head=TRUE,fill=TRUE,stringsAsFactors=FALSE,strip.white=TRUE)
}


miopdbModelOut <- function(modelName){
 # produce .out files (from miopdb)
  readExportStations(exportFile)
 # list all tables for this model(one table for each station)
  modelOutputFile <- createModelSqlFile(modelName)
  system(sqlpluscommand)
  # Read list of all the table names
  tablenameList<-scan(modelOutputFile,what=list(tablename=character()))
 # vector with table names
 tablenameVector<-tablenameList$tablename

  for (i in 1:length(tablenameVector)){
    if (miopdbTableOut(tablenameVector[i])){
     cat(tablenameVector[i], "produced .out file\n")
   } else {
     cat("error,",tablenameVector[i], "no .out\n")
   }
    
  }
ll 
  
  removeFileCommand <- paste("rm ",modelOutputFile,sep="")
  system(removeFileCommand)
  system("rm sql.ctl")
}



miopdbTableOut <- function(tableName){
  cat("miopdbTableOut",tableName,"\n")
  tableElements <- unlist(strsplit(tableName,"_"))
  lengthTableElements <- length(tableElements)
  # assume stationid is everything after the last underscore
  stationid <- tableElements[lengthTableElements]
  if (is.na(as.numeric(stationid))){
    cat("Non numeric stationid in table ", tableName,"\n")
    return(FALSE)
  }
  exportStation <- exportStations[exportStations$synop==stationid,]
  if (nrow(exportStation)==0){
   cat("this station not to be exported\n")
   return(FALSE)
 }
  tableOutputFile <- createTableSqlFile(tableName)
 # create an sql file, sql.ctl for downloading table to file tableOutputFile
  # log onto miopdb and execute the command
  cat (sqlpluscommand,"\n")
  result <- system(sqlpluscommand)
  if (result !=0){
    cat("system command",sqlpluscommand,"failed\n")
    return(FALSE)
  }
  return(TRUE)
}




miopdbModelLoad <- function(modelName,dryRun=TRUE){
  readExportStations(exportFile)
 # list all tables for this model(one table for each station)
  modelOutputFile <- paste(loaddir,modelName,"/",modelName,".out",sep="")
  listmodelfiles <- paste("ls ",outdir,modelName,"/",modelName,"_*.out  >", modelOutputFile,sep="")
  print(modelOutputFile)
  print(listmodelfiles)
  result <- system(listmodelfiles)
  if (result !=0){
    cat("system command",listmodelfiles,"failed\n")
    return(FALSE)
  }
 # Read list of all the table names
 tablefileList<-scan(modelOutputFile,what=list(tablefile=character()))
 # vector with table names
 tablefileVector<-tablefileList$tablefile
  for (i in 1:length(tablefileVector)){
   if (miopdbTableLoad(tablefileVector[i],dryRun)){
      if (dryRun)
        cat("dry run,", tablefileVector[i], "not loaded\n")
      else
        cat(tablefileVector[i], "loaded\n")
    }
    else{
      cat("error,",tablefileVector[i], "not loaded\n")
    }
  }
  removeFileCommand <- paste("rm ",modelOutputFile,sep="")
  system(removeFileCommand)
}



tableNameFromTableFilename <-function(tableFilename){
  #print(tableFilename)
  filename <- basename(tableFilename)
  #print(filename)
  # filename is of the form HIRLAM1_1003.out
  #split filename at .
  filenameElements <- unlist(strsplit(filename,"\\."))
  if (length(filenameElements)>0){
    #print(filenameElements[1]) 
    return (filenameElements[1])
  }
  return("")
}


miopdbTableLoad <- function(tableFilename,dryRun=TRUE){
  tableName <-  tableNameFromTableFilename(tableFilename)
  print(tableName)
  #now we have name of the form HIRLAM1_1003 (extract stationid from this)
  tableElements <- unlist(strsplit(tableName,"_"))
  lengthTableElements <- length(tableElements)

  # assume modelname is everything before the last underscore
  tablemodelName <- paste(tableElements[-lengthTableElements],collapse="_")
  # assume stationid is everything after the last underscore
  stationid <- tableElements[lengthTableElements]
  print(stationid)
  if (is.na(as.numeric(stationid))){
    cat("Non numeric stationid in table ", tableName,"\n")
    return(FALSE)
  }


  exportStation <- exportStations[exportStations$synop==stationid,]
  if (nrow(exportStation)==0){
   cat("this station not to be exported\n")
   return(FALSE)
 }
  geom <- tolower(exportStation$geom)
  fromtime <- strptime(exportStation$fromtime,"%Y-%m-%d")
  station.fromtime <- format(fromtime,"%Y-%m-%dT%H:%M:%S+00")
  # find data provider and stationid from 

  
  dataprovider<-dataproviderDefinitions[dataproviderDefinitions$tablemodelname==tablemodelName,]$dataprovider
  if (length(dataprovider)==0){
      cat("Data provider not found for ", tablemodelname,"\n")
      return(FALSE)
    }
  cat("The dataprovider is", dataprovider,"\n")
  cat("The station is", stationid,"\n")
  print(geom)
  print(station.fromtime)

  #split headers and data
  headerFile<- paste(tableName,".head",sep="")
  dataFile <- paste(tableName,".dat",sep="")
  # sqlplus problem, we get several headers in the output file
  grepheaders <- paste("grep \"AAR\"",tableFilename," >", headerFile)
  result <- system(grepheaders)
  if (result !=0){
    cat("system command",grepheaders,"failed\n")
    return(FALSE)
  }
  grepnoheaders <- paste("grep -v \"AAR\"",tableFilename," >", dataFile)
  result <- system(grepnoheaders)
  if (result !=0){
    cat("system command",grepnoheaders,"failed\n")
    return(FALSE)
  }
  #sort datafile to get rid of duplicate lines
  sortcommand <- paste("sort -u -o",dataFile,dataFile)
  cat(sortcommand,"\n")
  result <- system(sortcommand)
  if (result !=0){
    cat("system command",sortcommand,"failed\n")
    return(FALSE)
  }
 # write to file for fastload, names tableName.load
  fastloadFile <- paste(loaddir,tablemodelName,"/",tableName,".load",sep="")
  cat(paste(dataprovider,namespace,"\n",sep="\t"), file=fastloadFile)

  # todo deal with NA values, not factor
  headers<-read.table(headerFile,sep="",header=TRUE)
  df<-read.table(dataFile,sep="",header=FALSE)
  names(df)<-names(headers)
  if (!identical(names(df)[1:5],c("AAR","MND","DAG","TIM","PROG"))){
    cat("Wrong headers in sql file")
    return(FALSE)
  }

  # first column with data or (NIVA)
  firstDataColumn <- 6

  useGroundLevel <- TRUE
  # check if level (ie NIVA) given
  # if level is given it is in the first Data column
  levelpar <- names(df)[firstDataColumn]
  pdef <- levelParameterDefinitions[levelParameterDefinitions$miopdb_par==levelpar,]
  if (nrow(pdef)!=0) {
    cat("Level is given\n")
    firstDataColumn <- firstDataColumn+1
    useGroundLevel <- FALSE
    levelparametername <- as.character(pdef$levelparametername)
    miopdb_levelunit<- as.character(pdef$miopdb_unit)
    levelunit<- as.character(pdef$unit)
  }

  
  nrows <- nrow(df)
  validtimes <- c(NA)
  length(validtimes) <- nrows
  reftimes <- c(NA)
  length(reftimes) <- nrows
  levels <- c(NA)
  length(levels) <- nrows
  
  for (i in 1:nrows){
    row <- df[i,]
    aar <- row$AAR
    mnd <- row$MND
    dag <- row$DAG
    tim <- row$TIM
    prog <- row$PROG
    validtime <- ISOdatetime(aar,mnd,dag,tim,0,0,"GMT")
    if (is.na(validtime)){
      cat("Error, in validtime ",paste(unlist(row),collapse=" "), "row", i,"\n")
      return(FALSE)
    }
    timediff <- as.difftime(prog,units="hours")
    referencetime <- validtime-timediff

    validtimes[i] <- format(validtime,"%Y-%m-%dT%H:%M:%S+00")
    reftimes[i] <-format(referencetime,"%Y-%m-%dT%H:%M:%S+00")

    if (!useGroundLevel){
      levels[i] <- as.character(row[levelpar])
    }
    
  }

  # convert levels
  if(!useGroundLevel){
    if (!is.na(levelunit)&&!miopdb_levelunit==levelunit)
    levels <- ud.convert(levels,miopdb_levelunit,levelunit)
  }

  for (j in firstDataColumn:ncol(df)){
     # get parameter name and defintions
    col <- df[,j]
    par <- names(df)[j]
    #print(par)
    pdef <- parameterDefinitions[parameterDefinitions$miopdb_par==par,]
    # exit loop if unknown parameter
    if (nrow(pdef)!=0) {
      valueparametername <- as.character(pdef$valueparametername)
      if (useGroundLevel){
        # use default level for this parameter
        levelparametername <- as.character(pdef$groundlevelparametername)
        level <- as.character(pdef$groundlevel)
      } 
                    
      miopdb_unit<- as.character(pdef$miopdb_unit)
      unit<- as.character(pdef$unit)
   
      if (!is.na(unit)&&!miopdb_unit==unit)
        col <- ud.convert(col,miopdb_unit,unit)
      for (i in 1:nrows){      
        if (!useGroundLevel){
          level <- levels[i]
        }
        value <- as.character(col[i])
        if (useGeom)
          placename <-geom
        else
          placename <-stationid
        # here we check that the reftime is later than the earliest time to record data
	if (!is.na(value) && reftimes[i]>station.fromtime)	
          cat(value, placename,reftimes[i],validtimes[i],validtimes[i], valueparametername, levelparametername, level,level,"\n",sep="\t",file=fastloadFile,append=T)
      }
    }
  }


 # fastload - load into wdb
  if (!dryRun){
    fastloadCommand <- paste("wdb-fastload -d",dbname,"-u",user,"-h",host, "<", fastloadFile)
    cat("system command",fastloadCommand,"\n")
    result <- system(fastloadCommand)
    if (result !=0){
      cat("system command",fastloadCommand,"failed.\n")
      return(FALSE)
    }

  }
    # remove all files no longer needed
    removeFileCommand <- paste("rm ",tableName,".*",sep="")
    system(removeFileCommand)
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
  tableOutputFile <- paste(outdir,tableName,".out",sep="")
  tableQuery <- createTableQuery(tableName)
  #print(tableOutputFile)
  createSqlFile(tableQuery,tableOutputFile,TRUE)
  return(tableOutputFile)
}


createTableQuery <- function(tableName){
  if (missing(tableName)) 
        stop(" ==>  table name(s)  not specified.")
  tableQuery <- paste("select * from verifop.",tableName," ",miopdbwherestring,";\n",sep="")
  print(tableQuery)
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


miopdbFastload <- function(modelName){
  modelLoadListFile <- paste(loaddir,modelName,".list",sep="")
  listloadfiles <- paste("ls ",loaddir, modelName,"/",modelName,"_*.load  >", modelLoadListFile,sep="")
  print(modelLoadListFile)
  print(listloadfiles)
  result <- system(listloadfiles)
  if (result !=0){
    cat("system command",listloadfiles,"failed\n")
    return(FALSE)
  }
 # Read list of all the table names
 tablefileList<-scan(modelLoadListFile,what=list(tablefile=character()))
 # vector with table names
 tablefileVector<-tablefileList$tablefile
  for (i in 1:length(tablefileVector)){
    fastloadFile <- tablefileVector[i]
    print(fastloadFile)
    fastloadCommand <- paste("wdb-fastload -d",dbname,"-u",user,"-h",host, " --only-groups <", fastloadFile)
    cat("system command",fastloadCommand,"\n")
    result <- system(fastloadCommand)
    if (result !=0){
      cat("system command",fastloadCommand,"failed.\n")
    }
    fastloadCommand <- paste("wdb-fastload -d",dbname,"-u",user,"-h",host, "<", fastloadFile)
    cat("system command",fastloadCommand,"\n")
    result <- system(fastloadCommand)
    if (result !=0){
      cat("system command",fastloadCommand,"failed.\n")
    }


  }
  

}
