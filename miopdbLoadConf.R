namespace <- "88,456,88"

# define dataproviders
models<-c("UM4KM1","HIRLAM8KM1")
dataproviders <- c("um4","h8")
names(dataproviders)<-models

#define parameters
parameterDefinitions <- read.table("parameters.conf",sep=",",header=TRUE,stringsAsFactors=FALSE)
dataproviderDefinitions<- read.table("dataproviders.conf",sep=",",header=TRUE,stringsAsFactors=FALSE)

#wdb stuff
dbname <- "wdb"
user <- "wdb"
host <- "wdb-dev3"

#command to log onto sqlplus and run sql.ctl
#sqlpluscommand <- "sqlplus -s verif/verif@miopdb @ sql.ctl 2>/dev/null"
sqlpluscommand <- "/opt/instantclient_11_2/sqlplus -s verif/verif@miopdb @ sql.ctl 2>/dev/null"
