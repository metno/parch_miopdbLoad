namespace <- "88,456,88"

#define parameters
parameterDefinitions <- read.table("parameters.conf",sep=",",header=TRUE,stringsAsFactors=FALSE)
levelParameterDefinitions <- read.table("levelparameters.conf",sep=",",header=TRUE,stringsAsFactors=FALSE)
# define dataproviders
dataproviderDefinitions<- read.table("dataproviders.conf",sep=",",header=TRUE,stringsAsFactors=FALSE)

#wdb stuff
dbname <- "wdb"
user <- "wdb"
host <- "wdb-dev3"

#command to log onto sqlplus and run sql.ctl
#sqlpluscommand <- "sqlplus -s verif/verif@miopdb @ sql.ctl 2>/dev/null"
sqlpluscommand <- "/opt/instantclient_11_2/sqlplus -s verif/verif@miopdb @ sql.ctl 2>/dev/null"
