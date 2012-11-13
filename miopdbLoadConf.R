
#define parameters
parameterDefinitions <- read.table("parameters.conf",sep=",",header=TRUE,stringsAsFactors=FALSE)
levelParameterDefinitions <- read.table("levelparameters.conf",sep=",",header=TRUE,stringsAsFactors=FALSE)
# define dataproviders
dataproviderDefinitions<- read.table("dataproviders.conf",sep=",",header=TRUE,stringsAsFactors=FALSE)

#wdb stuff
dbname <- "wdb"
user <- "wdb"
host <- "wdb-dev2"
namespace <- "88,88001,88"

#command to log onto sqlplus and run sql.ctl
sqlpluscommand <- "sqlplus64 -s verif/verif@miopdb @ sql.ctl 2>/dev/null"
#sqlpluscommand <- "/opt/instantclient_11_2/sqlplus -s verif/verif@miopdb @ sql.ctl 2>/dev/null"

# directory for datafiles
datadir <- "/disk1/data/parch/"
