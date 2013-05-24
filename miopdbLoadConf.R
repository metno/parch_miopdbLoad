#define parameters
parameterDefinitions <- read.table("parameters.conf",sep=",",header=TRUE,stringsAsFactors=FALSE)
levelParameterDefinitions <- read.table("levelparameters.conf",sep=",",header=TRUE,stringsAsFactors=FALSE)
# define dataproviders
dataproviderDefinitions<- read.table("dataproviders.conf",sep=",",header=TRUE,stringsAsFactors=FALSE)
# stations to export (with synop number, fromtime=earliest time to export data from, geom i s geometric representation in database, use as name
exportFile <- "exportStations.txt"
#if use useGeom==TRUE use geom as name in fastload files, otherwise use synop number
useGeom <- FALSE

#wdb stuff
dbname <- "wdb"
user <- "wdb"
host <- "wdb-dev3"
namespace <- "88,42,88"


#command to log onto sqlplus and run sql.ctl
sqlpluscommand <- "sqlplus64 -s verif/verif@miopdb @ sql.ctl 2>/dev/null"
#sqlpluscommand <- "/opt/instantclient_11_2/sqlplus -s verif/verif@miopdb @ sql.ctl 2>/dev/null"
# which data to be exported
miopdbwherestring <- " where aar> 2000 and aar < 2002"

# directory for outfiles
outdir <- "/disk1/data/parch/out/2012/"
# directory for loadfiles
loaddir <- "/disk1/data/parch/load/2012/"
