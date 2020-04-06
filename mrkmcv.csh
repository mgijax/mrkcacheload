#!/bin/csh -f

#
# Usage mrkmcv.csh
#
# History
#
# sc	05/05/2010 - created
#

cd `dirname $0` && source ./Configuration

setenv TABLE MRK_MCV_Cache
setenv COUNT_TABLE MRK_MCV_Count_Cache
setenv MARKERKEY 0	# used only when called from EI by marker key

setenv CURATORLOG ${MRKCACHELOGDIR}/`basename $0 .csh`.curator.log
setenv TIMESTAMP `date '+%Y%m%d.%H%M'`

# archive old curator log file
if ( -f ${CURATORLOG} ) then
    setenv ARC_FILE "${CURATORLOG}.${TIMESTAMP}"
    mv ${CURATORLOG} ${ARC_FILE}
endif

setenv CACHELOG ${MRKCACHELOGDIR}/`basename $0 .csh`.log
rm -rf ${CACHELOG}
touch ${CACHELOG}


date | tee -a ${CACHELOG}

# Create  bcp file
${PYTHON} ./mrkmcv.py -S${MGD_DBSERVER} -D${MGD_DBNAME} -U${MGD_DBUSER} -P${MGD_DBPASSWORDFILE} -K${MARKERKEY} | tee -a ${CACHELOG}
set resultcode=$?
if ( $resultcode ) then
    exit $resultcode
endif

# Exit if bcp file is empty

if ( -z ${MRKCACHEBCPDIR}/${TABLE}.bcp ) then
echo 'BCP File is empty' | tee -a ${CACHELOG}
exit 0
endif

# truncate table

${SCHEMADIR}/table/${TABLE}_truncate.object | tee -a ${CACHELOG}

# Drop indexes
${SCHEMADIR}/index/${TABLE}_drop.object | tee -a ${CACHELOG}

# BCP new data into tables
${BCP_CMD} ${TABLE} ${MRKCACHEBCPDIR} ${TABLE}.bcp ${COLDELIM} ${LINEDELIM} ${PG_DB_SCHEMA} | tee -a ${CACHELOG}

# Create indexes
${SCHEMADIR}/index/${TABLE}_create.object | tee -a ${CACHELOG}

# Create the MCV Count bcp file
${PYTHON} ./mrkmcvcount.py | tee -a ${CACHELOG}

# Exit if bcp file is empty

if ( -z ${MRKCACHEBCPDIR}/${COUNT_TABLE}.bcp ) then
echo 'BCP File is empty' | tee -a ${CACHELOG}
exit 0
endif

# truncate table

${SCHEMADIR}/table/${COUNT_TABLE}_truncate.object | tee -a ${CACHELOG}

# Drop indexes
${SCHEMADIR}/index/${COUNT_TABLE}_drop.object | tee -a ${CACHELOG}

# BCP new data into tables
${BCP_CMD} ${COUNT_TABLE} ${MRKCACHEBCPDIR} ${COUNT_TABLE}.bcp ${COLDELIM} ${LINEDELIM} ${PG_DB_SCHEMA} | tee -a ${CACHELOG}

# Create indexes
${SCHEMADIR}/index/${COUNT_TABLE}_create.object | tee -a ${CACHELOG}

date | tee -a ${CACHELOG}
