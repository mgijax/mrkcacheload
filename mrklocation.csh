#!/bin/csh -f

#
# Usage:  mrklocation.csh
#
# History
#
# lec	04/27/2005
#

cd `dirname $0` && source ./Configuration

setenv TABLE MRK_Location_Cache

setenv LOG	${MRKCACHELOGDIR}/`basename $0 .csh`.log
rm -rf $LOG
touch $LOG

date | tee -a ${LOG}

# Create the bcp file
./mrklocation.py | tee -a ${LOG}

# Exit if bcp file is empty

if ( -z ${MRKCACHEBCPDIR}/${TABLE}.bcp ) then
echo 'BCP File is empty' | tee -a ${LOG}
exit 0
endif

# truncate table
${SCHEMADIR}/table/${TABLE}_truncate.object | tee -a ${LOG}

# Drop indexes
${SCHEMADIR}/index/${TABLE}_drop.object | tee -a ${LOG}

# BCP new data into tables
${BCP_CMD} ${TABLE} ${MRKCACHEBCPDIR} ${TABLE}.bcp ${COLDELIM} ${LINEDELIM} ${PG_DB_SCHEMA} | tee -a ${LOG}

# Create indexes
${SCHEMADIR}/index/${TABLE}_create.object | tee -a ${LOG}

date | tee -a ${LOG}
