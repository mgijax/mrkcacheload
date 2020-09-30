#!/bin/csh -f

#
# Usage:  mrkprobe.csh
#
# History
#
# lec	05/17/2000
#

cd `dirname $0` && source ./Configuration

setenv TABLE PRB_Marker

setenv LOG	${MRKCACHELOGDIR}/`basename $0 .csh`.log
rm -rf $LOG
touch $LOG

date | tee -a ${LOG}

# Create the bcp file

${PYTHON} ./mrkprobe.py | tee -a ${LOG}

if ( -z ${MRKCACHEBCPDIR}/${TABLE}.bcp ) then
echo 'BCP File is empty' | tee -a ${LOG}
exit 0
endif

# Drop indexes
${SCHEMADIR}/index/${TABLE}_drop.object | tee -a ${LOG}

# BCP new data into tables
${BCP_CMD} ${TABLE} ${MRKCACHEBCPDIR} ${TABLE}.bcp ${COLDELIM} ${LINEDELIM} ${PG_DB_SCHEMA} | tee -a ${LOG}

# Create indexes
${SCHEMADIR}/index/${TABLE}_create.object | tee -a ${LOG}
${SCHEMADIR}/autosequence/${TABLE}_create.object | tee -a ${LOG}

date | tee -a ${LOG}
