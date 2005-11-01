#!/bin/csh -fx

#
# Usage:  mrkref.csh
#
# History
#
# lec	05/17/2000
#

cd `dirname $0` && source ./Configuration

setenv LOG	${MRKCACHELOGDIR}/`basename $0`.log
rm -rf $LOG
touch $LOG

date | tee -a ${LOG}

# Create the bcp file

./mrkref.py | tee -a ${LOG}

if ( -z ${MRKCACHEBCPDIR}/MRK_Reference.bcp ) then
echo 'BCP File is empty' | tee -a ${LOG}
exit 0
endif

# Allow bcp into database and truncate tables

${DBUTILSBINDIR}/turnonbulkcopy.csh ${DBSERVER} ${DBNAME} | tee -a ${LOG}
${SCHEMADIR}/table/MRK_Reference_truncate.object | tee -a ${LOG}

# Drop indexes
${SCHEMADIR}/index/MRK_Reference_drop.object | tee -a ${LOG}

# BCP new data into tables
cat ${DBPASSWORDFILE} | bcp ${DBNAME}..MRK_Reference in ${MRKCACHEBCPDIR}/MRK_Reference.bcp -e ${MRKCACHEBCPDIR}/MRK_Reference.bcp.error -c -t${FIELDDELIM} -S${DBSERVER} -U${DBUSER} | tee -a ${LOG}

# Create indexes
${SCHEMADIR}/index/MRK_Reference_create.object | tee -a ${LOG}

date | tee -a ${LOG}
