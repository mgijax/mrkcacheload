#!/bin/csh -fx

#
# Usage:  mrkprobe.csh
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

./mrkprobe.py | tee -a ${LOG}

if ( -z ${MRKCACHEBCPDIR}/PRB_Marker.bcp ) then
echo 'BCP File is empty' | tee -a ${LOG}
exit 0
endif

# Drop indexes
${MGD_DBSCHEMADIR}/index/PRB_Marker_drop.object | tee -a ${LOG}

# BCP new data into tables
cat ${MGD_DBPASSWORDFILE} | bcp ${MGD_DBNAME}..PRB_Marker in ${MRKCACHEBCPDIR}/PRB_Marker.bcp -e ${MRKCACHEBCPDIR}/PRB_Marker.bcp.error -c -t${FIELDDELIM} -S${MGD_DBSERVER} -U${MGD_DBUSER} | tee -a ${LOG}

# Create indexes
${MGD_DBSCHEMADIR}/index/PRB_Marker_create.object | tee -a ${LOG}

date | tee -a ${LOG}
