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

date >>& $LOG

# Create the bcp file

./mrkprobe.py >>& $LOG

if ( -z ${MRKCACHEBCPDIR}/PRB_Marker.bcp ) then
echo 'BCP File is empty' >>& $LOG
exit 0
endif

# Allow bcp into database

${DBUTILSBINDIR}/turnonbulkcopy.csh ${DBSERVER} ${DBNAME} >>& $LOG

# Drop indexes
${SCHEMADIR}/index/PRB_Marker_drop.object >>& $LOG

# BCP new data into tables
cat ${DBPASSWORDFILE} | bcp ${DBNAME}..PRB_Marker in ${MRKCACHEBCPDIR}/PRB_Marker.bcp -e ${MRKCACHEBCPDIR}/PRB_Marker.bcp.error -c -t${FIELDDELIM} -S${DBSERVER} -U${DBUSER} >>& $LOG

# Create indexes
${SCHEMADIR}/index/PRB_Marker_create.object >>& $LOG

date >>& $LOG
