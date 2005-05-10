#!/bin/csh -fx

#
# Usage:  mrkref.csh
#
# History
#
# lec	05/17/2000
#

cd `dirname $0` && source Configuration

setenv LOG	${MRKCACHELOGDIR}/`basename $0`.log
rm -rf $LOG
touch $LOG

date >>& $LOG

cp ${MRKCACHEBCPDIR}/MRK_Reference.bcp ${MRKCACHEBCPDIR}/MRK_Reference.bcp.old

# Create the bcp file

./mrkref.py >>& $LOG

if ( -z ${MRKCACHEBCPDIR}/MRK_Reference.bcp ) then
echo 'BCP File is empty' >>& $LOG
exit 0
endif

# Allow bcp into database and truncate tables

${DBUTILSBINDIR}/turnonbulkcopy.csh ${DBSERVER} ${DBNAME} >>& $LOG
${SCHEMADIR}/table/MRK_Reference_truncate.object >>& $LOG

# Drop indexes
${SCHEMADIR}/index/MRK_Reference_drop.object >>& $LOG

# BCP new data into tables
cat ${DBPASSWORDFILE} | bcp ${DBNAME}..MRK_Reference in ${MRKCACHEBCPDIR}/MRK_Reference.bcp -e ${MRKCACHEBCPDIR}/MRK_Reference.bcp.error -c -t${FIELDDELIM} -S${DBSERVER} -U${DBUSER} >>& $LOG

# Create indexes
${SCHEMADIR}/index/MRK_Reference_create.object >>& $LOG

date >>& $LOG
