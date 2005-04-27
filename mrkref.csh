#!/bin/csh -fx

#
# Usage:  mrkref.csh
#
# History
#
# lec	05/17/2000
#

cd `dirname $0` && source Configuration

setenv DSQUERY	$DBSERVER
setenv MGD	$DBNAME

setenv LOG	${MRKREFLOGDIR}/`basename $0`.log
rm -rf $LOG
touch $LOG

date >>& $LOG

cp ${MRKREFBCPDIR}/MRK_Reference.bcp ${MRKREFBCPDIR}/MRK_Reference.bcp.old

# Create the bcp file

./mrkref.py >>& $LOG

if ( -z ${MRKREFBCPDIR}/MRK_Reference.bcp ) then
echo 'BCP File is empty' >>& $LOG
exit 0
endif

# Allow bcp into database and truncate tables

${DBUTILSBINDIR}/turnonbulkcopy.csh ${DBSERVER} ${DBNAME} >>& $LOG
${SCHEMADIR}/table/MRK_Reference_truncate.object >>& $LOG

# Drop indexes
${SCHEMADIR}/index/MRK_Reference_drop.object >>& $LOG

# BCP new data into tables
cat ${DBPASSWORDFILE} | bcp ${DBNAME}..MRK_Reference in ${MRKREFBCPDIR}/MRK_Reference.bcp -c -t\| -S${DBSERVER} -U${DBUSER} >>& $LOG

# Create indexes
${SCHEMADIR}/index/MRK_Reference_create.object >>& $LOG

date >>& $LOG
