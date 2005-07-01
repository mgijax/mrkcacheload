#!/bin/csh -fx

#
# Usage:  mrkomim.csh
#
# History
#
# lec	05/17/2000
#

cd `dirname $0` && source ./Configuration

setenv TABLE MRK_OMIM_Cache
setenv OBJECTKEY 0

setenv LOG	${MRKCACHELOGDIR}/`basename $0`.log
rm -rf $LOG
touch $LOG

date >>& $LOG

# Create the bcp file

./mrkomim.py -S${DBSERVER} -D${DBNAME} -U${DBUSER} -P${DBPASSWORDFILE} -K${OBJECTKEY} >>& $LOG

if ( -z ${MRKCACHEBCPDIR}/${TABLE}.bcp ) then
echo 'BCP File is empty' >>& $LOG
exit 0
endif

# Allow bcp into database and truncate tables

${DBUTILSBINDIR}/turnonbulkcopy.csh ${DBSERVER} ${DBNAME} >>& $LOG
${SCHEMADIR}/table/${TABLE}_truncate.object >>& $LOG

# Drop indexes
${SCHEMADIR}/index/${TABLE}_drop.object >>& $LOG

# BCP new data into tables
cat ${DBPASSWORDFILE} | bcp ${DBNAME}..${TABLE} in ${MRKCACHEBCPDIR}/${TABLE}.bcp -e ${MRKCACHEBCPDIR}/${TABLE}.bcp.error -c -t${FIELDDELIM} -S${DBSERVER} -U${DBUSER} >>& $LOG

# Create indexes
${SCHEMADIR}/index/${TABLE}_create.object >>& $LOG

date >>& $LOG
