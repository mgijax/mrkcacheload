#!/bin/csh -fx

#
# Usage:  mrkomim.csh
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

./mrkomim.py >>& $LOG

if ( -z ${MRKCACHEBCPDIR}/MRK_OMIM_Cache.bcp ) then
echo 'BCP File is empty' >>& $LOG
exit 0
endif

# Allow bcp into database and truncate tables

${DBUTILSBINDIR}/turnonbulkcopy.csh ${DBSERVER} ${DBNAME} >>& $LOG
${SCHEMADIR}/table/MRK_OMIM_Cache_truncate.object >>& $LOG

# Drop indexes
${SCHEMADIR}/index/MRK_OMIM_Cache_drop.object >>& $LOG

# BCP new data into tables
cat ${DBPASSWORDFILE} | bcp ${DBNAME}..MRK_OMIM_Cache in ${MRKCACHEBCPDIR}/MRK_OMIM_Cache.bcp -e ${MRKCACHEBCPDIR}/MRK_OMIM_Cache.bcp.error -c -t${FIELDDELIM} -S${DBSERVER} -U${DBUSER} >>& $LOG

# Create indexes
${SCHEMADIR}/index/MRK_OMIM_Cache_create.object >>& $LOG

date >>& $LOG
