#!/bin/csh -fx

#
# Usage:  mrklocation.csh
#
# History
#
# lec	04/27/2005
#

cd `dirname $0` && source ./Configuration

setenv LOG	${MRKCACHELOGDIR}/`basename $0`.log
rm -rf $LOG
touch $LOG

date | tee -a ${LOG}

# Create the bcp file

./mrklocation.py | tee -a ${LOG}

# Exit if bcp file is empty

if ( -z ${MRKCACHEBCPDIR}/MRK_Location_Cache.bcp ) then
echo 'BCP File is empty' | tee -a ${LOG}
exit 0
endif

# truncate table

${MGD_DBSCHEMADIR}/table/MRK_Location_Cache_truncate.object | tee -a ${LOG}

# Drop indexes
${MGD_DBSCHEMADIR}/index/MRK_Location_Cache_drop.object | tee -a ${LOG}

# BCP new data into tables
cat ${MGD_DBPASSWORDFILE} | bcp ${MGD_DBNAME}..MRK_Location_Cache in ${MRKCACHEBCPDIR}/MRK_Location_Cache.bcp -e ${MRKCACHEBCPDIR}/MRK_Location_Cache.bcp.error -c -t${FIELDDELIM} -S${MGD_DBSERVER} -U${MGD_DBUSER} | tee -a ${LOG}

# Create indexes
${MGD_DBSCHEMADIR}/index/MRK_Location_Cache_create.object | tee -a ${LOG}

date | tee -a ${LOG}
