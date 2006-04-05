#!/bin/csh -fx

#
# Usage:  mrkhomology.csh
#
# History
#
# lec	04/27/2005
#

cd `dirname $0` && source ./Configuration

setenv TABLE MRK_Homology_Cache
setenv OBJECTKEY 0

setenv LOG	${MRKCACHELOGDIR}/`basename $0`.log
rm -rf $LOG
touch $LOG

# Create the bcp file

date | tee -a ${LOG}
./mrkhomology.py -S${MGD_DBSERVER} -D${MGD_DBNAME} -U${MGD_DBUSER} -P${MGD_DBPASSWORDFILE} -K${OBJECTKEY} | tee -a ${LOG}
date | tee -a ${LOG}

# Exit if bcp file is empty

if ( -z ${MRKCACHEBCPDIR}/${TABLE}.bcp ) then
echo 'BCP File is empty' | tee -a ${LOG}
exit 0
endif

# truncate table

${MGD_DBSCHEMADIR}/table/${TABLE}_truncate.object | tee -a ${LOG}

# Drop indexes
${MGD_DBSCHEMADIR}/index/${TABLE}_drop.object | tee -a ${LOG}

# BCP new data into tables
cat ${MGD_DBPASSWORDFILE} | bcp ${MGD_DBNAME}..${TABLE} in ${MRKCACHEBCPDIR}/${TABLE}.bcp -e ${MRKCACHEBCPDIR}/${TABLE}.bcp.error -c -t${FIELDDELIM} -S${MGD_DBSERVER} -U${MGD_DBUSER} | tee -a ${LOG}

# Create indexes
${MGD_DBSCHEMADIR}/index/${TABLE}_create.object | tee -a ${LOG}

date | tee -a ${LOG}
