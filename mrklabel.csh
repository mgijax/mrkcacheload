#!/bin/csh -fx

#
# Usage:  mrklabel.csh
#
# History
#
# lec	05/17/2000
#

cd `dirname $0` && source ./Configuration

setenv TABLE MRK_Label

setenv LOG	${MRKCACHELOGDIR}/`basename $0`.log
rm -rf $LOG
touch $LOG

date | tee -a ${LOG}

# Create the bcp file

./mrklabel.py | tee -a ${LOG}

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
