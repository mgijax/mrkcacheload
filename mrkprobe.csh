#!/bin/csh -fx

#
# Usage:  mrkprobe.csh
#
# History
#
# lec	05/17/2000
#

cd `dirname $0` && source ./Configuration

setenv TABLE PRB_Marker

setenv LOG	${MRKCACHELOGDIR}/`basename $0`.log
rm -rf $LOG
touch $LOG

date | tee -a ${LOG}

# Create the bcp file

./mrkprobe.py | tee -a ${LOG}

if ( -z ${MRKCACHEBCPDIR}/${TABLE}.bcp ) then
echo 'BCP File is empty' | tee -a ${LOG}
exit 0
endif

# Drop indexes
${MGD_DBSCHEMADIR}/index/${TABLE}_drop.object | tee -a ${LOG}

# BCP new data into tables
${MGI_DBUTILS}/bin/bcpin.csh ${MGD_DBSERVER} ${MGD_DBNAME} ${TABLE} ${MRKCACHEBCPDIR} ${TABLE}.bcp ${COLDELIM} ${LINEDELIM} | tee -a ${LOG}

# Create indexes
${MGD_DBSCHEMADIR}/index/${TABLE}_create.object | tee -a ${LOG}

date | tee -a ${LOG}
