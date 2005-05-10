#!/bin/csh -fx

#
# Usage:  mrklabel.csh
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

./mrklabel.py | tee -a ${LOG}

# Exit if bcp file is empty

if ( -z ${MRKCACHEBCPDIR}/MRK_Label.bcp ) then
echo 'BCP File is empty' >>& $LOG
exit 0
endif

# Allow bcp into database and truncate tables

${DBUTILSBINDIR}/turnonbulkcopy.csh ${DBSERVER} ${DBNAME} | tee -a ${LOG}
${SCHEMADIR}/table/MRK_Label_truncate.object | tee -a ${LOG}

# Drop indexes
${SCHEMADIR}/index/MRK_Label_drop.object | tee -a ${LOG}

# BCP new data into tables
cat ${DBPASSWORDFILE} | bcp ${DBNAME}..MRK_Label in ${MRKCACHEBCPDIR}/MRK_Label.bcp -e ${MRKCACHEBCPDIR}/MRK_Label.bcp.error -c -t${FIELDDELIM} -S${DBSERVER} -U${DBUSER} | tee -a ${LOG}

# Create indexes
${SCHEMADIR}/index/MRK_Label_create.object | tee -a ${LOG}

date | tee -a ${LOG}
