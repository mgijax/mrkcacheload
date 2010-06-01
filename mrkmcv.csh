#!/bin/csh -fx

#
# Usage mrkmcv.csh
#
# History
#
# sc	05/05/2010 - created
#

cd `dirname $0` && source ./Configuration

setenv TABLE MRK_MCV_Cache
setenv COUNT_TABLE MRK_MCV_Count_Cache
setenv MARKERKEY 0	# used only when called from EI by marker key
setenv LOG	${MRKCACHELOGDIR}/`basename $0 .csh`.log
rm -rf $LOG
touch $LOG

date | tee -a ${LOG}

# Create  bcp file
./mrkmcv.py -S${MGD_DBSERVER} -D${MGD_DBNAME} -U${MGD_DBUSER} -P${MGD_DBPASSWORDFILE} -K${MARKERKEY} | tee -a ${LOG}

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
${MGI_DBUTILS}/bin/bcpin.csh ${MGD_DBSERVER} ${MGD_DBNAME} ${TABLE} ${MRKCACHEBCPDIR} ${TABLE}.bcp ${COLDELIM} ${LINEDELIM} | tee -a ${LOG}

# Create indexes
${MGD_DBSCHEMADIR}/index/${TABLE}_create.object | tee -a ${LOG}

# Create the MCV Count bcp file
./mrkmcvcount.py | tee -a ${LOG}

# Exit if bcp file is empty

if ( -z ${MRKCACHEBCPDIR}/${COUNT_TABLE}.bcp ) then
echo 'BCP File is empty' | tee -a ${LOG}
exit 0
endif

# truncate table

${MGD_DBSCHEMADIR}/table/${COUNT_TABLE}_truncate.object | tee -a ${LOG}

# Drop indexes
${MGD_DBSCHEMADIR}/index/${COUNT_TABLE}_drop.object | tee -a ${LOG}

# BCP new data into tables
${MGI_DBUTILS}/bin/bcpin.csh ${MGD_DBSERVER} ${MGD_DBNAME} ${COUNT_TABLE} ${MRKCACHEBCPDIR} ${COUNT_TABLE}.bcp ${COLDELIM} ${LINEDELIM} | tee -a ${LOG}

# Create indexes
${MGD_DBSCHEMADIR}/index/${COUNT_TABLE}_create.object | tee -a ${LOG}

date | tee -a ${LOG}
