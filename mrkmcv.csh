#!/bin/csh -f

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

setenv CURATORLOG ${MRKCACHELOGDIR}/`basename $0 .csh`.curator.log
echo "CURATORLOG: ${CURATORLOG}"
setenv TIMESTAMP `date '+%Y%m%d.%H%M'`

# archive old curator log file
if ( -f ${CURATORLOG} ) then
    setenv ARC_FILE "${CURATORLOG}.${TIMESTAMP}"
    echo "CURATORLOG ${CURATORLOG}"
    echo "ARC_FILE ${ARC_FILE}"
    mv ${CURATORLOG} ${ARC_FILE}
endif

setenv CACHELOG ${MRKCACHELOGDIR}/`basename $0 .csh`.log
rm -rf ${CACHELOG}
touch ${CACHELOG}


date | tee -a ${CACHELOG}

# Create  bcp file
./mrkmcv.py -S${MGD_DBSERVER} -D${MGD_DBNAME} -U${MGD_DBUSER} -P${MGD_DBPASSWORDFILE} -K${MARKERKEY} | tee -a ${CACHELOG}

# Exit if bcp file is empty

if ( -z ${MRKCACHEBCPDIR}/${TABLE}.bcp ) then
echo 'BCP File is empty' | tee -a ${CACHELOG}
exit 0
endif

# truncate table

${MGD_DBSCHEMADIR}/table/${TABLE}_truncate.object | tee -a ${CACHELOG}

# Drop indexes
${MGD_DBSCHEMADIR}/index/${TABLE}_drop.object | tee -a ${CACHELOG}

# BCP new data into tables
${MGI_DBUTILS}/bin/bcpin.csh ${MGD_DBSERVER} ${MGD_DBNAME} ${TABLE} ${MRKCACHEBCPDIR} ${TABLE}.bcp ${COLDELIM} ${LINEDELIM} | tee -a ${CACHELOG}

# Create indexes
${MGD_DBSCHEMADIR}/index/${TABLE}_create.object | tee -a ${CACHELOG}

# Create the MCV Count bcp file
./mrkmcvcount.py | tee -a ${CACHELOG}

# Exit if bcp file is empty

if ( -z ${MRKCACHEBCPDIR}/${COUNT_TABLE}.bcp ) then
echo 'BCP File is empty' | tee -a ${CACHELOG}
exit 0
endif

# truncate table

${MGD_DBSCHEMADIR}/table/${COUNT_TABLE}_truncate.object | tee -a ${CACHELOG}

# Drop indexes
${MGD_DBSCHEMADIR}/index/${COUNT_TABLE}_drop.object | tee -a ${CACHELOG}

# BCP new data into tables
${MGI_DBUTILS}/bin/bcpin.csh ${MGD_DBSERVER} ${MGD_DBNAME} ${COUNT_TABLE} ${MRKCACHEBCPDIR} ${COUNT_TABLE}.bcp ${COLDELIM} ${LINEDELIM} | tee -a ${CACHELOG}

# Create indexes
${MGD_DBSCHEMADIR}/index/${COUNT_TABLE}_create.object | tee -a ${CACHELOG}

date | tee -a ${CACHELOG}
