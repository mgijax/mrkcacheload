#!/bin/csh -f

echo "Running all mrk cache loads"
foreach load ( mrklocation.csh mrklabel.csh mrkref.csh mrkmcv.csh mrkprobe.csh mrkomim.csh )
    setenv DB_TYPE sybase
    ./$load
    setenv DB_TYPE postgres
    ./$load
end

echo "Performing test"
# mrk_label has more records in postgres due to distinct on case (this is ok)
python ${MGD_DBUTILS}/bin/comparePostgresTable.py -c mrk_location_cache mrk_mcv_cache mrk_mcv_count_cache mrk_reference prb_marker 

echo "Tests successful"
