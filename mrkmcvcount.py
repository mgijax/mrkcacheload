#!/usr/local/bin/python

'''
#
# Purpose: 
#
# Create bcp file for MRK_MCV_Cache, a cache table of the count of markers
# annotated to each marker category term and its descendants
#
# Usage: mrkmcvcount.py
#
# Processing:
#
# History
#
# sc	05/05/2010 - created
#	TR6839; Marker Types
#
'''

import sys
import os
import string
import sets
import mgi_utils

try:
	if os.environ['DB_TYPE'] == 'postgres':
		import pg_db
       		db = pg_db
       		db.setTrace()
       		db.setAutoTranslateBE()
	else:
     		import db
       		db.set_sqlLogFunction(db.sqlLogAll)

except:
    import db
    db.set_sqlLogFunction(db.sqlLogAll)

try:
	table = os.environ['COUNT_TABLE']
	outDir = os.environ['MRKCACHEBCPDIR']
except:
	table = 'MRK_MCV_Count_Cache'
	outDir = './'

COLDL = '|'
LINEDL = '\n'
createdBy = os.environ['CREATEDBY']
countBCP = '%s/%s.bcp' % (outDir, table)
date = mgi_utils.date("%m/%d/%Y")

def createBCPfile():
    '''
    #
    # MRK_MCV_Count_Cache is a cache table of the count of markers
    # annotated to each marker category term and its descendants
    #
    '''
    print 'Creating %s ...' % countBCP
    countFp = open(countBCP, 'w')
    db.sql('''select distinct _MCVTerm_key, count(_MCVTerm_key) as mkrCt
	into #mkrCt
	from MRK_MCV_Cache
	group by _MCVTerm_key''', None)
    # must be distinct or dups will be returned where there are both
    # SO and MCV ids associated with a term
    results = db.sql('''select distinct v.term, m.*
	from VOC_Term v, ACC_Accession a, #mkrCt m
	where m._MCVTerm_key = v._Term_key
	and v._Term_key = a._Object_key
	and a._MGIType_key = 13
	and a.preferred = 1''', 'auto')
    for r in results:
        termKey = r['_MCVTerm_key']
	markerCt = r['mkrCt']
	countFp.write(mgi_utils.prvalue(termKey) + COLDL + \
	    mgi_utils.prvalue(markerCt) + COLDL + \
	    createdBy + COLDL + \
	    createdBy + COLDL + \
	    date + COLDL + \
	    date + LINEDL)
    countFp.close()
#
# Main Routine
#

db.useOneConnection(1)
db.set_sqlLogFunction(db.sqlLogAll)
createBCPfile()
db.useOneConnection(0)

print '%s' % mgi_utils.date()

