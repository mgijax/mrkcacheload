#!/usr/local/bin/python

'''
#
# Purpose:
#
# Create bcp file for MRK_Location_Cache
#
# Uses environment variables to determine Server and Database
# (DSQUERY and MGD).
#
# Usage:
#	mrklocation.py [markerkey]
#
# If markerkey is provided, then only create the bcp file for that marker.
#
# Processing:
#
# History
#
'''

import sys
import os
import db
import mgi_utils

NL = '\n'
DL = '|'

cdate = mgi_utils.date("%m/%d/%Y")
createdBy = '1000'

outDir = os.environ['MRKCACHEBCPDIR']

def createBCPfile(markerKey):
	'''
	#
	# MRK_Location_Cache is a cache table of marker location data
	#
	'''

	print 'Creating MRK_Location_Cache.bcp...'

	locBCP = open(outDir + '/MRK_Location_Cache.bcp', 'w')

	db.sql('select m._Marker_key, m.chromosome, m.cytogeneticOffset, o.offset, c.sequenceNum ' + \
		'into #markers ' + \
		'from MRK_Marker m, MRK_Offset o, MRK_Chromosome c ' + \
		'where m._Marker_Status_key in (1,3) ' + \
		'and m._Organism_key = 1 ' + \
		'and m._Marker_key = o._Marker_key ' + \
		'and o.source = 0 ' + \
		'and m._Organism_key = c._Organism_key ' + \
		'and m.chromosome = c.chromosome ', None)

	db.sql('create index idx1 on #markers(_Marker_key)', None)

	results = db.sql('select m._Marker_key, c.startCoordinate, c.endCoordinate, c.strand, c.mapUnits, c.provider, c.version ' + \
		'from #markers m, SEQ_Marker_Cache mc, SEQ_Coord_Cache c ' + \
		'where m._Marker_key = mc._Marker_key ' + \
		'and mc._Qualifier_key = 615419 ' + \
		'and mc._Sequence_key = c._Sequence_key', 'auto')
	coord = {}
	for r in results:
	    key = r['_Marker_key']
	    value = r

	    if not coord.has_key(key):
		coord[key] = []
            coord[key].append(r)

	results = db.sql('select * from #markers order by _Marker_key', 'auto')
	for r in results:

	    key = r['_Marker_key']

	    # print one record out per coordinate

	    if coord.has_key(key):
		for c in coord[key]:
	            locBCP.write(mgi_utils.prvalue(r['_Marker_key']) + DL + \
			        r['chromosome'] + DL + \
			        mgi_utils.prvalue(r['sequenceNum']) + DL + \
			        mgi_utils.prvalue(r['cytogeneticOffset']) + DL + \
			        mgi_utils.prvalue(r['offset']) + DL + \
			        mgi_utils.prvalue(c['startCoordinate']) + DL + \
			        mgi_utils.prvalue(c['endCoordinate']) + DL + \
			        mgi_utils.prvalue(c['strand']) + DL + \
			        mgi_utils.prvalue(c['mapUnits']) + DL + \
			        mgi_utils.prvalue(c['provider']) + DL + \
			        mgi_utils.prvalue(c['version']) + DL + \
			        createdBy + DL + \
			        createdBy + DL + \
			        cdate + DL + \
			        cdate + NL)
	    else:
	        locBCP.write(mgi_utils.prvalue(r['_Marker_key']) + DL + \
			     r['chromosome'] + DL + \
			     mgi_utils.prvalue(r['sequenceNum']) + DL + \
			     mgi_utils.prvalue(r['cytogeneticOffset']) + DL + \
			     mgi_utils.prvalue(r['offset']) + DL + \
			     DL + \
			     DL + \
			     DL + \
			     DL + \
			     DL + \
			     DL + \
			     createdBy + DL + \
			     createdBy + DL + \
			     cdate + DL + \
			     cdate + NL)
	    locBCP.flush()

	locBCP.close()

#
# Main Routine
#

if len(sys.argv) == 2:
	markerKey = sys.argv[1]
else:
	markerKey = None

print '%s' % mgi_utils.date()

db.useOneConnection(1)
db.set_sqlLogFunction(db.sqlLogAll)
createBCPfile(markerKey)
db.useOneConnection(0)

print '%s' % mgi_utils.date()

