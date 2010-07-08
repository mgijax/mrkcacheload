#!/usr/local/bin/python

'''
#
# Purpose:
#
# Create bcp file for MRK_Location_Cache
#
# Uses environment variables to determine Server and Database
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
# 07/01/2010	lec
#	- TR10207/multiple coordinates for microRNA markers
#
# 05/03/2006	lec
#	- MGI 3.5; added UniSTS and miRBase coordinates
#
#
'''

import sys
import os
import db
import mgi_utils

try:
    COLDL = os.environ['COLDELIM']
    LINEDL = '\n'
    table = os.environ['TABLE']
    outDir = os.environ['MRKCACHEBCPDIR']
except:
    table = 'MRK_Location_Cache'

cdate = mgi_utils.date("%m/%d/%Y")
createdBy = '1000'

def createBCPfile(markerKey):
	'''
	#
	# MRK_Location_Cache is a cache table of marker location data
	#
	'''

	print 'Creating %s.bcp...' % (table)

	locBCP = open(outDir + '/%s.bcp' % (table), 'w')

	db.sql('select m._Marker_key, m._Marker_Type_key, m.symbol, m.chromosome, m.cytogeneticOffset, o.offset, c.sequenceNum ' + \
		'into #markers ' + \
		'from MRK_Marker m, MRK_Offset o, MRK_Chromosome c ' + \
		'where m._Organism_key = 1 ' + \
		'and m._Marker_key = o._Marker_key ' + \
		'and o.source = 0 ' + \
		'and m._Organism_key = c._Organism_key ' + \
		'and m.chromosome = c.chromosome ', None)

	db.sql('create index idx1 on #markers(_Marker_key)', None)

	#
	# the coordinate lookup should contain only one marker coordinate.
	#
	# see TR10207 for more information
	#
	# 1) add to the lookup all coordinates that do not contain a sequence
	# 2) add to the lookup all coordinates that do contain a sequence,
	#    but are not already in the lookup
	#

	coord = {}

	#
	# coordinates for Marker w/out Sequence coordinates
	#

	results = db.sql('select m._Marker_key, f.startCoordinate, f.endCoordinate, f.strand, ' + \
		'mapUnits = u.term, provider = c.name, cc.version ' + \
		'from #markers m, MAP_Coord_Collection c, MAP_Coordinate cc, MAP_Coord_Feature f, VOC_Term u ' + \
		'where m._Marker_key = f._Object_key ' + \
		'and f._MGIType_key = 2 ' + \
		'and f._Map_key = cc._Map_key ' + \
		'and cc._Collection_key = c._Collection_key ' + \
		'and cc._Units_key = u._Term_key', 'auto')
	for r in results:
	    key = r['_Marker_key']
	    value = r

	    if not coord.has_key(key):
		coord[key] = []
            coord[key].append(r)

	#
	# coordinates for Markers w/ Sequence coordinates
	#

	results = db.sql('select m._Marker_key, c.startCoordinate, c.endCoordinate, c.strand, c.mapUnits, c.provider, c.version ' + \
		'from #markers m, SEQ_Marker_Cache mc, SEQ_Coord_Cache c ' + \
		'where m._Marker_key = mc._Marker_key ' + \
		'and mc._Qualifier_key = 615419 ' + \
		'and mc._Sequence_key = c._Sequence_key', 'auto')
	for r in results:
	    key = r['_Marker_key']
	    value = r

	    # only one coordinate per marker
	    if not coord.has_key(key):
		coord[key] = []
                coord[key].append(r)
            #else:
	#	print key, value

	results = db.sql('select * from #markers order by _Marker_key', 'auto')
	for r in results:

	    key = r['_Marker_key']
	    symbol = r['symbol']
	    chr = r['chromosome']

	    if chr == 'UN' and coord.has_key(key):
		print 'Marker has UN chromosome and a coordinate:  ' + symbol

	    # print one record out per coordinate

	    if coord.has_key(key):
		for c in coord[key]:
	            locBCP.write(mgi_utils.prvalue(r['_Marker_key']) + COLDL + \
			        mgi_utils.prvalue(r['_Marker_Type_key']) + COLDL + \
			        chr + COLDL + \
			        mgi_utils.prvalue(r['sequenceNum']) + COLDL + \
			        mgi_utils.prvalue(r['cytogeneticOffset']) + COLDL + \
			        mgi_utils.prvalue(r['offset']) + COLDL + \
			        mgi_utils.prvalue(c['startCoordinate']) + COLDL + \
			        mgi_utils.prvalue(c['endCoordinate']) + COLDL + \
			        mgi_utils.prvalue(c['strand']) + COLDL + \
			        mgi_utils.prvalue(c['mapUnits']) + COLDL + \
			        mgi_utils.prvalue(c['provider']) + COLDL + \
			        mgi_utils.prvalue(c['version']) + COLDL + \
			        createdBy + COLDL + \
			        createdBy + COLDL + \
			        cdate + COLDL + \
			        cdate + LINEDL)
	    else:
	        locBCP.write(mgi_utils.prvalue(r['_Marker_key']) + COLDL + \
			     mgi_utils.prvalue(r['_Marker_Type_key']) + COLDL + \
			     chr + COLDL + \
			     mgi_utils.prvalue(r['sequenceNum']) + COLDL + \
			     mgi_utils.prvalue(r['cytogeneticOffset']) + COLDL + \
			     mgi_utils.prvalue(r['offset']) + COLDL + \
			     COLDL + \
			     COLDL + \
			     COLDL + \
			     COLDL + \
			     COLDL + \
			     COLDL + \
			     createdBy + COLDL + \
			     createdBy + COLDL + \
			     cdate + COLDL + \
			     cdate + LINEDL)
	    locBCP.flush()

	locBCP.close()

#
# Main Routine
#

print '%s' % mgi_utils.date()

if len(sys.argv) == 2:
	markerKey = sys.argv[1]
else:
	markerKey = None

db.useOneConnection(1)
db.set_sqlLogFunction(db.sqlLogAll)
createBCPfile(markerKey)
db.useOneConnection(0)

print '%s' % mgi_utils.date()

