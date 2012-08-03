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
# 08/03/2012	jsb
#	- updated to populate new genomicChromosome field
#
# 09/01/2011	lec
#	- TR10805;added _Organism_key in (1,2)
#	- human data added to the cache
#
# 04/04/2011	lec
#	- TR10658;add _Cache_key
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
#		where m._Organism_key = 1
#		and m._Marker_key = o._Marker_key

	print 'Creating %s.bcp...' % (table)

	locBCP = open(outDir + '/%s.bcp' % (table), 'w')

	# the chromosome retrieved from the marker table is the genetic
	# chromosome, and goes in the traditional 'chromosome' field in the
	# cache table

	db.sql('''select m._Marker_key, m._Marker_Type_key, m._Organism_key, m.symbol, 
		  m.chromosome, m.cytogeneticOffset, o.offset, c.sequenceNum
		into #markers
		from MRK_Marker m, MRK_Offset o, MRK_Chromosome c
		where m._Organism_key in (1,2)
		and m._Marker_key *= o._Marker_key
		and o.source = 0
		and m._Organism_key = c._Organism_key
		and m.chromosome = c.chromosome ''', None)

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
	# The chromosome retrieved with coordinates is the genomic chromosome,
	# and goes in the 'genomicChromosome' field in the cache table.  Most
	# often, it agrees with the genetic chromosome, but not always.

	coord = {}

	#
	# coordinates for Marker w/out Sequence coordinates
	#

	results = db.sql('select m._Marker_key, f.startCoordinate, f.endCoordinate, f.strand, ' + \
		'u.term as mapUnits, c.abbreviation as provider, cc.version, chrom.chromosome as genomicChromosome ' + \
		'from #markers m, MAP_Coord_Collection c, MAP_Coordinate cc, MAP_Coord_Feature f, VOC_Term u, MRK_Chromosome chrom ' + \
		'where m._Marker_key = f._Object_key ' + \
		'and f._MGIType_key = 2 ' + \
		'and f._Map_key = cc._Map_key ' + \
		'and cc._Collection_key = c._Collection_key ' + \
		'and cc._Object_key = chrom._Chromosome_key ' + \
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

	results = db.sql('''select m.symbol, m._Marker_key, c.startCoordinate, 
			c.endCoordinate, c.strand, c.mapUnits, mcc.abbreviation as provider, c.version, c.chromosome as genomicChromosome
                from #markers m, SEQ_Marker_Cache mc, SEQ_Coord_Cache c, 
		    MAP_Coord_Feature mcf, MAP_Coordinate map, MAP_Coord_Collection mcc, 
		    MRK_Marker mm
                where m._Marker_key = mc._Marker_key
                and mc._Qualifier_key = 615419
                and mc._Sequence_key = c._Sequence_key
                and c._Sequence_key = mcf._Object_key
                and mcf._MGIType_key = 19
                and mcf._Map_key = map._Map_key
                and map._Collection_key = mcc._Collection_key
                and m._Marker_key = mm._Marker_key''', 'auto')
	for r in results:
	    key = r['_Marker_key']
	    value = r

	    # only one coordinate per marker
	    if not coord.has_key(key):
		coord[key] = []
                coord[key].append(r)
            #else:
	#	print key, value

	nextMaxKey = 0

	results = db.sql('select * from #markers order by _Marker_key', 'auto')
	for r in results:

	    key = r['_Marker_key']
	    symbol = r['symbol']
	    chr = r['chromosome']

	    if chr == 'UN' and coord.has_key(key):
		print 'Marker has UN chromosome and a coordinate:  ' + symbol

	    # print one record out per coordinate

	    nextMaxKey = nextMaxKey + 1

	    if coord.has_key(key):
		for c in coord[key]:
	            locBCP.write(str(nextMaxKey) + COLDL +
				mgi_utils.prvalue(r['_Marker_key']) + COLDL + \
			        mgi_utils.prvalue(r['_Marker_Type_key']) + COLDL + \
			        mgi_utils.prvalue(r['_Organism_key']) + COLDL + \
			        chr + COLDL + \
			        mgi_utils.prvalue(r['sequenceNum']) + COLDL + \
			        mgi_utils.prvalue(r['cytogeneticOffset']) + COLDL + \
			        mgi_utils.prvalue(r['offset']) + COLDL + \
			        mgi_utils.prvalue(c['genomicChromosome']) + COLDL + \
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
	        locBCP.write(str(nextMaxKey) + COLDL + \
			     mgi_utils.prvalue(r['_Marker_key']) + COLDL + \
			     mgi_utils.prvalue(r['_Marker_Type_key']) + COLDL + \
			     mgi_utils.prvalue(r['_Organism_key']) + COLDL + \
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

