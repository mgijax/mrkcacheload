#!/usr/local/bin/python

'''
#
# Purpose:
#
# Create bcp file for MRK_Reference
#
# Uses environment variables to determine Server and Database
# (DSQUERY and MGD).
#
# Usage:
#	mrkref.py [markerkey]
#
# If markerkey is provided, then only create the bcp file for that marker.
#
# IMPORTANT:  Keep in synch with stored procedure MRK_reloadReference.
#
# Processing:
#
#	1. Select all unique Marker/Reference pairs from the other parts of the database.
#	   These are references that are inferred from mapping, expression, etc. curation.
#
#	2. Select all records from MGI_Reference_Assoc.  These are refereneces
#	   that have been explicitly curated by MGI staff.
#
#	3. Result of the union is a file of unique Marker/Reference pairs.
#
# History
#
# 12/08/2005	lec
#	- added jnumID, pubmedID, mgiID and jnum to MRK_Reference
#
# 12/09/2004	lec
#	- TR 5686; replaced MRK_Other with MGI_Synonym
#
# 01/30/2002	lec
#	- TR 2867; replaced GO_DataEvidence w/ VOC_Annot & VOC_Evidence
#
# 11/08/2001	lec
#	- TR 3091; include GO_DataEvidence
#
# 04/10/2001	lec
#	- TR 2217; added ALL_Reference
#	- forcing use of indexes
#	- original takes 20-25 minutes; new version 5 minutes.
#
# 08/08/2000	lec
#	- don't include references for private accession numbers
#
# 07/24/2000	lec
#	- for ACC_AccessionReference, remove restriction of only Sequence IDs
#
# 05/17/2000	lec
#	- created new CVS product 'mrkrefload'; separated MRK_Label
#	load from MRK_Reference
#
# 05/08/2000	lec
#	- added _Organism_key to MRK_Label
#
# 04/28/2000	lec
#	- TR 1404 - replace MRK_Label/MRK_Symbol w/ MRK_Lable; change semantics of load
#
# 04/18/2000	lec
#	- TR 1177 - add ALL_Synonym to MRK_Symbol and MRK_Label
#
# 04/10/2000	lec
#	- TR 1177 - MRK_Allele table renamed to ALL_Allele
#
# 10/05/1999	lec
#	- TR 365; added MRK_Other to Reference pairs
#
# 08/04/1999	lec
#	- TR 696; added Marker Sequence ID/Reference pairs
#
# 03/30/1999	lec
#	- added argument for creating BCPs for just one Marker
#
# 05/14/98	lec
#	- added GXD_Assay to createMRK_Reference
#
'''

import sys
import os
import db
import mgi_utils

NL = '\n'
DL = os.environ['FIELDDELIM']

cdate = mgi_utils.date("%m/%d/%Y")

outDir = os.environ['MRKCACHEBCPDIR']

def createMRK_Reference(markerKey):
	'''
	#
	# MRK_Reference is a cache table of
	# the union of all distinct Marker/Reference pairs
	# in the database + annotated Reference (MGI_Reference_Assoc)
	# EXCEPT for MLC.
	#
	# Datasets:
	#
	# 1.  Molecular Segments
	# 2.  Orthology
	# 3.  Marker History
	# 4.  Mapping
	# 5.  GXD Index
	# 6.  GXD Assay
	# 7.  Synonyms
	# 8.  Accession Reference
	# 9.  Allele References
	# 10. GO Annotations
	# 11. Manually curated (MGI_Reference_Assoc)
	#
	'''

	print 'Creating MRK_Reference.bcp...'

	refBCP = open(outDir + '/MRK_Reference.bcp', 'w')

	#
	# Probe/Marker
	#

	cmd = 'select distinct m._Marker_key, m._Refs_key ' + \
		'into #temp1 ' + \
		'from PRB_Marker m '

	if markerKey is not None:
		cmd = cmd + 'where m._Marker_key = %s' % markerKey

	db.sql(cmd, None)
	db.sql('create index idx1 on #temp1(_Marker_key)', None)
	db.sql('create index idx2 on #temp1(_Refs_key)', None)

	#
	# Orthology
	#

	cmd = 'select distinct m._Marker_key, h._Refs_key ' + \
		'into #temp3 ' + \
		'from MRK_Marker m, HMD_Homology_Marker hm, HMD_Homology h ' + \
		'where m._Organism_key = 1 and ' + \
		'm._Marker_key = hm._Marker_key and ' + \
		'hm._Homology_key = h._Homology_key '

	if markerKey is not None:
		cmd = cmd + 'and hm._Marker_key = %s' % markerKey

	db.sql(cmd, None)
	db.sql('create index idx1 on #temp3(_Marker_key)', None)
	db.sql('create index idx2 on #temp3(_Refs_key)', None)

	#
	# Marker History

	cmd = 'select distinct h._Marker_key, h._Refs_key ' + \
		'into #temp4 ' + \
		'from MRK_History h ' + \
		'where h._Refs_key is not null '

	if markerKey is not None:
		cmd = cmd + 'and h._Marker_key = %s' % markerKey

	db.sql(cmd, None)
	db.sql('create index idx1 on #temp4(_Marker_key)', None)
	db.sql('create index idx2 on #temp4(_Refs_key)', None)

	#
	# Mapping
	#

	cmd = 'select distinct _Marker_key, _Refs_key ' + \
		'into #temp5 ' + \
		'from MLD_Marker '

	if markerKey is not None:
		cmd = cmd + 'where _Marker_key = %s' % markerKey

	db.sql(cmd, None)
	db.sql('create index idx1 on #temp5(_Marker_key)', None)
	db.sql('create index idx2 on #temp5(_Refs_key)', None)

	#
	# GXD Index
	#

	cmd = 'select distinct _Marker_key, _Refs_key ' + \
		'into #temp6 ' + \
		'from GXD_Index '

	if markerKey is not None:
		cmd = cmd + 'where _Marker_key = %s' % markerKey

	db.sql(cmd, None)
	db.sql('create index idx1 on #temp6(_Marker_key)', None)
	db.sql('create index idx2 on #temp6(_Refs_key)', None)

	#
	# GXD Assay (actually, this should be redundant with GXD_Index)
	# 

	cmd = 'select distinct _Marker_key, _Refs_key ' + \
		'into #temp7 ' + \
		'from GXD_Assay '

	if markerKey is not None:
		cmd = cmd + 'where _Marker_key = %s' % markerKey

	db.sql(cmd, None)
	db.sql('create index idx1 on #temp7(_Marker_key)', None)
	db.sql('create index idx2 on #temp7(_Refs_key)', None)

	#
	# Marker Synonyms
	#

	cmd = 'select distinct _Marker_key = s._Object_key, s._Refs_key ' + \
		'into #temp8 ' + \
		'from MGI_Synonym s, MGI_SynonymType st ' + \
		'where s._MGIType_key = 2 ' + \
		'and s._Refs_key is not null ' + \
		'and s._SynonymType_key = st._SynonymType_key ' + \
		'and st._Organism_key = 1'

	if markerKey is not None:
		cmd = cmd + 'and s._Object_key = %s' % markerKey

	db.sql(cmd, None)
	db.sql('create index idx1 on #temp8(_Marker_key)', None)
	db.sql('create index idx2 on #temp8(_Refs_key)', None)

	#
	#  Note that this also handles Sequence/Reference associations, indirectly.
	#

	cmd = 'select distinct _Marker_key = a._Object_key, ar._Refs_key ' + \
		'into #temp9 ' + \
		'from MRK_Marker m, ACC_Accession a, ACC_AccessionReference ar ' + \
		'where m._Organism_key = 1 ' + \
		'and m._Marker_key = a._Object_key ' + \
		'and a._MGIType_key = 2 ' + \
		'and a.private = 0 ' + \
		'and a._Accession_key = ar._Accession_key '

	if markerKey is not None:
		cmd = cmd + 'and _Object_key = %s' % markerKey

	db.sql(cmd, None)
	db.sql('create index idx1 on #temp9(_Marker_key)', None)
	db.sql('create index idx2 on #temp9(_Refs_key)', None)

	#
	# Alleles
	#

	cmd = 'select distinct a._Marker_key, r._Refs_key ' + \
		'into #temp10 ' + \
		'from ALL_Allele a, MGI_Reference_Assoc r ' + \
		'where a._Marker_key is not null ' + \
		'and a._Allele_key = r._Object_key ' + \
		'and r._MGIType_key = 11 '

	if markerKey is not None:
		cmd = cmd + 'and _Object_key = %s' % markerKey

	db.sql(cmd, None)
	db.sql('create index idx1 on #temp10(_Marker_key)', None)
	db.sql('create index idx2 on #temp10(_Refs_key)', None)

	#
	# GO Annotations
	#

	cmd = 'select distinct _Marker_key = a._Object_key, r._Refs_key ' + \
		'into #temp11 ' + \
		'from VOC_Annot a, VOC_Evidence r ' + \
		'where a._AnnotType_key = 1000 ' + \
		'and a._Annot_key = r._Annot_key '

	if markerKey is not None:
		cmd = cmd + 'and _Object_key = %s' % markerKey

	db.sql(cmd, None)
	db.sql('create index idx1 on #temp11(_Marker_key)', None)
	db.sql('create index idx2 on #temp11(_Refs_key)', None)

	#
	# Curated References
	#

	cmd = 'select distinct _Marker_key = m._Object_key, m._Refs_key ' + \
		'into #temp12 ' + \
		'from MGI_Reference_Assoc m ' + \
		'where m._MGIType_key = 2 '

	if markerKey is not None:
		cmd = cmd + 'and m._Object_key = %s' % markerKey

	db.sql(cmd, None)
	db.sql('create index idx1 on #temp12(_Marker_key)', None)
	db.sql('create index idx2 on #temp12(_Refs_key)', None)

	#
	# union them all together
	#

	db.sql('select _Marker_key, _Refs_key into #refs from #temp1 ' + \
		'union select _Marker_key, _Refs_key from #temp3 ' + \
		'union select _Marker_key, _Refs_key from #temp4 ' + \
		'union select _Marker_key, _Refs_key from #temp5 ' + \
		'union select _Marker_key, _Refs_key from #temp6 ' + \
		'union select _Marker_key, _Refs_key from #temp7 ' + \
		'union select _Marker_key, _Refs_key from #temp8 ' + \
		'union select _Marker_key, _Refs_key from #temp9 ' + \
		'union select _Marker_key, _Refs_key from #temp10 ' + \
		'union select _Marker_key, _Refs_key from #temp11 ' + \
		'union select _Marker_key, _Refs_key from #temp12', None)
        db.sql('create index idx1 on #refs(_Refs_key)', None)

	mgiID = {}
	jnumID = {}
	jnum = {}
	pubmedID = {}

	results = db.sql('select r._Refs_key, a._LogicalDB_key, a.prefixPart, a.numericPart, a.accID ' + \
		'from #refs r, ACC_Accession a ' + \
		'where r._Refs_key = a._Object_key ' + \
		'and a._MGIType_key = 1 ' + \
		'and a._LogicalDB_key in (1, 29) ' + \
		'and a.preferred = 1', 'auto')
        for r in results:
	    key = r['_Refs_key']
	    value = r['accID']
	    lkey = r['_LogicalDB_key']
	    pp = r['prefixPart']
	    np = r['numericPart']

	    if lkey == 1 and pp == 'MGI:':
		mgiID[key] = value
	    elif lkey == 1 and pp == 'J:':
		jnumID[key] = value
		jnum[key] = np
            else:
		pubmedID[key] = value

	results = db.sql('select _Marker_key, _Refs_key from #refs', 'auto')
	for r in results:
	    key = r['_Refs_key']

	    refBCP.write(mgi_utils.prvalue(r['_Marker_key']) + DL + \
		       	mgi_utils.prvalue(key) + DL + \
			mgi_utils.prvalue(mgiID[key]) + DL + \
			mgi_utils.prvalue(jnumID[key]) + DL)

            if pubmedID.has_key(key):
		refBCP.write(mgi_utils.prvalue(pubmedID[key]))

            refBCP.write(DL + mgi_utils.prvalue(jnum[key]) + DL + \
			cdate + DL + \
			cdate + NL)
	    refBCP.flush()

	refBCP.close()

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
createMRK_Reference(markerKey)
db.useOneConnection(0)

print '%s' % mgi_utils.date()

