#!/usr/local/bin/python

'''
#
# Purpose:
#
# Create bcp file for PRB_Marker
#
# Uses environment variables to determine Server and Database
# (DSQUERY and MGD).
#
# Usage:
#	mrkprobe.py
#
# Processing:
#
# History
#
'''

import sys
import os
import string
import db
import mgi_utils

try:
    BCPDL = os.environ['FIELDDELIM']
    table = os.environ['TABLE']
    outDir = os.environ['MRKCACHEBCPDIR']
except:
    table = 'PRB_Marker'

NL = '\n'
cdate = mgi_utils.date("%m/%d/%Y")
createdBy = '1000'
refsKey = 86302	# J:85324
relationship = 'E'
lowQualityKey = 316340

def createBCPfile():
	'''
	#
	# Create bcp file
	#
	'''

	print 'Creating %s.bcp...' % (table)

        bcpFile = open(outDir + '/%s.bcp' % (table), 'w')

	# delete existing entries

	db.sql('delete from %s where _Refs_key = %s' % (table, refsKey), None)

	# exclude all problem Molecular Segments
	# (those with at least one Sequence of Low Quality)

	db.sql('select distinct c._Probe_key ' + \
		'into #excluded ' + \
		'from SEQ_Probe_Cache c, SEQ_Sequence s ' + \
		'where c._Sequence_key = s._Sequence_key ' + \
		'and s._SequenceQuality_key = %s' % (lowQualityKey), None)

	db.sql('create nonclustered index idx_key on #excluded(_Probe_key)', None)

	# select all mouse probes

	db.sql('select p._Probe_key ' + \
		'into #mouseprobes ' + \
		'from SEQ_Probe_Cache c, PRB_Probe p, PRB_Source s ' + \
		'where c._Probe_key = p._Probe_key ' + \
		'and p._SegmentType_key != 63473 ' + \
		'and p._Source_key = s._Source_key ' + \
		'and s._Organism_key = 1', None)
	
	db.sql('create nonclustered index idx_key on #mouseprobes(_Probe_key)', None)

	# select all mouse Probes and Markers which are annotated to the same nucleotide Sequence

	db.sql('select distinct p._Probe_key, m._Marker_key ' + \
		'into #annotations ' + \
		'from ACC_Accession a, SEQ_Marker_Cache m, SEQ_Probe_Cache p ' + \
		'where a._LogicalDB_key = 9 ' + \
		'and a._MGIType_key = 19 ' + \
		'and a._Object_key = m._Sequence_key ' + \
		'and m._Organism_key = 1 ' + \
		'and a._Object_key = p._Sequence_key ' + \
		'and exists (select 1 from #mouseprobes mp ' + \
		'where p._Probe_key = mp._Probe_key)', None)

	db.sql('create nonclustered index idx_pkey on #annotations(_Probe_key)', None)
	db.sql('create nonclustered index idx_mkey on #annotations(_Marker_key)', None)

	# select all Probes and Markers with Putative annotation
	
	db.sql('select _Probe_key, _Marker_key into #putatives from %s where relationship = "P"' % (table), None)

	db.sql('create nonclustered index idx_pkey on #putatives(_Probe_key)', None)
	db.sql('create nonclustered index idx_mkey on #putatives(_Marker_key)', None)

	# select all Probes and Markers with a non-Putative (E, H), or null Annotation

	db.sql('select _Probe_key, _Marker_key into #nonputatives from %s where relationship != "P" or relationship is null' % (table), None)

	db.sql('create nonclustered index idx_pkey on #nonputatives(_Probe_key)', None)
	db.sql('create nonclustered index idx_mkey on #nonputatives(_Marker_key)', None)

	# select all Molecular Segments which share a Sequence object with a Marker
	# and which already have a "P" association with that Marker

	db.sql('select distinct a._Probe_key, a._Marker_key ' + \
		'into #haveputative ' + \
		'from #annotations a  ' + \
		'where exists (select 1 from #putatives p ' + \
		'where a._Probe_key = p._Probe_key ' + \
		'and a._Marker_key = p._Marker_key) ' + \
		'and not exists (select 1 from #excluded e ' + \
		'where a._Probe_key = e._Probe_key)', None)

	# select all mouse Molecular Segments which share a Seq ID with a mouse Marker
	# and which do not have a non-P/null association with that Marker
	# that is, we don't want to overwrite a curated relationship (even if it's a null relationship)

	db.sql('select distinct a._Probe_key, a._Marker_key ' + \
		'into #createautoe ' + \
		'from #annotations a  ' + \
		'where not exists (select 1 from #nonputatives p ' + \
		'where a._Probe_key = p._Probe_key ' + \
		'and a._Marker_key = p._Marker_key) ' + \
		'and not exists (select 1 from #excluded e ' + \
		'where a._Probe_key = e._Probe_key)', None)

	# delete any putatives which can be trumped by an auto-E relationship

	db.sql('delete %s ' % (table) + \
		'from #haveputative p, #createautoe e, %s pm ' % (table) + \
		'where p._Probe_key = e._Probe_key ' + \
		'and p._Marker_key = e._Marker_key ' + \
		'and e._Probe_key = pm._Probe_key ' + \
		'and e._Marker_key = pm._Marker_key ' + \
		'and pm.relationship = "P"', None)

	# for each molecular segment/marker, create an auto-E relationship

        results = db.sql('select distinct _Probe_key, _Marker_key from #createautoe', 'auto')
	for r in results:
	    bcpFile.write(mgi_utils.prvalue(r['_Probe_key']) + BCPDL + \
	        mgi_utils.prvalue(r['_Marker_key']) + BCPDL + \
	        mgi_utils.prvalue(refsKey) + BCPDL + \
	    	relationship + BCPDL + \
	    	createdBy + BCPDL + \
	    	createdBy + BCPDL + \
	    	cdate + BCPDL + \
	    	cdate + NL)

	bcpFile.close()

#
# Main Routine
#

print '%s' % mgi_utils.date()

# need to delete data, so we need a user with delete permission
server = os.environ['DBSERVER']
database = os.environ['DBNAME']
user = os.environ['DBUSER']
passwordFile = os.environ['DBPASSWORDFILE']
password = string.strip(open(passwordFile, 'r').readline())
db.set_sqlLogin(user, password, server, database)

db.useOneConnection(1)
db.set_sqlLogFunction(db.sqlLogAll)
createBCPfile()
db.useOneConnection(0)

print '%s' % mgi_utils.date()

