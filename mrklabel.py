#!/usr/local/bin/python

'''
#
# Purpose:
#
# Create bcp file for MRK_Label
#
# Uses environment variables to determine Server and Database
#
# Usage:
#	mrklabel.py [markerkey]
#
# If markerkey is provided, then only create the bcp file for that marker.
#
# History
#
# 01/15/2013 - jsb - updated to use HomoloGene classes as the source for
#	homology data, rather than the old 1-1 homology tables
#
# 11/16/2004	lec
#	- TR 5686; reorganized; added new synonym support
#
# 08/26/2003	lec
#	- TR 4708
#
# 02/13/2003	lec
#	- TR 1892 (LAF2)
#	- added new fields to MRK_Label
#	- added Alleles, Orthology
#
# 04/05/2001	lec
#	- TR 1939; added ALL_Allele.nomenSymbol
#
# 05/17/2000	lec
#	- created new CVS product 'mrklabelload'; separated MRK_Label
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
import mgi_utils

try:
    BCPDL = os.environ['COLDELIM']
    table = os.environ['TABLE']
    outDir = os.environ['MRKCACHEBCPDIR']
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
    table = 'MRK_Label'

NL = '\n'
labelKey = 1
cdate = mgi_utils.date("%m/%d/%Y")

#
# priority	label type	label name
#
# 1		MS		current symbol (mouse)
# 2		MN		current name (mouse)
#
# 3		AS		allele symbol
# 4		AN		allele name
#
# 5		MS		old symbol
# 6		MN		old name
#
# 7		MY		synonym (exact)
# 8		MY		human synonym
# 9		MY		rat synonym
# 10		MY		related synonym (similar, broad or narrow)
#
# 11		OS		ortholog symbol (human)
# 11		MS		current symbol (human)
# 12		ON		ortholog name (human)
# 12		MN		current name (human)
#
# 13 		OS		ortholog name (rat)
# 13 		MS		current symbol (rat)
# 13 		MN		current name (rat)
#
# 14 		OS		ortholog name (not human or rat)
# 14 		MS		current symbol (not human or rat)
# 14 		MN		current name (not human or rat)
#
#

def writeRecord(results, labelStatusKey, priority, labelType, labelTypeName):

    global labelKey
    originalLTN = labelTypeName
    
    for r in results:

	if originalLTN is None:
	    labelTypeName = r['labelTypeName']
        outBCP.write(mgi_utils.prvalue(labelKey) + BCPDL + \
                mgi_utils.prvalue(r['_Marker_key']) + BCPDL + \
        	mgi_utils.prvalue(labelStatusKey) + BCPDL + \
        	mgi_utils.prvalue(r['_Organism_key']) + BCPDL + \
        	mgi_utils.prvalue(r['_OrthologOrganism_key']) + BCPDL + \
        	mgi_utils.prvalue(priority) + BCPDL + \
        	mgi_utils.prvalue(r['label']) + BCPDL + \
        	mgi_utils.prvalue(labelType) + BCPDL + \
        	mgi_utils.prvalue(labelTypeName) + BCPDL + \
        	cdate + BCPDL + \
        	cdate + NL)

	labelKey = labelKey + 1

    print 'processed (%d) records...%s' % (len(results), mgi_utils.date())

def priority1():

	# mouse symbols

        print 'processing priority 1...%s' % mgi_utils.date()

	cmd = 'select distinct _Marker_key, _Organism_key, null as _OrthologOrganism_key, symbol as label ' + \
		'from MRK_Marker ' + \
		'where _Marker_Status_key in (1,3) ' + \
		'and _Organism_key = 1 '

	if markerKey is not None:
		cmd = cmd + 'and _Marker_key = %s\n' % markerKey

	writeRecord(db.sql(cmd, 'auto'), 1, 1, 'MS', 'current symbol')

def priority2():

	# mouse names

        print 'processing priority 2...%s' % mgi_utils.date()

	cmd = 'select distinct _Marker_key, _Organism_key, null as _OrthologOrganism_key , name as label ' + \
		'from MRK_Marker ' + \
		'where _Marker_Status_key in (1,3) ' + \
		'and _Organism_key = 1 '

	if markerKey is not None:
		cmd = cmd + 'and _Marker_key = %s\n' % markerKey

	writeRecord(db.sql(cmd, 'auto'), 1, 2, 'MN', 'current name')

def priority3():

	# allele symbols

        print 'processing priority 3...%s' % mgi_utils.date()

	cmd = 'select distinct a._Marker_key, m._Organism_key, null as _OrthologOrganism_key , a.symbol as label ' + \
		'from ALL_Allele a, MRK_Marker m ' + \
		'where a._Marker_key = m._Marker_key ' + \
		'and a.isWildType = 0 ' + \
		'and m._Organism_key = 1 '

	if markerKey is not None:
		cmd = cmd + 'and a._Marker_key = %s\n' % markerKey

	writeRecord(db.sql(cmd, 'auto'), 1, 3, 'AS', 'allele symbol')

def priority4():

	# allele names

        print 'processing priority 4...%s' % mgi_utils.date()

	cmd = 'select distinct a._Marker_key, m._Organism_key, null as _OrthologOrganism_key , a.name as label ' + \
		'from ALL_Allele a, MRK_Marker m ' + \
		'where a._Marker_key = m._Marker_key ' + \
		'and a.name != "wild type" ' + \
		'and m._Organism_key = 1 '

	if markerKey is not None:
		cmd = cmd + 'and a._Marker_key = %s\n' % markerKey

	writeRecord(db.sql(cmd, 'auto'), 1, 4, 'AN', 'allele name')

def priority5():
	
	# historical (old) symbols

        print 'processing priority 5...%s' % mgi_utils.date()

	cmd = 'select distinct m._Marker_key, m._Organism_key, null as _OrthologOrganism_key , m2.symbol as label  ' + \
		'from MRK_History h, MRK_Marker m, MRK_Marker m2 ' + \
		'where h._Marker_key = m._Marker_key ' + \
		'and m._Organism_key = 1 ' + \
		'and h._History_key = m2._Marker_key ' + \
		'and h._History_key != m._Marker_key '

	if markerKey is not None:
		cmd = cmd + 'and h._Marker_key = %s\n' % markerKey

	writeRecord(db.sql(cmd, 'auto'), 2, 5, 'MS', 'old symbol')

def priority6():

	# historical (old) names

        print 'processing priority 6...%s' % mgi_utils.date()

	cmd = 'select distinct h._Marker_key, m._Organism_key, null as _OrthologOrganism_key, h.name as label  ' + \
		'from MRK_History h, MRK_Marker m, MRK_Marker m2 ' + \
		'where h.name is not null ' + \
		'and h._Marker_key = m._Marker_key ' + \
		'and m._Organism_key = 1 ' + \
		'and h._History_key = m2._Marker_key ' + \
		'and h._History_key != m._Marker_key '

	if markerKey is not None:
		cmd = cmd + 'and h._Marker_key = %s\n' % markerKey

	writeRecord(db.sql(cmd, 'auto'), 2, 6, 'MN', 'old name')

def priority7():

	# mouse synonyms (exact)

        print 'processing priority 7...%s' % mgi_utils.date()

	cmd = 'select distinct _Marker_key = s._Object_key, st._Organism_key, null as _OrthologOrganism_key , s.synonym as label ' + \
		'from MGI_SynonymType st, MGI_Synonym s ' + \
		'where st._MGIType_key = 2 ' + \
		'and st._Organism_key = 1 ' + \
		'and st.synonymType = "exact" ' + \
		'and st._SynonymType_key = s._SynonymType_key '

	if markerKey is not None:
		cmd = cmd + 'and s._Object_key = %s\n' % markerKey

	writeRecord(db.sql(cmd, 'auto'), 1, 7, 'MY', 'synonym')

def priority8():

	# human synonyms of mouse orthologs

        print 'processing priority 8...%s' % mgi_utils.date()

	# build a temp table (#orthology1) with four columns:
	#	_Marker_key           : mouse marker key
	#	m2                    : human marker key
	#	_Organism_key         : mouse organism key (1)
	#	_OrthologOrganism_key : human organism key (2)
	#
	# and populate it with mouse/human homologies from the HomoloGene
	# classes

	cmd = '''select distinct mouse._Marker_key,
			human._Marker_key as m2,
			mm._Organism_key,
			hm._Organism_key as _OrthologOrganism_key
		into #orthology1
		from VOC_Term vt,
			MRK_Cluster mc,
			MRK_ClusterMember mouse,
			MRK_Marker mm,
			MRK_ClusterMember human,
			MRK_Marker hm
		where (vt.term = 'HomoloGene' or vt.term = 'HGNC')
			and vt._Term_key = mc._ClusterSource_key
			and mc._Cluster_key = mouse._Cluster_key
			and mouse._Marker_key = mm._Marker_key
			and mm._Organism_key = 1
			and mc._Cluster_key = human._Cluster_key
			and human._Marker_key = hm._Marker_key
			and hm._Organism_key = 2
		'''

	if markerKey is not None:
		cmd = cmd + 'and mm._Marker_key = %s\n' % markerKey

	db.sql(cmd, None)
	db.sql('create index idx3 on #orthology1(m2)', None)
	db.sql('create index idx4 on #orthology1(_OrthologOrganism_key)', None)

	# human synonym

	cmd = 'select distinct o._Marker_key, o._Organism_key, o._OrthologOrganism_key, label = s.synonym ' + \
		'from #orthology1 o, MGI_SynonymType st, MGI_Synonym s ' + \
		'where st._MGIType_key = 2 ' + \
		'and st._Organism_key = o._OrthologOrganism_key ' + \
		'and st._SynonymType_key = s._SynonymType_key ' + \
		'and o.m2 = s._Object_key '

	if markerKey is not None:
		cmd = cmd + 'and s._Object_key = %s\n' % markerKey

	writeRecord(db.sql(cmd, 'auto'), 1, 8, 'MY', 'human synonym')

def priority9():

	# rat synonyms of mouse orthologs

        print 'processing priority 9...%s' % mgi_utils.date()

	# build a temp table (#orthology2) with four columns:
	#	_Marker_key           : mouse marker key
	#	m2                    : rat marker key
	#	_Organism_key         : mouse organism key (1)
	#	_OrthologOrganism_key : rat organism key (40)
	#
	# and populate it with mouse/rat homologies from the HomoloGene
	# classes

	cmd = '''select distinct mouse._Marker_key,
			human._Marker_key as m2,
			mm._Organism_key,
			hm._Organism_key as _OrthologOrganism_key
		into #orthology2
		from VOC_Term vt,
			MRK_Cluster mc,
			MRK_ClusterMember mouse,
			MRK_Marker mm,
			MRK_ClusterMember human,
			MRK_Marker hm
		where vt.term = 'HomoloGene'
			and vt._Term_key = mc._ClusterSource_key
			and mc._Cluster_key = mouse._Cluster_key
			and mouse._Marker_key = mm._Marker_key
			and mm._Organism_key = 1
			and mc._Cluster_key = human._Cluster_key
			and human._Marker_key = hm._Marker_key
			and hm._Organism_key = 40
		'''

	if markerKey is not None:
		cmd = cmd + 'and mm._Marker_key = %s\n' % markerKey

	db.sql(cmd, None)
	db.sql('create index idx5 on #orthology2(m2)', None)
	db.sql('create index idx6 on #orthology2(_OrthologOrganism_key)', None)

	# rat synonym

	cmd = 'select distinct o._Marker_key, o._Organism_key, o._OrthologOrganism_key, label = s.synonym ' + \
		'from #orthology2 o, MGI_SynonymType st, MGI_Synonym s ' + \
		'where st._MGIType_key = 2 ' + \
		'and st._Organism_key = o._OrthologOrganism_key ' + \
		'and st._SynonymType_key = s._SynonymType_key ' + \
		'and o.m2 = s._Object_key '

	if markerKey is not None:
		cmd = cmd + 'and s._Object_key = %s\n' % markerKey

	writeRecord(db.sql(cmd, 'auto'), 1, 9, 'MY', 'rat synonym')

def priority10():

	# mouse synonyms (similar, broad, narrow)

        print 'processing priority 10...%s' % mgi_utils.date()

	cmd = 'select distinct _Marker_key = s._Object_key, st._Organism_key, null as  _OrthologOrganism_key, s.synonym as label ' + \
		'from MGI_SynonymType st, MGI_Synonym s ' + \
		'where st._MGIType_key = 2 ' + \
		'and st._Organism_key = 1 ' + \
		'and st.synonymType in ("similar", "broad", "narrow") ' + \
		'and st._SynonymType_key = s._SynonymType_key '

	if markerKey is not None:
		cmd = cmd + 'and s._Object_key = %s\n' % markerKey

	writeRecord(db.sql(cmd, 'auto'), 1, 10, 'MY', 'related synonym')

def priority11():

	# human ortholog (symbol)

        print 'processing priority 11...%s' % mgi_utils.date()

	cmd = 'select o.*, label = m.symbol, s.commonName + " symbol" as labelTypeName ' + \
		'from #orthology1 o, MRK_Marker m, MGI_Organism s ' + \
		'where o.m2 = m._Marker_key ' + \
		'and o._OrthologOrganism_key = s._Organism_key '

	writeRecord(db.sql(cmd, 'auto'), 1, 11, 'OS', None)

	# human symbol

	cmd = 'select _Marker_key, _Organism_key, null as _OrthologOrganism_key , symbol as label ' + \
		'from MRK_Marker ' + \
		'where _Organism_key = 2 '

	writeRecord(db.sql(cmd, 'auto'), 1, 11, 'MS', 'current symbol')

def priority12():

	# human ortholog (name)

        print 'processing priority 12...%s' % mgi_utils.date()

	cmd = 'select o.*, label = m.name, s.commonName + " name" as labelTypeName ' + \
		'from #orthology1 o, MRK_Marker m, MGI_Organism s ' + \
		'where o.m2 = m._Marker_key ' + \
		'and o._OrthologOrganism_key = s._Organism_key '

	writeRecord(db.sql(cmd, 'auto'), 1, 12, 'ON', None)

	# human name

	cmd = 'select _Marker_key, _Organism_key, null as _OrthologOrganism_key , name as label ' + \
		'from MRK_Marker ' + \
		'where _Organism_key = 2 '

	writeRecord(db.sql(cmd, 'auto'), 1, 12, 'MN', 'current name')

def priority13():

	# rat ortholog (symbol)

        print 'processing priority 13...%s' % mgi_utils.date()

	cmd = 'select o.*, label = m.symbol, s.commonName + " symbol" as labelTypeName ' + \
		'from #orthology2 o, MRK_Marker m, MGI_Organism s ' + \
		'where o.m2 = m._Marker_key ' + \
		'and o._OrthologOrganism_key = s._Organism_key '

	# rat symbol

	writeRecord(db.sql(cmd, 'auto'), 1, 13, 'OS', None)

	cmd = 'select _Marker_key, _Organism_key, null as _OrthologOrganism_key, symbol as label ' + \
		'from MRK_Marker ' + \
		'where _Organism_key = 40 '

	# rat name

	writeRecord(db.sql(cmd, 'auto'), 1, 13, 'MS', 'current symbol')

	cmd = 'select _Marker_key, _Organism_key, null as _OrthologOrganism_key , name as label ' + \
		'from MRK_Marker ' + \
		'where _Organism_key = 40 '

	writeRecord(db.sql(cmd, 'auto'), 1, 13, 'MN', 'current name')

def priority14():

	# other ortholog (symbol)

        print 'processing priority 14...%s' % mgi_utils.date()

	# build a temp table (#orthology3) with four columns:
	#	_Marker_key           : mouse marker key
	#	m2                    : non-human/rat/mouse marker key
	#	_Organism_key         : mouse organism key (1)
	#	_OrthologOrganism_key : non-human/rat/mouse organism key
	#
	# and populate it with mouse/other (non-rat, non-human, non-mouse)
	# homologies from the HomoloGene classes

	cmd = '''select distinct mouse._Marker_key,
			human._Marker_key as m2,
			mm._Organism_key,
			hm._Organism_key as _OrthologOrganism_key
		into #orthology3
		from VOC_Term vt,
			MRK_Cluster mc,
			MRK_ClusterMember mouse,
			MRK_Marker mm,
			MRK_ClusterMember human,
			MRK_Marker hm
		where vt.term = 'HomoloGene'
			and vt._Term_key = mc._ClusterSource_key
			and mc._Cluster_key = mouse._Cluster_key
			and mouse._Marker_key = mm._Marker_key
			and mm._Organism_key = 1
			and mc._Cluster_key = human._Cluster_key
			and human._Marker_key = hm._Marker_key
			and hm._Organism_key not in (1, 2, 40)
		'''

	if markerKey is not None:
		cmd = cmd + 'and mm._Marker_key = %s\n' % markerKey

	db.sql(cmd, None)
	db.sql('create index idx1 on #orthology3(m2)', None)
	db.sql('create index idx2 on #orthology3(_OrthologOrganism_key)', None)

	cmd = 'select o.*, label = m.symbol, s.commonName + " symbol" as labelTypeName ' + \
		'from #orthology3 o, MRK_Marker m, MGI_Organism s ' + \
		'where o.m2 = m._Marker_key ' + \
		'and o._OrthologOrganism_key = s._Organism_key '

	homologs = db.sql(cmd, 'auto')

	# tweak organism names as needed

	for row in homologs:
		row['labelTypeName'] = row['labelTypeName'].replace (
			', domestic', '')

	writeRecord(homologs, 1, 14, 'OS', None)

	# other symbol

	cmd = 'select _Marker_key, _Organism_key, null as _OrthologOrganism_key, symbol as label ' + \
		'from MRK_Marker ' + \
		'where _Organism_key not in (1,2,40) '

	if markerKey is not None:
		cmd = cmd + 'and _Marker_key = %s\n' % markerKey

	writeRecord(db.sql(cmd, 'auto'), 1, 14, 'MS', 'current symbol')

	# other name

	cmd = 'select _Marker_key, _Organism_key, null as _OrthologOrganism_key , name as label ' + \
		'from MRK_Marker ' + \
		'where _Organism_key not in (1,2,40) '

	if markerKey is not None:
		cmd = cmd + 'and _Marker_key = %s\n' % markerKey

	writeRecord(db.sql(cmd, 'auto'), 1, 14, 'MN', 'current name')

#
# Main Routine
#

print '%s' % mgi_utils.date()

db.useOneConnection(1)
#db.set_sqlLogFunction(db.sqlLogAll)

if len(sys.argv) == 2:
	markerKey = sys.argv[1]
else:
	markerKey = None

outBCP = open(outDir + '/%s.bcp' % (table), 'w')

priority1()
priority2()
priority3()
priority4()
priority5()
priority6()
priority7()
priority8()
priority9()
priority10()
priority11()
priority12()
priority13()
priority14()

outBCP.close()
db.useOneConnection(0)

print '%s' % mgi_utils.date()

