#!/usr/local/bin/python

'''
#
# Purpose:
#
# Create bcp file for MRK_OMIM_Cache, a cache table of:
#
#	1.  Mouse genotype-to-OMIM Disease annotations
#	2.  Human-to-OMIM Disease annotations
#
# Uses environment variables to determine Server and Database
# (DSQUERY and MGD).
#
# Usage:
#	mrkomim.py
#
# Processing:
#
# TR 3853/OMIM User Requirements
#
# phenotype detail phenoegory
#
#	1. orthologous
#		a. mouse genotype is annotated to Term A (and IS annotation)
#		b. mouse marker has human ortholog
#		c. human ortholog is annotated to Term A
#
#	2. distinct etiology
#		a. mouse genotype is annotated to Term A (and IS annotation)
#		b. mouse marker has human ortholog
#		c. human ortholog is not annotated to Term A
#	   OR
#		a. mouse genotype is annotated to Term A (and IS annotation)
#		b. mouse marker has no human ortholog
#		c. every human gene annotated to Term A has a mouse ortholog
#
#	3. unresolved/unknown etiology
#		a. mouse genotype is annotated to Term A (and IS annotation)
#		b. mouse marker has no human ortholog (implies no structural gene)
#		c. at least one human gene is annotated to Term A and has no
#		   mouse ortholog
#	   OR
#		a. mouse genotype is annotated to Term A (and IS annotation)
#		b. no human gene is annotated to Term A
#	
#	4.  no similarity
#		a. mouse genotype is annotated to Term A (and IS NOT annotation)
#
#	5. mutations
#		a. mouse genotype is annotated to Term A (and IS annotation)
#		b. marker type = "Other Genome Feature", "Complex/Cluster/Region", "QTL"
#
#
# History
#
'''

import sys
import os
import string
import db
import mgi_utils

#NL = os.environ['COLDELIM']
NL = '\n'
#DL = os.environ['FIELDDELIM']
DL = '\t'

cdate = mgi_utils.date("%m/%d/%Y")
createdBy = '1000'

mouseOMIMannotationKey = 1005
humanOMIMannotationKey = 1006
mouseOrganismKey = 1
humanOrganismKey = 2

humanOrtholog = {}	# mouse marker key : human ortholog (key, symbol)
mouseOrtholog = {}	# human marker key : mouse ortholog (key, symbol)

genotypeToOMIM = {}	# mouse genotype key : list of OMIM term ids
humanToOMIM = {}	# human marker key : OMIM term id
mouseToOMIM = {}	# mouse marker key : OMIM term id
OMIMToHuman = {}	# OMIM term id : list of human marker keys

mutation = [9, 10, 6]

pheno1Header = '%s is associated with the disease in humans; the mouse model also carries %s mutations.'
pheno2Header = '%s is not associated with this disease in humans.'
pheno3Header = 'No causative human gene is known for the disease; or the mouse structural gene is not identified; or no ortholog has been estabished between the mouse and a human gene.'
pheno4HeaderA = '%s is associated with this human disease.  '
pheno4HeaderB = 'The mouse genotype involves %s mutations but the phenotype did not resemble the human disease.'

outDir = os.environ['MRKCACHEBCPDIR']

def deriveCategory1(r):

	marker = r['_Marker_key']
	symbol = r['markerSymbol']
	termID = r['termID']
	hasOrtholog = 0

	if humanOrtholog.has_key(marker):
	    hasOrtholog = 1
	    ortholog = humanOrtholog[marker]
	    orthologKey = ortholog['orthologKey']
	    orthologSymbol = ortholog['orthologSymbol']

	header = ''

#
#	4.  no similarity
#		a. mouse genotype is annotated to Term A (and IS NOT annotation)
#

	if r['isNot'] == 1:
	    if hasOrtholog:
		header = pheno4HeaderA % (orthologSymbol)
	    header = header + pheno4HeaderB % (symbol)
	    return 4, header

#
#	5. transgene
#		a. mouse genotype is annotated to Term A (and IS annotation)
#		b. marker type = "Other Genome Feature"
#

	elif r['_Marker_Type_key'] in mutation:
	    # need header
	    return 5, header

#
#	1. orthologous
#		a. mouse genotype is annotated to Term A (and IS annotation)
#		b. mouse marker has human ortholog
#		c. human ortholog is annotated to Term A
#
#	2. distinct etiology
#		a. mouse genotype is annotated to Term A (and IS annotation)
#		b. mouse marker has human ortholog
#		c. human ortholog is not annotated to Term A
#

	elif hasOrtholog:
	    if humanToOMIM.has_key(orthologKey):
		omim = humanToOMIM[orthologKey]
		if termID in omim:
		    header = pheno1Header % (orthologSymbol, symbol)
		    return 1, header
		else:
		    header = pheno2Header % (orthologSymbol)
		    return 2, header
	    else:
	        header = pheno2Header % (orthologSymbol)
	        return 2, header

#
#	2. distinct etiology
#		a. mouse genotype is annotated to Term A (and IS annotation)
#		b. mouse marker has no human ortholog
#		c. every human gene annotated to Term A has a mouse ortholog
#
#	3. unresolved/unknown etiology
#		a. mouse genotype is annotated to Term A (and IS annotation)
#		b. mouse marker has no human ortholog (implies no structural gene)
#		c. at least one human gene is annotated to Term A and has no mouse ortholog
#	   OR
#		a. mouse genotype is annotated to Term A (and IS annotation)
#		b. no human gene is annotated to Term A
#	

	else:
	    if OMIMToHuman.has_key(termID):
		orthologFound = 1
		for g in OMIMToHuman[termID]:
		    if not mouseOrtholog.has_key(g):
			orthologFound = 0
	        if orthologFound:
		    # header?
		    return 2, header
	        else:
		    return 3, pheno3Header
	    else:
		return 3, pheno3Header

	return -1, header

def deriveCategory2(r):

#
#	1. Term A is annotated in both Mouse and Human
#	2. Term A is annotated in Mouse but not Human
#	3. Term A is annotated in Human but not Mouse
#

	organism = r['_Organism_key']
	marker = r['_Marker_key']
	symbol = r['markerSymbol']
	termID = r['termID']
	hasOrtholog = 0

	#
	# process mouse reocrd
	#

	if organism == mouseOrganismKey:

	    if humanOrtholog.has_key(marker):
	        hasOrtholog = 1
	        ortholog = humanOrtholog[marker]
	        orthologKey = ortholog['orthologKey']
	        orthologSymbol = ortholog['orthologSymbol']

	    if r['isNot'] == '1':
	        return -1

	    elif hasOrtholog:
	        if humanToOMIM.has_key(orthologKey):
		    omim = humanToOMIM[orthologKey]
		    if termID in omim:
		        return 1
		    else:
		        return 2
	        else:
	            return 2
	    else:
		return 2

	#
	# process human record
	# 

	else:

	    if mouseOrtholog.has_key(marker):
	        hasOrtholog = 1
	        ortholog = mouseOrtholog[marker]
	        orthologKey = ortholog['orthologKey']
	        orthologSymbol = ortholog['orthologSymbol']

	    if hasOrtholog:
	        if mouseToOMIM.has_key(orthologKey):
		    omim = mouseToOMIM[orthologKey]
		    if termID in omim:
		        return 1
		    else:
		        return 3
	        else:
	            return 3
	    else:
		return 3

def selectMouse():

	global humanOrtholog, genotypeToOMIM, mouseToOMIM

	#
	# select all mouse genotypes annotated to OMIM Disease Terms
	#
	db.sql('select g._Marker_key, g._Allele_key, g._Genotype_key, g.sequenceNum, ' + \
		'a._Term_key, a.isNot, e._Refs_key ' + \
		'into #omimmouse1 ' + \
		'from GXD_AlleleGenotype g, VOC_Annot a, VOC_Evidence e ' + \
		'where g._Genotype_key = a._Object_key ' + \
		'and a._AnnotType_key = %s ' % (mouseOMIMannotationKey) + \
		'and a._Annot_key = e._Annot_key', None)
	db.sql('create index idx1 on #omimmouse1(_Marker_key)', None)
	db.sql('create index idx2 on #omimmouse1(_Allele_key)', None)

	#
	# resolve marker symbol
	#
	db.sql('select o.*, m._Organism_key, markerSymbol = m.symbol, m._Marker_Type_key, alleleSymbol = a.symbol ' + \
		'into #omimmouse2 ' + \
		'from #omimmouse1 o, MRK_Marker m, ALL_Allele a ' + \
		'where o._Marker_key = m._Marker_key ' + \
		'and o._Allele_key = a._Allele_key', None)
	db.sql('create index idx1 on #omimmouse2(_Marker_key)', None)

	#
	# resolve Marker ID
	# 
	db.sql('select o.*, mgiID = a.accID ' + \
		'into #omimmouse3 ' + \
		'from #omimmouse2 o, ACC_Accession a ' + \
		'where o._Marker_key = a._Object_key ' + \
		'and a._MGIType_key = 2 ' + \
		'and a._LogicalDB_key = 1 ' + \
		'and a.prefixPart = "MGI:" ' + \
		'and a.preferred = 1', None)
	db.sql('create index idx1 on #omimmouse3(_Term_key)', None)

	#
	# resolve OMIM term and ID
	#
	db.sql('select o.*, t.term, termID = a.accID ' + \
		'into #omimmouse4 ' + \
		'from #omimmouse3 o, VOC_Term t, ACC_Accession a ' + \
		'where o._Term_key = t._Term_key ' + \
		'and o._Term_key = a._Object_key ' + \
		'and a._MGIType_key = 13 ' + \
		'and a.preferred = 1', None)
	db.sql('create index idx1 on #omimmouse4(_Refs_key)', None)

	#
	# cache all terms annotated to genotypes
	#
	results = db.sql('select o._Genotype_key, o.termID from #omimmouse4 o order by o._Genotype_key', 'auto')
	for r in results:
	    key = r['_Genotype_key']
	    value = r['termID']
	    if not genotypeToOMIM.has_key(key):
		genotypeToOMIM[key] = []
	    genotypeToOMIM[key].append(value)

	#
	# cache all terms annotated to mouse markers
	#
	results = db.sql('select distinct o._Marker_key, o.termID from #omimmouse4 o order by o._Marker_key', 'auto')
	for r in results:
	    key = r['_Marker_key']
	    value = r['termID']
	    if not mouseToOMIM.has_key(key):
		mouseToOMIM[key] = []
	    mouseToOMIM[key].append(value)

	#
	# resolve Jnumber
	#
	db.sql('select o.*, jnumID = a.accID ' + \
		'into #omimmouse5 ' + \
		'from #omimmouse4 o, ACC_Accession a ' + \
		'where o._Refs_key = a._Object_key ' + \
		'and a._MGIType_key = 1 ' + \
		'and a._LogicalDB_key = 1 ' + \
		'and a.prefixPart = "J:" ' + \
		'and a.preferred = 1', None)
	db.sql('create index idx1 on #omimmouse5(_Genotype_key)', None)

	#
	# resolve genotype Strain
	#
	db.sql('select o.*, s.strain ' + \
		'into #omimmouse6 ' + \
		'from #omimmouse5 o, GXD_Genotype g, PRB_Strain s ' + \
		'where o._Genotype_key = g._Genotype_key ' + \
		'and g._Strain_key = s._Strain_key', None)
	db.sql('create index idx1 on #omimmouse6(_Allele_key)', None)

	#
	# resolve genotype display
	#
	genotypeDisplay = {}
	results = db.sql('select o._Genotype_key, note = rtrim(nc.note) from #omimmouse1 o, MGI_Note n, MGI_NoteChunk nc ' + \
		'where o._Genotype_key = n._Object_key ' + \
		'and n._NoteType_key = 1018 ' + \
		'and n._Note_key = nc._Note_key ' + \
		'order by o._Genotype_key, nc.sequenceNum', 'auto')
	for r in results:
	    key = r['_Genotype_key']
	    value = r['note']
	    if not genotypeDisplay.has_key(key):
		genotypeDisplay[key] = []
	    genotypeDisplay[key].append(value)

	#
	# resolve human ortholog
	#
	results = db.sql('select distinct o._Marker_key, orthologKey = h2._Marker_key, orthologSymbol = m2.symbol ' + \
        	'from #omimmouse1 o, HMD_Homology r1, HMD_Homology_Marker h1, ' + \
        	'HMD_Homology r2, HMD_Homology_Marker h2, ' + \
        	'MRK_Marker m2 ' + \
        	'where o._Marker_key = h1._Marker_key ' + \
        	'and h1._Homology_key = r1._Homology_key ' + \
        	'and r1._Class_key = r2._Class_key ' + \
        	'and r2._Homology_key = h2._Homology_key ' + \
        	'and h2._Marker_key = m2._Marker_key ' + \
        	'and m2._Organism_key = %s' % (humanOrganismKey), 'auto')
	for r in results:
	    key = r['_Marker_key']
	    value = r
	    humanOrtholog[key] = value

def printMouse():

	results = db.sql('select * from #omimmouse6 order by _Marker_key', 'auto')
	for r in results:

	    marker = r['_Marker_key']
	    genotype = r['_Genotype_key']
	    displayCategory1, header = deriveCategory1(r)
	    displayCategory2 = deriveCategory2(r)
	    mgiID = r['mgiID']

	    omimBCP.write(
	        mgi_utils.prvalue(displayCategory1) + DL + \
	        mgi_utils.prvalue(displayCategory2) + DL + \
		mgi_utils.prvalue(mouseOrganismKey) + DL +  \
		mgi_utils.prvalue(marker) + DL +  \
		mgi_utils.prvalue(r['_Allele_key']) + DL + \
		mgi_utils.prvalue(genotype) + DL + \
		mgi_utils.prvalue(r['_Term_key']) + DL + \
		mgi_utils.prvalue(r['_Refs_key']) + DL + \
		r['markerSymbol'] + DL + \
		r['alleleSymbol'] + DL + \
		r['term'] + DL + \
		r['termID'] + DL + \
		r['jnumID'] + DL + \
		r['strain'] + DL + \
		mgi_utils.prvalue(r['isNot']) + DL + \
		mgi_utils.prvalue(r['sequenceNum']) + DL)

#		string.join(genotypeDisplay[genotype], '') + DL + \

	    if humanOrtholog.has_key(marker):
		h = humanOrtholog[marker]
	        omimBCP.write(mgi_utils.prvalue(humanOrganismKey) + DL + \
	        	mgi_utils.prvalue(h['orthologKey']) + DL + \
	        	h['orthologSymbol'])
	    else:
		omimBCP.write(DL + DL)

	    omimBCP.write(NL)

	    if displayCategory1 == 1:
		pheno1HeaderBCP.write(mgiID + DL + header + NL)
	    elif displayCategory1 == 2:
		pheno2HeaderBCP.write(mgiID + DL + header + NL)
	    elif displayCategory1 == 3:
		pheno3HeaderBCP.write(mgiID + DL + header + NL)
	    elif displayCategory1 == 4:
		pheno4HeaderBCP.write(mgiID + DL + header + NL)
	    elif displayCategory1 == 5:
		pheno5HeaderBCP.write(mgiID + DL + header + NL)

	    reviewBCP.write(
	        mgi_utils.prvalue(displayCategory1) + DL + \
	        mgi_utils.prvalue(displayCategory2) + DL + \
		r['markerSymbol'] + DL + \
		r['alleleSymbol'] + DL + \
		r['term'] + DL + \
		r['termID'] + DL + \
		r['jnumID'] + DL + \
		r['strain'] + DL + \
		mgi_utils.prvalue(r['isNot']) + DL)

	    if humanOrtholog.has_key(marker):
		h = humanOrtholog[marker]
	        reviewBCP.write(h['orthologSymbol'])
	    else:
	        reviewBCP.write(DL)

	    reviewBCP.write(NL)

def selectHuman():

	global mouseOrtholog, humanToOMIM, OMIMToHuman

	#
	# select all human genes annotated to OMIM Gene or Disease Terms
	#

	db.sql('select _Marker_key = a._Object_key, termID = ac.accID, a._Term_key, t.term, a.isNot, e._Refs_key ' + \
		'into #omimhuman1 ' + \
		'from VOC_Annot a, VOC_Evidence e, VOC_Term t, ACC_Accession ac ' + \
		'where a._AnnotType_key = %s ' % (humanOMIMannotationKey) + \
		'and a._Annot_key = e._Annot_key ' + \
		'and a._Term_key = t._Term_key ' + \
		'and a._Term_key = ac._Object_key ' + \
		'and ac._MGIType_key = 13 ' + \
		'and ac.preferred = 1', None)
	db.sql('create index idx1 on #omimhuman1(_Marker_key)', None)

	#
	# resolve marker symbol
	#
	db.sql('select o.*, m._Organism_key, markerSymbol = m.symbol ' + \
		'into #omimhuman2 ' + \
		'from #omimhuman1 o, MRK_Marker m ' + \
		'where o._Marker_key = m._Marker_key ', None)
	db.sql('create index idx1 on #omimhuman2(_Marker_key)', None)

	#
	# cache all terms annotated to human markers
	# cache all human markers annotated to terms
	#
	results = db.sql('select o._Marker_key, o.termID from #omimhuman2 o order by o._Marker_key', 'auto')
	for r in results:
	    key = r['_Marker_key']
	    value = r['termID']
	    if not humanToOMIM.has_key(key):
		humanToOMIM[key] = []
	    humanToOMIM[key].append(value)

	    key = r['termID']
	    value = r['_Marker_key']
	    if not OMIMToHuman.has_key(key):
		OMIMToHuman[key] = []
	    OMIMToHuman[key].append(value)

	#
	# resolve Jnumber
	#
	db.sql('select o.*, jnumID = a.accID ' + \
		'into #omimhuman3 ' + \
		'from #omimhuman2 o, ACC_Accession a ' + \
		'where o._Refs_key = a._Object_key ' + \
		'and a._MGIType_key = 1 ' + \
		'and a._LogicalDB_key = 1 ' + \
		'and a.prefixPart = "J:" ' + \
		'and a.preferred = 1 ' + \
		'union ' + \
		'select o.*, jnumID = null from #omimhuman2 o where _Refs_key = -1', None)

	#
	# resolve mouse ortholog
	#
	results = db.sql('select distinct o._Marker_key, orthologKey = h2._Marker_key, orthologSymbol = m2.symbol ' + \
        	'from #omimhuman1 o, HMD_Homology r1, HMD_Homology_Marker h1, ' + \
        	'HMD_Homology r2, HMD_Homology_Marker h2, ' + \
        	'MRK_Marker m2 ' + \
        	'where o._Marker_key = h1._Marker_key ' + \
        	'and h1._Homology_key = r1._Homology_key ' + \
        	'and r1._Class_key = r2._Class_key ' + \
        	'and r2._Homology_key = h2._Homology_key ' + \
        	'and h2._Marker_key = m2._Marker_key ' + \
        	'and m2._Organism_key = %s' % (mouseOrganismKey), 'auto')
	for r in results:
	    key = r['_Marker_key']
	    value = r
	    mouseOrtholog[key] = value

def printHuman():

	results = db.sql('select * from #omimhuman3', 'auto')

	for r in results:

	    marker = r['_Marker_key']
	    displayCategory1 = -1
	    displayCategory2 = deriveCategory2(r)

	    # don't really have to write these out because the mouse counterpart is already there

	    if displayCategory2 == 1:
		continue

	    omimBCP.write(
	        mgi_utils.prvalue(displayCategory1) + DL + \
	        mgi_utils.prvalue(displayCategory2) + DL + \
		mgi_utils.prvalue(humanOrganismKey) + DL + 
		mgi_utils.prvalue(marker) + DL + 
		DL + \
		DL + \
		mgi_utils.prvalue(r['_Term_key']) + DL + \
		mgi_utils.prvalue(r['_Refs_key']) + DL + \
		r['markerSymbol'] + DL + \
		DL + \
		mgi_utils.prvalue(r['term']) + DL + \
		r['termID'] + DL + \
		mgi_utils.prvalue(r['jnumID']) + DL + \
		DL + \
		mgi_utils.prvalue(r['isNot']) + DL + \
		DL)

	    if mouseOrtholog.has_key(marker):
		h = mouseOrtholog[marker]
	        omimBCP.write(mgi_utils.prvalue(mouseOrganismKey) + DL + \
	        	mgi_utils.prvalue(h['orthologKey']) + DL + \
	        	h['orthologSymbol'])

	    omimBCP.write(NL)

	    reviewBCP.write(
	        mgi_utils.prvalue(displayCategory1) + DL + \
	        mgi_utils.prvalue(displayCategory2) + DL + \
		r['markerSymbol'] + DL + \
		DL + \
		mgi_utils.prvalue(r['term']) + DL + \
		r['termID'] + DL + \
		mgi_utils.prvalue(r['jnumID']) + DL + \
		DL + \
		mgi_utils.prvalue(r['isNot']) + DL)

	    if mouseOrtholog.has_key(marker):
		h = mouseOrtholog[marker]
	        reviewBCP.write(h['orthologSymbol'])

	    reviewBCP.write(NL)

#
# Main Routine
#

print '%s' % mgi_utils.date()

db.useOneConnection(1)
db.set_sqlLogFunction(db.sqlLogAll)
omimBCP = open(outDir + '/MRK_OMIM_Cache.bcp', 'w')
reviewBCP = open(outDir + '/OMIM_Cache_Review.tab', 'w')
pheno1HeaderBCP = open(outDir + '/MRK_OMIM_PhenoHeader1.bcp', 'w')
pheno2HeaderBCP = open(outDir + '/MRK_OMIM_PhenoHeader2.bcp', 'w')
pheno3HeaderBCP = open(outDir + '/MRK_OMIM_PhenoHeader3.bcp', 'w')
pheno4HeaderBCP = open(outDir + '/MRK_OMIM_PhenoHeader4.bcp', 'w')
pheno5HeaderBCP = open(outDir + '/MRK_OMIM_PhenoHeader5.bcp', 'w')

selectMouse()
selectHuman()
printMouse()
printHuman()

omimBCP.close()
reviewBCP.close()
pheno1HeaderBCP.close()
pheno2HeaderBCP.close()
pheno3HeaderBCP.close()
pheno4HeaderBCP.close()
pheno5HeaderBCP.close()
db.useOneConnection(0)

print '%s' % mgi_utils.date()

