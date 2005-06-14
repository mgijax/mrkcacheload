#!/usr/local/bin/python

'''
#
# Purpose:
#
# Create bcp file for MRK_OMIM_Cache, a cache table of:
#
#	1.  Mouse genotype-to-OMIM Disease annotations
#	2.  Human-to-OMIM Disease/Gene annotations
#
# Uses environment variables to determine Server and Database
# (DSQUERY and MGD).
#
# Usage:
#	mrkomim.py
#
# Processing:
#
#	1.  Select all Mouse genotype-to-OMIM Disease annotations
#	2.  Categorize them
#	3.  Write records to output file
#
#	4.  Select all Human-to-OMIM Disease annotations
#	5.  Categorize them
#	6.  Write records to output file
#
# TR 3853/OMIM User Requirements
#
# History
#
# 05/26/2005	lec
#	- TR 3853/OMIM
#
'''

import sys
import os
import string
import regsub
import db
import mgi_utils

NL = '\n'
DL = os.environ['FIELDDELIM']
RDL = '\t'

cdate = mgi_utils.date("%m/%d/%Y")
createdBy = os.environ['CREATEDBY']

mouseOMIMannotationKey = 1005
humanOMIMannotationKey = 1006
mouseOrganismKey = 1
humanOrganismKey = 2

humanOrtholog = {}	# mouse marker key : human ortholog (key, symbol)
mouseOrtholog = {}	# human marker key : mouse ortholog (key, symbol)

humanToOMIM = {}	# human marker key : OMIM term id
mouseToOMIM = {}	# mouse marker key : OMIM term id
OMIMToHuman = {}	# OMIM term id : list of human marker keys

genotypeDisplay = {}	# mouse genotype key: genotype display

gene = 1

#
# Headers on Phenotype Detail Page
#
phenoHeader1 = 'Models with phenotypic similarity to human diseases associated with %s'
phenoHeader2a = 'Models with phenotypic similarity to human diseases not associated with %s'
phenoHeader2b = 'Models with phenotypic similarity to human diseases having known causal genes with established orthologies'
phenoHeader3a = 'Models with phenotypic similarity to human diseases with unknown etiology'
phenoHeader3b = 'Models with phenotypic similarity to human diseases having known causal genes without established mouse orthologs, or diseases with unknown human etiology. '
phenoHeader4 = 'No similarity to the expected human disease phenotype was found'
phenoHeader5 = 'Models involving transgenes or other mutation types'

#
# Header Footnotes on Phenotype Detail Page
#
headerFootnote2b = 'The human diseases are associated with human genes, but %s is not known to be an ortholog of any of them.'
headerFootnote4a = '%s is associated with this human disease.  The mouse genotype involves %s mutations but the phenotype did not resemble the human disease.'
headerFootnote4b = 'The mouse genotype involves %s mutations but the phenotype did not resemble the human disease.'
headerFootnote4c = 'The mouse genotype involves %s mutations but the phenotype did not resemble the human disease.'
headerFootnote5 = 'Models which involve transgenes or other mutation types may appear in other sections of the table.'

#
# Genotype Footnotes on Phenotype Detail Page
#
genotypeFootnote1 = '%s is associated with this disease in humans.  '

outDir = os.environ['MRKCACHEBCPDIR']

def deriveCategory1(r):
	#
	# Purpose: derives the appropriate Phenotype Detail page category for the record 
	#          and hence the appropriate Human Disease Detai page, table 2
	# Returns: the category (1,2,3,4,5), -1 if no category could be determined
	# Assumes:
	# Effects:
	# Throws:
	#

	marker = r['_Marker_key']
	symbol = r['markerSymbol']
	termID = r['termID']
	genotype = r['_Genotype_key']
	markerType = r['_Marker_Type_key']
	hasOrtholog = 0
	header = ''
	headerFootnote = ''
	genotypeFootnote = ''

	# this is only appropriate for mouse-centric records

	if r['_Organism_key'] != mouseOrganismKey:
	    return -1, header, headerFootnote, genotypeFootnote

	# check if marker has a human ortholog

	if humanOrtholog.has_key(marker):
	    hasOrtholog = 1
	    ortholog = humanOrtholog[marker]
	    orthologKey = ortholog['orthologKey']
	    orthologSymbol = ortholog['orthologSymbol']
            isHumanOrthologAnnotated = humanToOMIM.has_key(orthologKey)
	else:
            isHumanOrthologAnnotated = 0

	# check if any human gene is annotated to Term
        isHumanGeneAnnotated = OMIMToHuman.has_key(termID)

	#
	#  5. non-gene
	#	a. mouse genotype is annotated to Term and is a IS annotation
	#	b. marker type != "Gene"
	#

	if markerType != gene:
	    return 5, phenoHeader5, headerFootnote5, genotypeFootnote

	#
	#  1. orthologous
	#	a. mouse genotype is annotated to Term and is a IS annotation
	#	b. mouse marker has human ortholog
	#	c. human ortholog is annotated to Term
	#
	#  2. distinct etiology
	#	a. mouse genotype is annotated to Term and is a IS annotation
	#	b. mouse marker has human ortholog
	#	c. human ortholog is not annotated to Term
	#
	#  3. unresolved/unknown etiology
	#	a. mouse genotype is annotated to Term and IS annotation
	#	b. no human gene is annotated to Term
	#
	#
	#  4.  no similarity
	#	a. mouse genotype is annotated to Term and is a IS NOT annotation
	#

	if hasOrtholog:

	    if r['isNot'] == 1:
	        if isHumanOrthologAnnotated:
		    omim = humanToOMIM[orthologKey]
	            # human ortholog is annotated to Term
		    if termID in omim:
	                headerFootnote = headerFootnote4a % (orthologSymbol, symbol)
	                return 4, phenoHeader4, headerFootnote, genotypeFootnote
	            else:
	                headerFootnote = headerFootnote4b % (symbol)
			return 4, phenoHeader4, headerFootnote, genotypeFootnote
	        else:
	            headerFootnote = headerFootnote4b % (symbol)
		    return 4, phenoHeader4, headerFootnote, genotypeFootnote

	    if not isHumanGeneAnnotated:
	        return 3, phenoHeader3a, headerFootnote, genotypeFootnote

	    if isHumanOrthologAnnotated:
		omim = humanToOMIM[orthologKey]
	        # human ortholog is annotated to Term
		if termID in omim:
		    header = phenoHeader1 % (orthologSymbol)
		    genotypeFootnote = genotypeFootnote1 % (orthologSymbol) + genotypeFootnote
		    return 1, header, headerFootnote, genotypeFootnote
	        # human ortholog is not annotated to Term (but is to other Terms)
		else:
		    header = phenoHeader2a % (orthologSymbol)
		    return 2, header, headerFootnote, genotypeFootnote

	    # human ortholog does not have annotations
	    else:
		header = phenoHeader2a % (orthologSymbol)
	        return 2, header, headerFootnote, genotypeFootnote

	#
	#  2. distinct etiology
	#	a. mouse genotype is annotated to Term and is a IS annotation
	#	b. mouse marker has no human ortholog
	#	c. every human gene annotated to Term has a mouse ortholog
	#
	#  3. unresolved/unknown etiology
	#	a. mouse genotype is annotated to Term and is a IS annotation
	#	b. mouse marker has no human ortholog (implies no structural gene)
	#	c. at least one human gene is annotated to Term and has no mouse ortholog
	#   OR
	#	a. mouse genotype is annotated to Term and IS annotation
	#	b. no human gene is annotated to Term
	#	

	else:
	    if isHumanGeneAnnotated:

		# check each human gene annotated to Term

		orthologFound = 1

		for g in OMIMToHuman[termID]:
		    # if human gene has no mouse ortholog, flag it
		    if not mouseOrtholog.has_key(g):
			orthologFound = 0

	        if orthologFound:
		    headerFootnote = headerFootnote2b % (symbol)
		    return 2, phenoHeader2b, headerFootnote, genotypeFootnote
	        else:
		    # at least one human gene has no mouse ortholog
		    return 3, phenoHeader3b, headerFootnote, genotypeFootnote

	    # human ortholog is not annotated to Term
	    else:
		return 3, phenoHeader3b, headerFootnote, genotypeFootnote

	return -1, header, headerFootnote, genotypeFootnote

def deriveCategory2(r):
	#
	# Purpose: derives the appropriate Human Disease Detail page Table 1 category for the record 
	# Returns: the category (1,2,3), -1 if no category could be determined
	# Assumes:
	# Effects:
	# Throws:
	#

	#
	#  1. Term of record r is annotated in both Mouse and Human
	#  2. Term of record r is annotated in Mouse but not Human
	#  3. Term of record r is annotated in Human but not Mouse
	#
	#  for "is" annotations only
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

	    # if annotation is an "is not", do not assign a category

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
	#
	# Purpose: 
	# Returns:
	# Assumes:
	# Effects:
	# Throws:
	#

	global humanOrtholog, mouseToOMIM, genotypeDisplay

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
	results = db.sql('select distinct o._Genotype_key, note = rtrim(nc.note) from #omimmouse1 o, MGI_Note n, MGI_NoteChunk nc ' + \
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
	#
	# Purpose:
	# Returns:
	# Assumes:
	# Effects:
	# Throws:
	#

        genotypeCategory3 = {}	# mouse genotype key + termID: display category 3

	#
	# for each genotype/term, cache the mininum display category 1 value
	# this will be display category 3 (human disease table 2)
	#

	results = db.sql('select * from #omimmouse6 order by _Genotype_key, termID', 'auto')

	for r in results:

	    genotype = r['_Genotype_key']
	    termID = r['termID']
	    gcKey = `genotype` + termID
	    displayCategory1, header, headerFootnote, genotypeFootnote = deriveCategory1(r)

	    if genotypeCategory3.has_key(gcKey):
		if displayCategory1 < genotypeCategory3[gcKey]:
		    genotypeCategory3[gcKey] = displayCategory1
	    else:
		genotypeCategory3[gcKey] = displayCategory1

	#
	# now process each individual marker/genotype record
	#

	results = db.sql('select * from #omimmouse6 order by _Genotype_key, alleleSymbol, term', 'auto')
	for r in results:

	    marker = r['_Marker_key']
	    genotype = r['_Genotype_key']
	    termID = r['termID']
	    mgiID = r['mgiID']
	    gcKey = `genotype` + termID

	    displayCategory1, header, headerFootnote, genotypeFootnote = deriveCategory1(r)
	    displayCategory2 = deriveCategory2(r)

	    # if the genotype belongs in section 5 (non-gene), then it's section 5 for both categories

	    if displayCategory1 == 5:
		displayCategory3 = 5
	    else:
		displayCategory3 = genotypeCategory3[gcKey]

	    if humanOrtholog.has_key(marker):
		h = humanOrtholog[marker]
		orthologOrganism = humanOrganismKey
		orthologKey = h['orthologKey']
		orthologSymbol = h['orthologSymbol']
            else:
		orthologOrganism = ''
		orthologKey = ''
		orthologSymbol = ''

	    if genotypeDisplay.has_key(gcKey):
		g1 = string.join(genotypeDisplay[genotype])
            else:
		g1 = ''

	    omimBCP.write(
		mgi_utils.prvalue(mouseOrganismKey) + DL +  \
		mgi_utils.prvalue(marker) + DL +  \
		mgi_utils.prvalue(r['_Allele_key']) + DL + \
		mgi_utils.prvalue(genotype) + DL + \
		mgi_utils.prvalue(r['_Term_key']) + DL + \
		mgi_utils.prvalue(r['_Refs_key']) + DL + \
		mgi_utils.prvalue(orthologOrganism) + DL + \
		mgi_utils.prvalue(orthologKey) + DL + \
	        mgi_utils.prvalue(displayCategory1) + DL + \
	        mgi_utils.prvalue(displayCategory2) + DL + \
	        mgi_utils.prvalue(displayCategory3) + DL + \
		mgi_utils.prvalue(r['sequenceNum']) + DL + \
		mgi_utils.prvalue(r['isNot']) + DL + \
		r['markerSymbol'] + DL + \
		r['term'] + DL + \
		r['termID'] + DL + \
		r['jnumID'] + DL + \
		r['alleleSymbol'] + DL + \
		mgi_utils.prvalue(orthologSymbol) + DL + \
		r['strain'] + DL + \
		g1 + DL + \
	        mgi_utils.prvalue(header) + DL + \
	        mgi_utils.prvalue(headerFootnote) + DL + \
	        mgi_utils.prvalue(genotypeFootnote) + DL + \
		cdate + DL + cdate + NL)

	    reviewBCP.write(
	        mgi_utils.prvalue(displayCategory1) + RDL + \
	        mgi_utils.prvalue(displayCategory2) + RDL + \
	        mgi_utils.prvalue(displayCategory3) + RDL + \
		mgi_utils.prvalue(genotype) + RDL + \
		r['markerSymbol'] + RDL + \
		r['alleleSymbol'] + RDL + \
		r['term'] + RDL + \
		r['termID'] + RDL + \
		r['jnumID'] + RDL + \
		r['strain'] + RDL + \
	        mgi_utils.prvalue(header) + RDL + \
	        mgi_utils.prvalue(headerFootnote) + RDL + \
	        mgi_utils.prvalue(genotypeFootnote) + RDL + \
		mgi_utils.prvalue(r['isNot']) + RDL)

	    if humanOrtholog.has_key(marker):
		h = humanOrtholog[marker]
	        reviewBCP.write(h['orthologSymbol'])
	    else:
	        reviewBCP.write(RDL)

	    reviewBCP.write(NL)

def selectHuman():
	#
	# Purpose:
	# Returns:
	# Assumes:
	# Effects:
	# Throws:
	#

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
	#
	# Purpose:
	# Returns:
	# Assumes:
	# Effects:
	# Throws:
	#

	results = db.sql('select * from #omimhuman3 order by markerSymbol, term', 'auto')

	for r in results:

	    marker = r['_Marker_key']
	    displayCategory1 = -1
	    displayCategory2 = deriveCategory2(r)
	    displayCategory3 = -1

	    if mouseOrtholog.has_key(marker):
		h = mouseOrtholog[marker]
		orthologOrganism = mouseOrganismKey
		orthologKey = h['orthologKey']
		orthologSymbol = h['orthologSymbol']
            else:
		orthologOrganism = ''
		orthologKey = ''
		orthologSymbol = ''

	    # don't really have to write these out because the mouse counterpart is already there

#	    if displayCategory2 == 1:
#		continue

	    omimBCP.write(
		mgi_utils.prvalue(humanOrganismKey) + DL +  \
		mgi_utils.prvalue(marker) + DL +  \
		DL + \
		DL + \
		mgi_utils.prvalue(r['_Term_key']) + DL + \
		mgi_utils.prvalue(r['_Refs_key']) + DL + \
		mgi_utils.prvalue(orthologOrganism) + DL + \
		mgi_utils.prvalue(orthologKey) + DL + \
	        mgi_utils.prvalue(displayCategory1) + DL + \
	        mgi_utils.prvalue(displayCategory2) + DL + \
	        mgi_utils.prvalue(displayCategory3) + DL + \
		DL + \
		mgi_utils.prvalue(r['isNot']) + DL + \
		r['markerSymbol'] + DL + \
		r['term'] + DL + \
		r['termID'] + DL + \
		r['jnumID'] + DL + \
		DL + \
		mgi_utils.prvalue(orthologSymbol) + DL + \
		DL + \
	        DL + \
	        DL + \
		DL + \
		DL + \
		cdate + DL + cdate + NL)

	    reviewBCP.write(
	        mgi_utils.prvalue(displayCategory1) + RDL + \
	        mgi_utils.prvalue(displayCategory2) + RDL + \
	        mgi_utils.prvalue(displayCategory3) + RDL + \
		r['markerSymbol'] + RDL + \
		RDL + \
		mgi_utils.prvalue(r['term']) + RDL + \
		r['termID'] + RDL + \
		mgi_utils.prvalue(r['jnumID']) + RDL + \
		mgi_utils.prvalue(r['isNot']) + RDL)

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

selectMouse()
selectHuman()
printMouse()
printHuman()

omimBCP.close()
reviewBCP.close()
db.useOneConnection(0)

print '%s' % mgi_utils.date()

