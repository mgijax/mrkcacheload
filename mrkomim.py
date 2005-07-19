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
import getopt
import string
import regsub
import db
import mgi_utils

NL = '\n'
RDL = '\t'

omimBCP = None
reviewBCP = None

cdate = mgi_utils.date("%m/%d/%Y")

try:
    BCPDL = os.environ['FIELDDELIM']
    table = os.environ['TABLE']
    createdBy = os.environ['CREATEDBY']
    outDir = os.environ['MRKCACHEBCPDIR']
except:
    table = 'MRK_OMIM_Cache'

mouseOMIMannotationKey = 1005
humanOMIMannotationKey = 1006
mouseOrganismKey = 1
humanOrganismKey = 2

humanOrtholog = {}	# mouse marker key : human ortholog (key, symbol)
mouseOrtholog = {}	# human marker key : mouse ortholog (key, symbol)
genotypeOrtholog = {}   # genotype key : list of human orthlogs

humanToOMIM = {}	# human marker key : OMIM term id
mouseToOMIM = {}	# mouse marker key : OMIM term id
OMIMToHuman = {}	# OMIM term id : list of human marker keys

genotypeDisplay = {}	# mouse genotype key: genotype display
genotypeCategory3 = {}	# mouse genotype key + termID: display category 3

gene = 1

#
# Headers on Phenotype Detail Page
#
phenoHeader1 = 'Models with phenotypic similarity to human diseases associated with %s'
phenoHeader2a = 'Models with phenotypic similarity to human diseases not associated with %s'
phenoHeader2b = 'Models with phenotypic similarity to human diseases having known causal genes with established orthologs'
phenoHeader3a = 'Models with phenotypic similarity to human diseases with unknown etiology'
phenoHeader3b = 'Models with phenotypic similarity to human diseases having known causal genes without established mouse orthologs, or diseases with unknown human etiology. '
phenoHeader4 = 'Models involving transgenes or other mutation types'
phenoHeader5 = 'No similarity to the expected human disease phenotype was found'

#
# Header Footnotes on Phenotype Detail Page
#
headerFootnote2b = 'The human diseases are associated with human genes, but %s is not known to be an ortholog of any of them.'
headerFootnote4 = 'Models which involve transgenes or other mutation types may appear in other sections of the table.'
headerFootnote5 = 'One or more human genes may be associated with the human disease.  The mouse genotype may involve mutations in orthologous genes, but the phenotype does not resemble the human disease.'

#
# Genotype Footnotes on Phenotype Detail Page
#
genotypeFootnote1 = '%s is associated with this disease in humans.'
genotypeFootnote2 = '%s are associated with this disease in humans.'

deleteSQL = 'delete from MRK_OMIM_Cache where _Genotype_key = %s'
insertSQL = 'insert into MRK_OMIM_Cache values (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,"%s","%s","%s","%s","%s","%s","%s","%s","%s","%s","%s","%s","%s")'

def showUsage():
	'''
	#
	# Purpose: Displays the correct usage of this program and exits
	#
	'''
 
	usage = 'usage: %s\n' % sys.argv[0] + \
		'-S server\n' + \
		'-D database\n' + \
		'-U user\n' + \
		'-P password file\n' + \
		'-K object key\n'

	sys.stderr.write(usage)
	sys.exit(1)
 
def deriveCategory1(r):
	#
	# Purpose: derives the appropriate Phenotype Detail page category for the record 
	#          and hence the appropriate Human Disease Detai page, table 2
	# Returns: the category (1,2,3,4,5), -1 if no category could be determined
	#          the phenotype header
	#          the phenotype header footnote
	#          the genotype footnote
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
	genotypeOrthologToPrint = []

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

	    # list of human orthologs for this genotype that don't include this marker's ortholog
	    for s in genotypeOrtholog[genotype]:
		if s != orthologSymbol:
		    genotypeOrthologToPrint.append(s)

	else:
            isHumanOrthologAnnotated = 0

	# check if any human gene is annotated to Term
        isHumanGeneAnnotated = OMIMToHuman.has_key(termID)

	#
	#  4. non-gene
	#	a. mouse genotype is annotated to Term and is a IS annotation
	#	b. marker type != "Gene"
	#

	if markerType != gene:
	    return 4, phenoHeader4, headerFootnote4, genotypeFootnote

	#
	#  5.  no similarity
	#	a. mouse genotype is annotated to Term and is a IS NOT annotation
	#

        elif r['isNot'] == 1:
	    if hasOrtholog and isHumanOrthologAnnotated:
		omim = humanToOMIM[orthologKey]
	        # human ortholog is annotated to Term
		if termID in omim:
		    if len(genotypeOrtholog[genotype]) == 1:
		        genotypeFootnote = genotypeFootnote1 % (genotypeOrtholog[genotype][0]) + genotypeFootnote
                    else:
		        genotypeFootnote = genotypeFootnote2 % (string.join(genotypeOrtholog[genotype], ',')) + genotypeFootnote
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

	elif hasOrtholog:

	    if not isHumanGeneAnnotated:
	        return 3, phenoHeader3a, headerFootnote, genotypeFootnote

	    if isHumanOrthologAnnotated:
		omim = humanToOMIM[orthologKey]
	        # human ortholog is annotated to Term
		if termID in omim:
		    header = phenoHeader1 % (orthologSymbol)
		    if len(genotypeOrtholog[genotype]) == 1:
		        genotypeFootnote = genotypeFootnote1 % (genotypeOrtholog[genotype][0]) + genotypeFootnote
                    else:
		        if len(genotypeOrthologToPrint) == 1:
		            genotypeFootnote = genotypeFootnote1 % (string.join(genotypeOrthologToPrint, ',')) + genotypeFootnote 
			else: 
			    genotypeFootnote = genotypeFootnote2 % (string.join(genotypeOrthologToPrint, ',')) + genotypeFootnote
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

            if r['isNot'] == 1:
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
	# Purpose:  selects the appropriate mouse gentoype data
	# Returns:
	# Assumes:  temp table omimmouse1 has already been created
	# Effects:  initializes global dictionaries/caches
	#	- humanOrtholog, mouseToOMIM, genotypeDisplay, genotypeOrtholog
	# Throws:
	#

	global humanOrtholog, mouseToOMIM, genotypeDisplay, genotypeOrtholog

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
	# resolve OMIM term and ID
	#
	db.sql('select o.*, t.term, termID = a.accID ' + \
		'into #omimmouse3 ' + \
		'from #omimmouse2 o, VOC_Term t, ACC_Accession a ' + \
		'where o._Term_key = t._Term_key ' + \
		'and o._Term_key = a._Object_key ' + \
		'and a._MGIType_key = 13 ' + \
		'and a.preferred = 1', None)
	db.sql('create index idx1 on #omimmouse3(_Refs_key)', None)

	#
	# cache all terms annotated to mouse markers
	#
	results = db.sql('select distinct o._Marker_key, o.termID from #omimmouse3 o order by o._Marker_key', 'auto')
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
		'into #omimmouse4 ' + \
		'from #omimmouse3 o, ACC_Accession a ' + \
		'where o._Refs_key = a._Object_key ' + \
		'and a._MGIType_key = 1 ' + \
		'and a._LogicalDB_key = 1 ' + \
		'and a.prefixPart = "J:" ' + \
		'and a.preferred = 1', None)
	db.sql('create index idx1 on #omimmouse4(_Genotype_key)', None)

	#
	# resolve genotype Strain
	#
	db.sql('select o.*, s.strain ' + \
		'into #omimmouse5 ' + \
		'from #omimmouse4 o, GXD_Genotype g, PRB_Strain s ' + \
		'where o._Genotype_key = g._Genotype_key ' + \
		'and g._Strain_key = s._Strain_key', None)
	db.sql('create index idx1 on #omimmouse5(_Allele_key)', None)

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
	db.sql('select distinct o._Marker_key, orthologKey = h2._Marker_key, orthologSymbol = m2.symbol ' + \
		'into #ortholog ' + \
        	'from #omimmouse1 o, HMD_Homology r1, HMD_Homology_Marker h1, ' + \
        	'HMD_Homology r2, HMD_Homology_Marker h2, ' + \
        	'MRK_Marker m2 ' + \
        	'where o._Marker_key = h1._Marker_key ' + \
        	'and h1._Homology_key = r1._Homology_key ' + \
        	'and r1._Class_key = r2._Class_key ' + \
        	'and r2._Homology_key = h2._Homology_key ' + \
        	'and h2._Marker_key = m2._Marker_key ' + \
        	'and m2._Organism_key = %s' % (humanOrganismKey), None)
	db.sql('create index idx1 on #ortholog(_Marker_key)', None)

	results = db.sql('select * from #ortholog', 'auto')
	for r in results:
	    key = r['_Marker_key']
	    value = r
	    humanOrtholog[key] = value

	#
	# resolve genotype-to-orthologs
	#
        results = db.sql('select distinct g._Genotype_key, o.orthologSymbol ' + \
		'from #omimmouse1 g, #ortholog o ' + \
		'where g._Marker_key = o._Marker_key', 'auto')
	for r in results:
	    key = r['_Genotype_key']
	    value = r['orthologSymbol']
	    if not genotypeOrtholog.has_key(key):
		genotypeOrtholog[key] = []
	    genotypeOrtholog[key].append(value)

def cacheGenotypeDisplay3():
	#
	# Purpose:  initializes global dictionary of genotype/terms and minimum display category 1 values
	# Returns:
	# Assumes:  temp table #omimmouse5 exists
	# Effects:  initializes global genotypeCategory3 dictionary
	# Throws:
	#

	global genotypeCategory3

	#
	# for each genotype/term, cache the mininum display category 1 value
	# this will be display category 3 (human disease table 2)
	#

	results = db.sql('select * from #omimmouse5 order by _Genotype_key, termID', 'auto')

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

def processMouse(processType):
	#
	# Purpose:  process Mouse records either by bcp or sql
	# Returns:
	# Assumes:
	# Effects:  if processType = bcp, then writes records to bcp file
	# Effects:  if processType = sql, then executes in-line SQL insert commands
	# Throws:
	#

	#
	# process each individual marker/genotype record
	#

	results = db.sql('select * from #omimmouse5 order by _Genotype_key, alleleSymbol, term', 'auto')

	for r in results:

	    marker = r['_Marker_key']
	    genotype = r['_Genotype_key']
	    termID = r['termID']
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
		if processType == 'bcp':
		    orthologOrganism = ''
		    orthologKey = ''
		    orthologSymbol = ''
		else:
		    orthologOrganism = 'NULL'
		    orthologKey = 'NULL'
		    orthologSymbol = 'NULL'

	    if processType == 'bcp':

                omimBCP.write(
	            mgi_utils.prvalue(mouseOrganismKey) + BCPDL +  \
	            mgi_utils.prvalue(r['_Marker_key']) + BCPDL +  \
	            mgi_utils.prvalue(r['_Marker_Type_key']) + BCPDL +  \
	            mgi_utils.prvalue(r['_Allele_key']) + BCPDL + \
	            mgi_utils.prvalue(r['_Genotype_key']) + BCPDL + \
	            mgi_utils.prvalue(r['_Term_key']) + BCPDL + \
	            mgi_utils.prvalue(r['_Refs_key']) + BCPDL + \
	            mgi_utils.prvalue(orthologOrganism) + BCPDL + \
	            mgi_utils.prvalue(orthologKey) + BCPDL + \
                    mgi_utils.prvalue(displayCategory1) + BCPDL + \
                    mgi_utils.prvalue(displayCategory2) + BCPDL + \
                    mgi_utils.prvalue(displayCategory3) + BCPDL + \
	            mgi_utils.prvalue(r['sequenceNum']) + BCPDL + \
	            mgi_utils.prvalue(r['isNot']) + BCPDL + \
	            r['markerSymbol'] + BCPDL + \
	            r['term'] + BCPDL + \
	            r['termID'] + BCPDL + \
	            r['jnumID'] + BCPDL + \
	            r['alleleSymbol'] + BCPDL + \
	            mgi_utils.prvalue(orthologSymbol) + BCPDL + \
	            r['strain'] + BCPDL + \
	            string.join(genotypeDisplay[genotype]) + BCPDL + \
                    mgi_utils.prvalue(header) + BCPDL + \
                    mgi_utils.prvalue(headerFootnote) + BCPDL + \
                    mgi_utils.prvalue(genotypeFootnote) + BCPDL + \
	            cdate + BCPDL + cdate + NL)

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
            
                if humanOrtholog.has_key(r['_Marker_key']):
	            h = humanOrtholog[r['_Marker_key']]
                    reviewBCP.write(h['orthologSymbol'])
                else:
                    reviewBCP.write(RDL)

                reviewBCP.write(NL)

	    elif processType == 'sql':

		db.sql(insertSQL % (
	            mgi_utils.prvalue(mouseOrganismKey), \
	            mgi_utils.prvalue(r['_Marker_key']), \
	            mgi_utils.prvalue(r['_Marker_Type_key']), \
	            mgi_utils.prvalue(r['_Allele_key']), \
	            mgi_utils.prvalue(r['_Genotype_key']), \
	            mgi_utils.prvalue(r['_Term_key']), \
	            mgi_utils.prvalue(r['_Refs_key']), \
	            mgi_utils.prvalue(orthologOrganism), \
	            mgi_utils.prvalue(orthologKey), \
                    mgi_utils.prvalue(displayCategory1), \
                    mgi_utils.prvalue(displayCategory2), \
                    mgi_utils.prvalue(displayCategory3), \
	            mgi_utils.prvalue(r['sequenceNum']), \
	            mgi_utils.prvalue(r['isNot']), \
	            r['markerSymbol'], \
	            r['term'], \
	            r['termID'], \
	            r['jnumID'], \
	            r['alleleSymbol'], \
	            mgi_utils.prvalue(orthologSymbol), \
	            r['strain'], \
	            string.join(genotypeDisplay[genotype]), \
                    mgi_utils.prvalue(header), \
                    mgi_utils.prvalue(headerFootnote), \
                    mgi_utils.prvalue(genotypeFootnote), \
	            cdate , cdate), None)

def selectHuman(byOrtholog = 0):
	#
	# Purpose:  selects the appropriate human annotation data
	# Returns:
	# Assumes:
	# Effects:  initializes global dictionaries: mouseOrtholog, humanToOMIM, OMIMToHuman
	# Throws:
	#

	global mouseOrtholog, humanToOMIM, OMIMToHuman

	if byOrtholog == 1:

	    #
	    # select all human genes w/ mouse orthologs annotated to OMIM Gene or Disease Terms
	    # this statement is used if we are processing a specfic Marker, Allele or Genotype
	    # data set so that only those human markers that are orthologs to the data set specified
	    # are selected.
	    #

	    db.sql('select _Marker_key = a._Object_key, termID = ac.accID, a._Term_key, t.term, a.isNot, e._Refs_key ' + \
		    'into #omimhuman1 ' + \
		    'from #ortholog o, VOC_Annot a, VOC_Evidence e, VOC_Term t, ACC_Accession ac ' + \
		    'where a._AnnotType_key = %s ' % (humanOMIMannotationKey) + \
		    'and a._Object_key = o.orthologKey ' + \
		    'and a._Annot_key = e._Annot_key ' + \
		    'and a._Term_key = t._Term_key ' + \
		    'and a._Term_key = ac._Object_key ' + \
		    'and ac._MGIType_key = 13 ' + \
		    'and ac.preferred = 1', None)
	    db.sql('create index idx1 on #omimhuman1(_Marker_key)', None)

	else:
	
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
	db.sql('select o.*, m._Organism_key, markerSymbol = m.symbol, m._Marker_Type_key ' + \
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

def processHuman():
	#
	# Purpose:  process Human records
	# Returns:
	# Assumes:
	# Effects:  writes records to bcp file
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
		mgi_utils.prvalue(humanOrganismKey) + BCPDL +  \
		mgi_utils.prvalue(marker) + BCPDL +  \
	        mgi_utils.prvalue(r['_Marker_Type_key']) + BCPDL +  \
		BCPDL + \
		BCPDL + \
		mgi_utils.prvalue(r['_Term_key']) + BCPDL + \
		mgi_utils.prvalue(r['_Refs_key']) + BCPDL + \
		mgi_utils.prvalue(orthologOrganism) + BCPDL + \
		mgi_utils.prvalue(orthologKey) + BCPDL + \
	        mgi_utils.prvalue(displayCategory1) + BCPDL + \
	        mgi_utils.prvalue(displayCategory2) + BCPDL + \
	        mgi_utils.prvalue(displayCategory3) + BCPDL + \
		BCPDL + \
		mgi_utils.prvalue(r['isNot']) + BCPDL + \
		r['markerSymbol'] + BCPDL + \
		r['term'] + BCPDL + \
		r['termID'] + BCPDL + \
		r['jnumID'] + BCPDL + \
		BCPDL + \
		mgi_utils.prvalue(orthologSymbol) + BCPDL + \
		BCPDL + \
	        BCPDL + \
	        BCPDL + \
		BCPDL + \
		BCPDL + \
		cdate + BCPDL + cdate + NL)

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

def processDeleteReload():
	#
	# Purpose:  processes data for BCP-type processing; aka delete/reload
	# Returns:
	# Assumes:
	# Effects:  initializes global file pointers:  omimBCP, reviewBCP
	# Throws:
	#

	global omimBCP, reviewBCP

	print '%s' % mgi_utils.date()

	omimBCP = open(outDir + '/' + table + '.bcp', 'w')
	reviewBCP = open(outDir + '/OMIM_Cache_Review.tab', 'w')

	#
	# select all mouse genotypes annotated to OMIM Disease Terms
	#

	db.sql('select g._Marker_key, g._Allele_key, g._Genotype_key, g.sequenceNum, ' + \
		'a._Term_key, a.isNot, e._Refs_key ' + \
		'into #omimmouse1 ' + \
		'from GXD_AlleleGenotype g, VOC_Annot a, VOC_Evidence e ' + \
		'where g._Genotype_key = a._Object_key ' + \
		'and a._AnnotType_key = %s ' % (mouseOMIMannotationKey) + \
		'and a._Annot_key = e._Annot_key\n', None)

	selectMouse()
	selectHuman()
	cacheGenotypeDisplay3()
	processMouse('bcp')
	processHuman()
	omimBCP.close()
	reviewBCP.close()

	print '%s' % mgi_utils.date()

def processByAllele(alleleKey):
	#
	# Purpose:  processes data for a specific Allele
	# Returns:
	# Assumes:
	# Effects:
	# Throws:
	#

	#
	# select all Genotypes of specified Allele
	#

	db.sql('select distinct g._Genotype_key into #toprocess ' + \
		'from GXD_AlleleGenotype g ' + \
		'where g._Allele_key = ' + alleleKey, None)

	db.sql('create index idx1 on #toprocess(_Genotype_key)', None)

	#
	# delete existing cache records for this allele
	#

	db.sql('delete %s from #toprocess p, %s g where p._Genotype_key = g._Genotype_key' % (table, table), None)

	#
	# select all annotations for Genotypes of specified Allele
	#

	db.sql('select g._Marker_key, g._Allele_key, g._Genotype_key, g.sequenceNum, ' + \
		'a._Term_key, a.isNot, e._Refs_key ' + \
		'into #omimmouse1 ' + \
		'from #toprocess p, GXD_AlleleGenotype g, VOC_Annot a, VOC_Evidence e ' + \
		'where p._Genotype_key = g._Genotype_key ' + \
		'and g._Genotype_key = a._Object_key ' + \
		'and a._AnnotType_key = %s ' % (mouseOMIMannotationKey) + \
		'and a._Annot_key = e._Annot_key', None)

	selectMouse()
	selectHuman(byOrtholog = 1)
	cacheGenotypeDisplay3()
	processMouse('sql')

def processByGenotype(genotypeKey):
	#
	# Purpose:  processes data for a specific Genotype
	# Returns:
	# Assumes:
	# Effects:
	# Throws:
	#

	#
	# delete existing cache records for this genotype
	#

	db.sql(deleteSQL % (genotypeKey), None)

	#
	# select all annotations for given genotypeKey
	#

	db.sql('select g._Marker_key, g._Allele_key, g._Genotype_key, g.sequenceNum, ' + \
		'a._Term_key, a.isNot, e._Refs_key ' + \
		'into #omimmouse1 ' + \
		'from GXD_AlleleGenotype g, VOC_Annot a, VOC_Evidence e ' + \
		'where g._Genotype_key = a._Object_key ' + \
		'and a._AnnotType_key = %s ' % (mouseOMIMannotationKey) + \
		'and a._Annot_key = e._Annot_key ' + \
	        'and g._Genotype_key = %s' % (genotypeKey), None)

	selectMouse()
	selectHuman(byOrtholog = 1)
	cacheGenotypeDisplay3()
	processMouse('sql')

def processByMarker(markerKey):
	#
	# Purpose:  processes data for a specific Marker
	# Returns:
	# Assumes:
	# Effects:
	# Throws:
	#

	#
	# select all Genotypes of specified Marker
	#

	db.sql('select distinct g._Genotype_key into #toprocess ' + \
		'from GXD_AlleleGenotype g ' + \
		'where g._Marker_key = ' + markerKey, None)

	db.sql('create index idx1 on #toprocess(_Genotype_key)', None)

	#
	# delete existing cache records for this marker
	#

	db.sql('delete %s from #toprocess p, %s g where p._Genotype_key = g._Genotype_key' % (table, table), None)

	#
	# select all annotations for Genotypes of specified Marker
	#

	db.sql('select g._Marker_key, g._Allele_key, g._Genotype_key, g.sequenceNum, ' + \
		'a._Term_key, a.isNot, e._Refs_key ' + \
		'into #omimmouse1 ' + \
		'from #toprocess p, GXD_AlleleGenotype g, VOC_Annot a, VOC_Evidence e ' + \
		'where p._Genotype_key = g._Genotype_key ' + \
		'and g._Genotype_key = a._Object_key ' + \
		'and a._AnnotType_key = %s ' % (mouseOMIMannotationKey) + \
		'and a._Annot_key = e._Annot_key', None)

	selectMouse()
	selectHuman(byOrtholog = 1)
	cacheGenotypeDisplay3()
	processMouse('sql')

#
# Main Routine
#

try:
	optlist, args = getopt.getopt(sys.argv[1:], 'S:D:U:P:K:')
except:
	showUsage()

server = None
database = None
user = None
password = None
objectKey = None

for opt in optlist:
	if opt[0] == '-S':
		server = opt[1]
	elif opt[0] == '-D':
		database = opt[1]
	elif opt[0] == '-U':
		user = opt[1]
	elif opt[0] == '-P':
		password = string.strip(open(opt[1], 'r').readline())
	elif opt[0] == '-K':
		objectKey = opt[1]
	else:
		showUsage()

if server is None or \
   database is None or \
   user is None or \
   password is None or \
   objectKey is None:
	showUsage()

db.set_sqlLogin(user, password, server, database)
db.useOneConnection(1)
db.set_sqlLogFunction(db.sqlLogAll)

scriptName = os.path.basename(sys.argv[0])

# call functions based on the way the program is invoked

if scriptName == 'mrkomim.py':
    processDeleteReload()

# all of these invocations will only affect a certain subset of data

elif scriptName == 'mrkomimByAllele.py':
    processByAllele(objectKey)

elif scriptName == 'mrkomimByGenotype.py':
    processByGenotype(objectKey)

elif scriptName == 'mrkomimByMarker.py':
    processByMarker(objectKey)

db.useOneConnection(0)

