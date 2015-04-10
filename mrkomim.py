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
# Usage:
#	mrkomim.py -Sdbserver -Ddatabase -Uuser -Ppasswordfile -Kobjectkey
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
# alleleMouseModels is the Allele Detail Mouse Model table. (MRK_OMIM_Cache.omimCategory1)
# diseaseAssociatedGenes is the Disease Detail Associated Genes and Transgenes sections. (MRK_OMIM_Cache.omimCategory2)
# diseaseMouseModels is the Disease Detail Mouse Model table. (MRK_OMIM_Cache.omimCategory3)
#
# possible values for omimCategory1, omimCategory2, omimCategory3):
#
#	-1: ignore
#	1 : Models with phenotypic similarity to human disease where etiologies involve orthologs.
#	2 : Models with phenotypic similarity to human disease where etiologies are distinct.
#	3 : Models with phenotypic similarity to human diseases where etiology is unknown or involving genes where ortholog is unknown.
#	4 : Models involving transgenes or other mutation types.
#	5 : No similarity to the expected human disease phenotype was found.
#
# TR 3853/OMIM User Requirements
#
# History
#
# 04/10/2015	sc
#	TR11886 - MandM project update to Use Hybrid Homology (MRK_Cluster* tables) rather than obsolete
#	HMD_* tables
#
# 12/11/2012	lec/kstone
#	- TR10273
#	deriveDiseaseAssociatedGenes()
#	AS OF 2012-12-05 kstone - We are suppressing the marker "Gt(ROSA)26Sor" from the 
# 	associated genes section of every disease detail per Janan
#
# 04/04/2011	lec
#	- TR10658;add _Cache_key
#
# 06/30/2006	lec
#	- TR 7728; add filter for Cre alleles to Disease/Mouse Models/Transgene section
#
# 04/06/2006	lec
#	- replaced regex with re
#
# 05/26/2005	lec
#	- TR 3853/OMIM
#
'''

import sys
import os
import getopt
import string
import re
import mgi_utils

try:
    COLDL = os.environ['COLDELIM']
    COLDL = COLDL.decode("string_escape")
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
    COLDL = '\t'
    table = 'MRK_OMIM_Cache'
    outDir = './'

LINEDL = '\n'

RDL = '\t'

omimBCP = None
reviewBCP = None

cdate = mgi_utils.date("%m/%d/%Y")

mouseOMIMannotationKey = 1005
humanOMIMannotationKey = 1006
mouseOrganismKey = 1
humanOrganismKey = 2

humanOrtholog = {}	# mouse marker key : human ortholog (key, symbol)
mouseOrtholog = {}	# human marker key : mouse ortholog (key, symbol)
genotypeOrtholog = {}   # genotype key : list of human marker keys:symbols

humanToOMIM = {}	# human marker key : OMIM term id
mouseToOMIM = {}	# mouse marker key : OMIM term id
mouseIsNot = {}		# mouse marker key : OMIM term id that are "NOT" annotations
OMIMToHuman = {}	# OMIM term id : list of human marker keys

genotypeDisplay = {}	# mouse genotype key: genotype display
genotypeAlleleMouseModels = {}	# mouse genotype key + termID: display category 3

gene = 1
notQualifier = []

nextMaxKey = 0		# max(_Cache_key)

crepattern = re.compile(".*\(.*[Cc]re.*\).*")

#
# Headers on Phenotype Detail Page
#
phenoHeader1 = 'Models with phenotypic similarity to human diseases associated with human %s.'
phenoHeader2a = 'Models with phenotypic similarity to human diseases not associated with human %s.'
phenoHeader2b = 'Models with phenotypic similarity to human diseases associated with genes having mouse orthologs.'
phenoHeader3a = 'Models with phenotypic similarity to human diseases with unknown etiology.'
phenoHeader3b = 'Models with phenotypic similarity to human diseases associated with genes lacking mouse orthologs, or diseases of unknown human etiology.'
phenoHeader4 = 'Models involving transgenes or other mutation types.'
phenoHeader5 = 'No similarity to expected human disease phenotype was found.'

#
# Header Footnotes on Phenotype Detail Page
#
headerFootnote2b = 'The human diseases are associated with human genes, but %s is not known to be an ortholog of any of them.'
headerFootnote4 = 'Models involving transgenes or other mutation types may also appear in other sections of the table.'
headerFootnote5 = 'One or more human genes may be associated with the human disease.  The mouse genotype may involve mutations in orthologous genes, but the phenotype did not resemble the human disease.'

#
# Genotype Footnotes on Phenotype Detail Page
#
genotypeFootnote1 = '%s is associated with this disease in humans.'
genotypeFootnote2 = '%s are associated with this disease in humans.'

deleteSQL = 'delete from MRK_OMIM_Cache where _Genotype_key = %s'
insertSQL = 'insert into MRK_OMIM_Cache values (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,"%s","%s","%s","%s","%s","%s","%s","%s",%s,"%s",%s,%s,"%s","%s")'

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
 
def deriveAlleleDetailMouseModels(r):
	#
	# Purpose: derives the appropriate Phenotype Detail page category for the record 
	#          and hence the appropriate Human Disease Detail page (Mouse Models)
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
	alleleSymbol = r['alleleSymbol']
	hasOrtholog = 0
	header = ''
	headerFootnote = ''
	genotypeFootnote = ''
	genotypeOrthologToPrint = []
	orthologKey = 0

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

	# list of human orthologs for this genotype that don't include this marker's ortholog
	# but are annotated to this term

	if genotypeOrtholog.has_key(genotype):
	    for s in genotypeOrtholog[genotype]:
	        if s['orthologKey'] != orthologKey:
	            if humanToOMIM.has_key(s['orthologKey']):
	                omim = humanToOMIM[s['orthologKey']]
	                # human ortholog is annotated to Term
	                if termID in omim:
	                    genotypeOrthologToPrint.append(s['orthologSymbol'])

	# check if any human gene is annotated to Term
        isHumanGeneAnnotated = OMIMToHuman.has_key(termID)

	#
	#  5.  no similarity
	#	a. mouse genotype is annotated to Term and is a NOT annotation
	#

        if r['_Qualifier_key'] in notQualifier:
	    if hasOrtholog and isHumanOrthologAnnotated:
		omim = humanToOMIM[orthologKey]
	        # human ortholog is annotated to Term
		if termID in omim:
		    if len(genotypeOrthologToPrint) == 1:
		        genotypeFootnote = genotypeFootnote1 % (string.join(genotypeOrthologToPrint, ',')) + genotypeFootnote 
                    elif len(genotypeOrthologToPrint) >= 1:
		        genotypeFootnote = genotypeFootnote2 % (string.join(genotypeOrthologToPrint, ',')) + genotypeFootnote
	    return 5, phenoHeader5, headerFootnote5, genotypeFootnote

	#
	#  4. non-gene
	#	a. mouse genotype is annotated to Term and is a IS annotation
	#	b. marker type != "Gene"
	#

	elif markerType != gene:
	    return 4, phenoHeader4, headerFootnote4, genotypeFootnote

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

	    # display footnote in all cases

	    if len(genotypeOrtholog[genotype]) != 1:
	        if len(genotypeOrthologToPrint) == 1:
	            genotypeFootnote = genotypeFootnote1 % (string.join(genotypeOrthologToPrint, ',')) + genotypeFootnote 
                elif len(genotypeOrthologToPrint) >= 1:
	            genotypeFootnote = genotypeFootnote2 % (string.join(genotypeOrthologToPrint, ',')) + genotypeFootnote

	    if not isHumanGeneAnnotated:
	        return 3, phenoHeader3a, headerFootnote, genotypeFootnote

	    if isHumanOrthologAnnotated:
		omim = humanToOMIM[orthologKey]
	        # human ortholog is annotated to Term
		if termID in omim:
		    header = phenoHeader1 % (orthologSymbol)
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

def deriveDiseaseAssociatedGenes(r):
	#
	# Purpose: derives the appropriate Human Disease Detail page Table 1 (Associated Genes)
	#          category for the record 
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

	# AS OF 2012-12-05 kstone - We are suppressing the marker "Gt(ROSA)26Sor" from the 
	# associated genes section of every disease detail per Janan
	if marker==37270:
		return -1

	#
	# process mouse reocrd
	#

	if organism == mouseOrganismKey:

	    #
	    # Cre alleles should not appear in this table
	    #
	    m = crepattern.match(r['alleleSymbol'])
	    if m is not None:
		return -1

	    #
	    # mouse NOTs appear nowhere...
	    #

            if r['_Qualifier_key'] in notQualifier:
		return -1

	    if humanOrtholog.has_key(marker):
	        hasOrtholog = 1
	        ortholog = humanOrtholog[marker]
	        orthologKey = ortholog['orthologKey']
	        orthologSymbol = ortholog['orthologSymbol']

	    if hasOrtholog:
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

		#
		# if the corresponding mouse ortholog annotation is a NOT, then it can only be in category 3
		#

		isNot = 0
		if mouseIsNot.has_key(orthologKey):
		    omim = mouseIsNot[orthologKey]
		    if termID in omim:
			isNot = 1

	        if mouseToOMIM.has_key(orthologKey):
		    omim = mouseToOMIM[orthologKey]
		    if termID in omim and isNot == 0:
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
	#	- humanOrtholog, mouseToOMIM, genotypeDisplay, genotypeOrtholog, mouseIsNot
	# Throws:
	#

	global humanOrtholog, mouseToOMIM, genotypeDisplay, genotypeOrtholog, mouseIsNot

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
	db.sql('create index idx3 on #omimmouse2(_Marker_key)', None)

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
	db.sql('create index idx4 on #omimmouse3(_Refs_key)', None)

	#
	# cache all terms annotated to mouse markers
	#
	mouseIs = {}
	results = db.sql('select distinct o._Marker_key, o.termID, o.qualifier, o._Qualifier_key ' + \
	     'from #omimmouse3 o order by o._Marker_key, o.termID, o.qualifier', 'auto')

	for r in results:

	    key = r['_Marker_key']
	    value = r['termID']

	    if not mouseToOMIM.has_key(key):
		mouseToOMIM[key] = []
	    mouseToOMIM[key].append(value)

	    if r['_Qualifier_key'] not in notQualifier:
	        if not mouseIs.has_key(key):
		    mouseIs[key] = []
		mouseIs[key].append(value)

	    # specifically cache the "NOT" annotations; 
	    # only if the "NOT" is the *only* annotation for this term
	    if r['_Qualifier_key'] in notQualifier:
		if mouseIs.has_key(key):
		    if value not in mouseIs[key]:
	                if not mouseIsNot.has_key(key):
		            mouseIsNot[key] = []
			mouseIsNot[key].append(value)
		else:
	            if not mouseIsNot.has_key(key):
		        mouseIsNot[key] = []
		    mouseIsNot[key].append(value)

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
	db.sql('create index idx5 on #omimmouse4(_Genotype_key)', None)

	#
	# resolve genotype Strain
	#
	db.sql('select o.*, s.strain ' + \
		'into #omimmouse5 ' + \
		'from #omimmouse4 o, GXD_Genotype g, PRB_Strain s ' + \
		'where o._Genotype_key = g._Genotype_key ' + \
		'and g._Strain_key = s._Strain_key', None)
	db.sql('create index idx6 on #omimmouse5(_Allele_key)', None)

	#
	# resolve genotype display
	#
	results = db.sql('select distinct o._Genotype_key, rtrim(nc.note) as note, nc.sequencenum from #omimmouse1 o, MGI_Note n, MGI_NoteChunk nc ' + \
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
	db.sql('''select distinct o._Marker_key, cm2._Marker_key as orthologKey,  
		    m2.symbol as orthologSymbol
                into #orthologHuman
                from #omimmouse1 o, MRK_Cluster c1, MRK_ClusterMember cm1, MRK_Cluster c2, 
		    MRK_ClusterMember cm2, MRK_Marker m2 
                where o._Marker_key = cm1._Marker_key
                and cm1._Cluster_key = c1._Cluster_key
                and c1._ClusterType_key = 9272150
                and c1._ClusterSource_key = 13764519
                and c1._Cluster_key = c2._Cluster_key
                and c2._Cluster_key = cm2._Cluster_key
                and cm2._Marker_key = m2._Marker_key 
                and m2._Organism_key = %s''' % (humanOrganismKey), None)
	db.sql('create index idx7 on #orthologHuman(_Marker_key)', None)

	results = db.sql('select * from #orthologHuman', 'auto')
	for r in results:
	    key = r['_Marker_key']
	    value = r
	    humanOrtholog[key] = value

	#
	# resolve genotype-to-orthologs
	#
        results = db.sql('select distinct g._Genotype_key, o.orthologKey, o.orthologSymbol ' + \
		'from #omimmouse1 g, #orthologHuman o ' + \
		'where g._Marker_key = o._Marker_key', 'auto')
	for r in results:
	    key = r['_Genotype_key']
	    value = r
	    if not genotypeOrtholog.has_key(key):
		genotypeOrtholog[key] = []
	    genotypeOrtholog[key].append(value)

def cacheGenotypeDisplay3():
	#
	# Purpose:  initializes global dictionary of genotype/terms and minimum display category 1 values
	# Returns:
	# Assumes:  temp table #omimmouse5 exists
	# Effects:  initializes global genotypeAlleleMouseModels dictionary
	# Throws:
	#

	global genotypeAlleleMouseModels

	#
	# for each genotype/term, cache the mininum display category 1 value
	# this will be display category 3 (human disease table 2 (Mouse Models))
	#

	results = db.sql('select * from #omimmouse5 order by _Genotype_key, termID', 'auto')

	for r in results:

	    genotype = r['_Genotype_key']
	    termID = r['termID']
	    gcKey = `genotype` + termID
	    alleleDetailMouseModels, header, headerFootnote, genotypeFootnote = deriveAlleleDetailMouseModels(r)

	    if genotypeAlleleMouseModels.has_key(gcKey):
		if alleleDetailMouseModels < genotypeAlleleMouseModels[gcKey]:
		    genotypeAlleleMouseModels[gcKey] = alleleDetailMouseModels
	    else:
		genotypeAlleleMouseModels[gcKey] = alleleDetailMouseModels

def processMouse(processType):
	#
	# Purpose:  process Mouse records either by bcp or sql
	# Returns:
	# Assumes:
	# Effects:  if processType = bcp, then writes records to bcp file
	# Effects:  if processType = sql, then executes in-line SQL insert commands
	# Throws:
	#

	global nextMaxKey

	#
	# process each individual marker/genotype record
	#

	results = db.sql('select * from #omimmouse5 order by _Genotype_key, alleleSymbol, term', 'auto')

	for r in results:

	    marker = r['_Marker_key']
	    genotype = r['_Genotype_key']
	    termID = r['termID']
	    gcKey = `genotype` + termID

	    alleleDetailMouseModels, header, headerFootnote, genotypeFootnote = deriveAlleleDetailMouseModels(r)
	    diseaseAssociatedGenes = deriveDiseaseAssociatedGenes(r)

	    nextMaxKey = nextMaxKey + 1

	    # If non-gene (transgenes, other mutations)...
	    # then Cre alleles should not appear in the Mouse Model/Transgene section on the Disease page
	    # However, Cre alleles will, by default, appear in the Mouse Model section on the Allele page,
	    # and the front-end must apply another filter to exclude Cre alleles (when appropriate)
	    # from these Allele page.

	    if alleleDetailMouseModels == 4:
	        m = crepattern.match(r['alleleSymbol'])
	        if m is not None:
		    diseaseMouseModels = -1
                else:
		    diseaseMouseModels = 4
	    else:
		diseaseMouseModels = genotypeAlleleMouseModels[gcKey]

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
		    orthologOrganism = None
		    orthologKey =  None
		    orthologSymbol =  None

	    if not genotypeDisplay.has_key(genotype):
		fullGenotypeDisplay = ''
	    else:
		fullGenotypeDisplay = string.join(genotypeDisplay[genotype], '')

	    if len(fullGenotypeDisplay) > 255:
	        genotypeDisplay1 = fullGenotypeDisplay[:255]
	        genotypeDisplay2 = fullGenotypeDisplay[255:]
	    else:
	        genotypeDisplay1 = fullGenotypeDisplay
		genotypeDisplay2 = ''

	    if processType == 'bcp':
		if os.environ['DB_TYPE'] == 'postgres':
		    genotypeDisplay1 = genotypeDisplay1.replace('\n','\\n')
		    genotypeDisplay2 = genotypeDisplay2.replace('\n','\\n')

                omimBCP.write(
	            str(nextMaxKey) + COLDL +  \
	            mgi_utils.prvalue(mouseOrganismKey) + COLDL +  \
	            mgi_utils.prvalue(r['_Marker_key']) + COLDL +  \
	            mgi_utils.prvalue(r['_Marker_Type_key']) + COLDL +  \
	            mgi_utils.prvalue(r['_Allele_key']) + COLDL + \
	            mgi_utils.prvalue(r['_Genotype_key']) + COLDL + \
	            mgi_utils.prvalue(r['_Term_key']) + COLDL + \
	            mgi_utils.prvalue(r['_Refs_key']) + COLDL + \
	            mgi_utils.prvalue(orthologOrganism) + COLDL + \
	            mgi_utils.prvalue(orthologKey) + COLDL + \
                    mgi_utils.prvalue(alleleDetailMouseModels) + COLDL + \
                    mgi_utils.prvalue(diseaseAssociatedGenes) + COLDL + \
                    mgi_utils.prvalue(diseaseMouseModels) + COLDL + \
	            mgi_utils.prvalue(r['sequenceNum']) + COLDL + \
	            mgi_utils.prvalue(r['qualifier']) + COLDL + \
	            r['markerSymbol'] + COLDL + \
	            r['term'] + COLDL + \
	            r['termID'] + COLDL + \
	            r['jnumID'] + COLDL + \
	            r['alleleSymbol'] + COLDL + \
	            mgi_utils.prvalue(orthologSymbol) + COLDL + \
	            r['strain'] + COLDL + \
		    genotypeDisplay1 + COLDL + \
		    genotypeDisplay2 + COLDL + \
                    mgi_utils.prvalue(header) + COLDL + \
                    mgi_utils.prvalue(headerFootnote) + COLDL + \
                    mgi_utils.prvalue(genotypeFootnote) + COLDL + \
	            cdate + COLDL + cdate + LINEDL)

                reviewBCP.write(
                    mgi_utils.prvalue(alleleDetailMouseModels) + RDL + \
                    mgi_utils.prvalue(diseaseAssociatedGenes) + RDL + \
                    mgi_utils.prvalue(diseaseMouseModels) + RDL + \
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
	            mgi_utils.prvalue(r['qualifier']) + RDL)
            
                if humanOrtholog.has_key(r['_Marker_key']):
	            h = humanOrtholog[r['_Marker_key']]
                    reviewBCP.write(h['orthologSymbol'])
                else:
                    reviewBCP.write(RDL)

                reviewBCP.write(LINEDL)

	    elif processType == 'sql':

		if genotypeDisplay2 == '':
		    printGenotypeDisplay2 = 'null'
                else:
		    printGenotypeDisplay2 = '"' + genotypeDisplay2 + '"'

		if headerFootnote == '':
		    printHeaderFootnote = 'null'
                else:
		    printHeaderFootnote = '"' + headerFootnote + '"'

		if genotypeFootnote == '':
		    printGenotypeFootnote = 'null'
                else:
		    printGenotypeFootnote = '"' + genotypeFootnote + '"'

		if r['qualifier'] is None:
		    printQualifier = 'null'
                else:
		    printQualifier = '"' + r['qualifier'] + '"'

		db.sql(insertSQL % (
	            str(nextMaxKey), \
	            mgi_utils.prvalue(mouseOrganismKey), \
	            mgi_utils.prvalue(r['_Marker_key']), \
	            mgi_utils.prvalue(r['_Marker_Type_key']), \
	            mgi_utils.prvalue(r['_Allele_key']), \
	            mgi_utils.prvalue(r['_Genotype_key']), \
	            mgi_utils.prvalue(r['_Term_key']), \
	            mgi_utils.prvalue(r['_Refs_key']), \
	            mgi_utils.value(orthologOrganism), \
	            mgi_utils.value(orthologKey), \
                    mgi_utils.prvalue(alleleDetailMouseModels), \
                    mgi_utils.prvalue(diseaseAssociatedGenes), \
                    mgi_utils.prvalue(diseaseMouseModels), \
	            mgi_utils.prvalue(r['sequenceNum']), \
	            printQualifier, \
	            r['markerSymbol'], \
	            r['term'], \
	            r['termID'], \
	            r['jnumID'], \
	            r['alleleSymbol'], \
	            mgi_utils.prvalue(orthologSymbol), \
	            r['strain'], \
	            genotypeDisplay1, \
		    printGenotypeDisplay2, \
                    mgi_utils.prvalue(header), \
                    printHeaderFootnote, \
                    printGenotypeFootnote, \
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

	    db.sql('select _Marker_key = a._Object_key, termID = ac.accID, ' + \
		    'a._Term_key, t.term, qualifier = q.term, a._Qualifier_key, e._Refs_key ' + \
		    'into #omimhuman1 ' + \
		    'from #orthologHuman o, VOC_Annot a, VOC_Evidence e, VOC_Term t, ACC_Accession ac, VOC_Term q ' + \
		    'where a._AnnotType_key = %s ' % (humanOMIMannotationKey) + \
		    'and a._Qualifier_key = q._Term_key ' + \
		    'and a._Object_key = o.orthologKey ' + \
		    'and a._Annot_key = e._Annot_key ' + \
		    'and a._Term_key = t._Term_key ' + \
		    'and a._Term_key = ac._Object_key ' + \
		    'and ac._MGIType_key = 13 ' + \
		    'and ac.preferred = 1 ' + \
		    'union ' + \
	           'select _Marker_key = a._Object_key, termID = ac.accID, ' + \
		    'a._Term_key, t.term, qualifier = q.term, a._Qualifier_key, e._Refs_key ' + \
		    'from #omimmouse3 o, VOC_Annot a, VOC_Evidence e, VOC_Term t, ACC_Accession ac, VOC_Term q ' + \
		    'where a._AnnotType_key = %s ' % (humanOMIMannotationKey) + \
		    'and a._Qualifier_key = q._Term_key ' + \
		    'and a._Term_key = o._Term_key ' + \
		    'and a._Annot_key = e._Annot_key ' + \
		    'and a._Term_key = t._Term_key ' + \
		    'and a._Term_key = ac._Object_key ' + \
		    'and ac._MGIType_key = 13 ' + \
		    'and ac.preferred = 1', None)
	    db.sql('create index idx8 on #omimhuman1(_Marker_key)', None)

	else:
	
	    #
	    # select all human genes annotated to OMIM Gene or Disease Terms
	    #

	    db.sql('select _Marker_key = a._Object_key, termID = ac.accID, ' + \
		    'a._Term_key, t.term, qualifier = q.term, a._Qualifier_key, e._Refs_key ' + \
		    'into #omimhuman1 ' + \
		    'from VOC_Annot a, VOC_Evidence e, VOC_Term t, ACC_Accession ac, VOC_Term q ' + \
		    'where a._AnnotType_key = %s ' % (humanOMIMannotationKey) + \
		    'and a._Qualifier_key = q._Term_key ' + \
		    'and a._Annot_key = e._Annot_key ' + \
		    'and a._Term_key = t._Term_key ' + \
		    'and a._Term_key = ac._Object_key ' + \
		    'and ac._MGIType_key = 13 ' + \
		    'and ac.preferred = 1', None)
	    db.sql('create index idx9 on #omimhuman1(_Marker_key)', None)

	#
	# resolve marker symbol
	#
	db.sql('select o.*, m._Organism_key, markerSymbol = m.symbol, m._Marker_Type_key ' + \
		'into #omimhuman2 ' + \
		'from #omimhuman1 o, MRK_Marker m ' + \
		'where o._Marker_key = m._Marker_key ', None)
	db.sql('create index idx10 on #omimhuman2(_Marker_key)', None)

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
		'select o.*, null as jnumID from #omimhuman2 o where _Refs_key = -1', None)

	#
	# resolve mouse ortholog
	#
        results = db.sql('''select distinct o._Marker_key, cm2._Marker_key as orthologKey,
                    m2.symbol as orthologSymbol
                from #omimmouse1 o, MRK_Cluster c1, MRK_ClusterMember cm1, MRK_Cluster c2,
                    MRK_ClusterMember cm2, MRK_Marker m2
                where o._Marker_key = cm1._Marker_key
                and cm1._Cluster_key = c1._Cluster_key
                and c1._ClusterType_key = 9272150
                and c1._ClusterSource_key = 13764519
                and c1._Cluster_key = c2._Cluster_key
                and c2._Cluster_key = cm2._Cluster_key
                and cm2._Marker_key = m2._Marker_key
                and m2._Organism_key = %s''' % (mouseOrganismKey), 'auto')

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

	global nextMaxKey

	results = db.sql('select * from #omimhuman3 order by markerSymbol, term', 'auto')

	for r in results:

	    marker = r['_Marker_key']
	    alleleDetailMouseModels = -1
	    diseaseAssociatedGenes = deriveDiseaseAssociatedGenes(r)
	    diseaseMouseModels = -1

	    nextMaxKey = nextMaxKey + 1

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

#	    if diseaseAssociatedGenes == 1:
#		continue

	    omimBCP.write(
		str(nextMaxKey) + COLDL +  \
		mgi_utils.prvalue(humanOrganismKey) + COLDL +  \
		mgi_utils.prvalue(marker) + COLDL +  \
	        mgi_utils.prvalue(r['_Marker_Type_key']) + COLDL +  \
		COLDL + \
		COLDL + \
		mgi_utils.prvalue(r['_Term_key']) + COLDL + \
		mgi_utils.prvalue(r['_Refs_key']) + COLDL + \
		mgi_utils.prvalue(orthologOrganism) + COLDL + \
		mgi_utils.prvalue(orthologKey) + COLDL + \
	        mgi_utils.prvalue(alleleDetailMouseModels) + COLDL + \
	        mgi_utils.prvalue(diseaseAssociatedGenes) + COLDL + \
	        mgi_utils.prvalue(diseaseMouseModels) + COLDL + \
		COLDL + \
		mgi_utils.prvalue(r['qualifier']) + COLDL + \
		r['markerSymbol'] + COLDL + \
		r['term'] + COLDL + \
		r['termID'] + COLDL + \
		r['jnumID'] + COLDL + \
		COLDL + \
		mgi_utils.prvalue(orthologSymbol) + COLDL + \
		COLDL + \
	        COLDL + \
	        COLDL + \
	        COLDL + \
		COLDL + \
		COLDL + \
		cdate + COLDL + cdate + LINEDL)

	    reviewBCP.write(
	        mgi_utils.prvalue(alleleDetailMouseModels) + RDL + \
	        mgi_utils.prvalue(diseaseAssociatedGenes) + RDL + \
	        mgi_utils.prvalue(diseaseMouseModels) + RDL + \
		r['markerSymbol'] + RDL + \
		RDL + \
		mgi_utils.prvalue(r['term']) + RDL + \
		r['termID'] + RDL + \
		mgi_utils.prvalue(r['jnumID']) + RDL + \
		mgi_utils.prvalue(r['qualifier']) + RDL)

	    if mouseOrtholog.has_key(marker):
		h = mouseOrtholog[marker]
	        reviewBCP.write(h['orthologSymbol'])

	    reviewBCP.write(LINEDL)

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
		'a._Term_key, qualifier = q.term, a._Qualifier_key, e._Refs_key ' + \
		'into #omimmouse1 ' + \
		'from GXD_AlleleGenotype g, VOC_Annot a, VOC_Evidence e, VOC_Term q ' + \
		'where g._Genotype_key = a._Object_key ' + \
		'and a._Qualifier_key = q._Term_key ' + \
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

	db.sql('create index idx11 on #toprocess(_Genotype_key)', None)

	#
	# delete existing cache records for this allele
	#

	db.sql('delete %s from #toprocess p, %s g where p._Genotype_key = g._Genotype_key' % (table, table), None)

	#
	# select all annotations for Genotypes of specified Allele
	#

	db.sql('select g._Marker_key, g._Allele_key, g._Genotype_key, g.sequenceNum, ' + \
		'a._Term_key, qualifier = q.term, a._Qualifier_key, e._Refs_key ' + \
		'into #omimmouse1 ' + \
		'from #toprocess p, GXD_AlleleGenotype g, VOC_Annot a, VOC_Evidence e, VOC_Term q ' + \
		'where p._Genotype_key = g._Genotype_key ' + \
		'and g._Genotype_key = a._Object_key ' + \
		'and a._Qualifier_key = q._Term_key ' + \
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
		'a._Term_key, qualifier = q.term, a._Qualifier_key, e._Refs_key ' + \
		'into #omimmouse1 ' + \
		'from GXD_AlleleGenotype g, VOC_Annot a, VOC_Evidence e, VOC_Term q ' + \
		'where g._Genotype_key = a._Object_key ' + \
		'and a._Qualifier_key = q._Term_key ' + \
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

	db.sql('create index idx12 on #toprocess(_Genotype_key)', None)

	#
	# delete existing cache records for this marker
	#

	db.sql('delete %s from #toprocess p, %s g where p._Genotype_key = g._Genotype_key' % (table, table), None)

	#
	# select all annotations for Genotypes of specified Marker
	#

	db.sql('select g._Marker_key, g._Allele_key, g._Genotype_key, g.sequenceNum, ' + \
		'a._Term_key, qualifier = q.term, a._Qualifier_key, e._Refs_key ' + \
		'into #omimmouse1 ' + \
		'from #toprocess p, GXD_AlleleGenotype g, VOC_Annot a, VOC_Evidence e, VOC_Term q ' + \
		'where p._Genotype_key = g._Genotype_key ' + \
		'and g._Genotype_key = a._Object_key ' + \
		'and a._Qualifier_key = q._Term_key ' + \
		'and a._AnnotType_key = %s ' % (mouseOMIMannotationKey) + \
		'and a._Annot_key = e._Annot_key', None)

	selectMouse()
	selectHuman(byOrtholog = 1)
	cacheGenotypeDisplay3()
	processMouse('sql')

#
# Main Routine
#

print '%s' % mgi_utils.date()

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

scriptName = os.path.basename(sys.argv[0])

#
# term key for 'not' qualifier
#

results = db.sql('select _Term_key from VOC_Term where _Vocab_key = 53 and term like "NOT%"', 'auto')
for r in results:
    notQualifier.append(r['_Term_key'])

#
# next available primary key: max(_Cache_key)
#
    
results = db.sql('select max(_Cache_key) as cacheKey from %s' % (table), 'auto')
for r in results:
    nextMaxKey = r['cacheKey']

if nextMaxKey == None:
    nextMaxKey = 0

# call functions based on the way the program is invoked

if scriptName == 'mrkomim.py':
    nextMaxKey = 0
    processDeleteReload()

# all of these invocations will only affect a certain subset of data

elif scriptName == 'mrkomimByAllele.py':
    processByAllele(objectKey)

elif scriptName == 'mrkomimByGenotype.py':
    processByGenotype(objectKey)

elif scriptName == 'mrkomimByMarker.py':
    processByMarker(objectKey)

db.useOneConnection(0)

print '%s' % mgi_utils.date()

