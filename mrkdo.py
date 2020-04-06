
'''
#
# Purpose:
#
# Create bcp file for MRK_DO_Cache, a cache table of:
#
#	1.  Mouse genotype-to-DO Disease annotations
#	2.  Human-to-DO Disease/Gene annotations
#
# Usage:
#	mrkdo.py -Sdbserver -Ddatabase -Uuser -Ppasswordfile
#
# Processing:
#
#	1.  Select all Mouse genotype-to-DO Disease annotations
#	2.  Categorize them
#	3.  Write records to output file
#	4.  Select all Human-to-DO Disease annotations
#	5.  Categorize them
#	6.  Write records to output file
#
# diseaseMouseModels is the Disease Detail Mouse Model table. (MRK_DO_Cache.doCategory3)
#
# possible values for doCategory3:
#
#	-1: ignore
#	1 : Models with phenotypic similarity to human disease where etiologies involve orthologs.
#	2 : Models with phenotypic similarity to human disease where etiologies are distinct.
#	3 : Models with phenotypic similarity to human diseases where etiology is unknown or involving genes where ortholog is unknown.
#	4 : Models involving transgenes or other mutation types.
#	5 : No similarity to the expected human disease phenotype was found.
#
# Used:
#	femover/gather/disease_gatherer.py
#	femover/gather/mp_annotation_gatherer.py
#	qcreports_db/mgd/MRK_GOIEA.py
#	reports_db/mgimarkerfeed/mgiMarkerFeed.py
#	wi_test_suite
#
# History
#
# 11/28/2016    lec
#       - TR12427/Disease Ontology (DO)
#
#
'''

import sys
import os
import getopt
import re
import mgi_utils
import db


try:
    COLDL = os.environ['COLDELIM']
    LINEDL = '\n'
    table = os.environ['TABLE']
    outDir = os.environ['MRKCACHEBCPDIR']
except:
    table = 'MRK_DO_Cache'

doBCP = None

cdate = mgi_utils.date("%m/%d/%Y")

mouseDOannotationKey = 1020
humanDOannotationKey = 1022
mouseOrganismKey = 1
humanOrganismKey = 2

humanOrtholog = {}	# mouse marker key : human ortholog (key, symbol)
mouseOrtholog = {}	# human marker key : mouse ortholog (key, symbol)
genotypeOrtholog = {}   # genotype key : list of human marker keys:symbols

humanToDO = {}	# human marker key : DO term id
DOToHuman = {}	# DO term id : list of human marker keys

genotypeAlleleMouseModels = {}	# mouse genotype key + termID: display category 3

notQualifier = []

gene = 1
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
                '-P password file\n'

        sys.stderr.write(usage)
        sys.exit(1)
 
def deriveAlleleDetailMouseModels(r):
        #
        # Purpose: derives the appropriate Phenotype Detail page category for the record 
        #          and hence the appropriate Human Disease Detail page (Mouse Models)
        # Returns: the category (1,2,3,4,5), -1 if no category could be determined
        #	**the 'category' is only used to determine if the Allele is Cre : when the category is 4**
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

        if marker in humanOrtholog:
            hasOrtholog = 1
            ortholog = humanOrtholog[marker]
            orthologKey = ortholog['orthologKey']
            orthologSymbol = ortholog['orthologSymbol']
            isHumanOrthologAnnotated = orthologKey in humanToDO
        else:
            isHumanOrthologAnnotated = 0

        # list of human orthologs for this genotype that don't include this marker's ortholog
        # but are annotated to this term

        if genotype in genotypeOrtholog:
            for s in genotypeOrtholog[genotype]:
                if s['orthologKey'] != orthologKey:
                    if s['orthologKey'] in humanToDO:
                        dolookup = humanToDO[s['orthologKey']]
                        # human ortholog is annotated to Term
                        if termID in dolookup:
                            genotypeOrthologToPrint.append(s['orthologSymbol'])

        # check if any human gene is annotated to Term
        isHumanGeneAnnotated = termID in DOToHuman

        #
        #  5.  no similarity
        #	a. mouse genotype is annotated to Term and is a NOT annotation
        #

        if r['_Qualifier_key'] in notQualifier:
            if hasOrtholog and isHumanOrthologAnnotated:
                dolookup = humanToDO[orthologKey]
                # human ortholog is annotated to Term
                if termID in dolookup:
                    if len(genotypeOrthologToPrint) == 1:
                        genotypeFootnote = genotypeFootnote1 % (str.join(',', genotypeOrthologToPrint)) + genotypeFootnote 
                    elif len(genotypeOrthologToPrint) >= 1:
                        genotypeFootnote = genotypeFootnote2 % (str.join(',', genotypeOrthologToPrint)) + genotypeFootnote
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
                    genotypeFootnote = genotypeFootnote1 % (str.join(',', genotypeOrthologToPrint)) + genotypeFootnote 
                elif len(genotypeOrthologToPrint) >= 1:
                    genotypeFootnote = genotypeFootnote2 % (str.join(',', genotypeOrthologToPrint)) + genotypeFootnote

            if not isHumanGeneAnnotated:
                return 3, phenoHeader3a, headerFootnote, genotypeFootnote

            if isHumanOrthologAnnotated:
                dolookup = humanToDO[orthologKey]
                # human ortholog is annotated to Term
                if termID in dolookup:
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

                for g in DOToHuman[termID]:
                    # if human gene has no mouse ortholog, flag it
                    if g not in mouseOrtholog:
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

def selectMouse():
        #
        # Purpose:  selects the appropriate mouse gentoype data
        # Returns:
        # Assumes:  temp table domouse1 has already been created
        # Effects:  initializes global dictionaries/caches
        #	- humanOrtholog, genotypeOrtholog
        # Throws:
        #

        global humanOrtholog, genotypeOrtholog

        db.sql('create index idx1 on domouse1(_Marker_key)', None)
        db.sql('create index idx2 on domouse1(_Allele_key)', None)

        #
        # resolve marker symbol
        #
        db.sql('''select o.*, m._Organism_key, m.symbol as markerSymbol, m._Marker_Type_key, a.symbol as alleleSymbol 
                INTO TEMPORARY TABLE domouse2 
                from domouse1 o, MRK_Marker m, ALL_Allele a 
                where o._Marker_key = m._Marker_key 
                and o._Allele_key = a._Allele_key
                ''', None)
        db.sql('create index idx3 on domouse2(_Marker_key)', None)

        #
        # resolve DO term and ID
        #
        db.sql('''select o.*, t.term, a.accID as termID 
                INTO TEMPORARY TABLE domouse3 
                from domouse2 o, VOC_Term t, ACC_Accession a 
                where o._Term_key = t._Term_key 
                and o._Term_key = a._Object_key 
                and a._MGIType_key = 13 
                and a.preferred = 1
                ''', None)
        db.sql('create index idx4 on domouse3(_Refs_key)', None)

        #
        # resolve Jnumber
        #
        db.sql('''select o.*, a.accID as jnumID 
                INTO TEMPORARY TABLE domouse4 
                from domouse3 o, ACC_Accession a 
                where o._Refs_key = a._Object_key 
                and a._MGIType_key = 1 
                and a._LogicalDB_key = 1 
                and a.prefixPart = 'J:' 
                and a.preferred = 1
                ''', None)
        db.sql('create index idx5 on domouse4(_Genotype_key)', None)
        db.sql('create index idx6 on domouse4(_Allele_key)', None)

        #
        # resolve human ortholog
        #
        db.sql('''select distinct o._Marker_key, cm2._Marker_key as orthologKey,  
                    m2.symbol as orthologSymbol
                INTO TEMPORARY TABLE orthologHuman
                from domouse1 o, MRK_Cluster c1, MRK_ClusterMember cm1, MRK_Cluster c2, 
                    MRK_ClusterMember cm2, MRK_Marker m2 
                where o._Marker_key = cm1._Marker_key
                and cm1._Cluster_key = c1._Cluster_key
                and c1._ClusterType_key = 9272150
                and c1._ClusterSource_key = 13764519
                and c1._Cluster_key = c2._Cluster_key
                and c2._Cluster_key = cm2._Cluster_key
                and cm2._Marker_key = m2._Marker_key 
                and m2._Organism_key = %s''' % (humanOrganismKey), None)
        db.sql('create index idx7 on orthologHuman(_Marker_key)', None)

        results = db.sql('select * from orthologHuman', 'auto')
        for r in results:
            key = r['_Marker_key']
            value = r
            humanOrtholog[key] = value

        #
        # resolve genotype-to-orthologs
        #
        results = db.sql('''select distinct g._Genotype_key, o.orthologKey, o.orthologSymbol 
                from domouse1 g, orthologHuman o 
                where g._Marker_key = o._Marker_key
                ''', 'auto')
        for r in results:
            key = r['_Genotype_key']
            value = r
            if key not in genotypeOrtholog:
                genotypeOrtholog[key] = []
            genotypeOrtholog[key].append(value)

def cacheGenotypeDisplay3():
        #
        # Purpose:  initializes global dictionary of genotype/terms and minimum display category 1 values
        # Returns:
        # Assumes:  temp table domouse4 exists
        # Effects:  initializes global genotypeAlleleMouseModels dictionary
        # Throws:
        #

        global genotypeAlleleMouseModels

        #
        # for each genotype/term, cache the mininum display category 1 value
        # this will be display category 3 (human disease table 2 (Mouse Models))
        #

        results = db.sql('select * from domouse4 order by _Genotype_key, termID', 'auto')

        for r in results:

            genotype = r['_Genotype_key']
            termID = r['termID']
            gcKey = repr(genotype) + termID
            alleleDetailMouseModels, header, headerFootnote, genotypeFootnote = deriveAlleleDetailMouseModels(r)

            if gcKey in genotypeAlleleMouseModels:
                if alleleDetailMouseModels < genotypeAlleleMouseModels[gcKey]:
                    genotypeAlleleMouseModels[gcKey] = alleleDetailMouseModels
            else:
                genotypeAlleleMouseModels[gcKey] = alleleDetailMouseModels

def processMouse():
        #
        # Purpose:  process Mouse records either by bcp
        # Returns:
        # Assumes:
        # Effects:
        # Throws:
        #

        global nextMaxKey

        #
        # process each individual marker/genotype record
        #

        results = db.sql('select * from domouse4 order by _Genotype_key, alleleSymbol, term', 'auto')

        for r in results:

            marker = r['_Marker_key']
            genotype = r['_Genotype_key']
            termID = r['termID']
            gcKey = repr(genotype) + termID

            alleleDetailMouseModels, header, headerFootnote, genotypeFootnote = deriveAlleleDetailMouseModels(r)
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

            doBCP.write(
                    str(nextMaxKey) + COLDL +  \
                    mgi_utils.prvalue(r['_Organism_key']) + COLDL +  \
                    mgi_utils.prvalue(r['_Marker_key']) + COLDL +  \
                    mgi_utils.prvalue(r['_Genotype_key']) + COLDL + \
                    mgi_utils.prvalue(r['_Term_key']) + COLDL + \
                    mgi_utils.prvalue(r['_Refs_key']) + COLDL + \
                    mgi_utils.prvalue(diseaseMouseModels) + COLDL + \
                    mgi_utils.prvalue(r['qualifier']) + COLDL + \
                    r['term'] + COLDL + \
                    r['termID'] + COLDL + \
                    r['jnumID'] + COLDL + \
                    mgi_utils.prvalue(header) + COLDL + \
                    mgi_utils.prvalue(headerFootnote) + COLDL + \
                    mgi_utils.prvalue(genotypeFootnote) + COLDL + \
                    cdate + COLDL + cdate + LINEDL)

def selectHuman():
        #
        # Purpose:  selects the appropriate human annotation data
        # Returns:
        # Assumes:
        # Effects:  initializes global dictionaries: mouseOrtholog, humanToDO, DOToHuman
        # Throws:
        #

        global mouseOrtholog, humanToDO, DOToHuman

        #
        # select all human genes annotated to DO Gene or Disease Terms
        #

        db.sql('''select a._Object_key as _Marker_key, ac.accID as termID, 
            a._Term_key, t.term, q.term as qualifier, a._Qualifier_key, e._Refs_key 
            INTO TEMPORARY TABLE dohuman1 
            from VOC_Annot a, VOC_Evidence e, VOC_Term t, ACC_Accession ac, VOC_Term q 
            where a._AnnotType_key = %s
            and a._Qualifier_key = q._Term_key 
            and a._Annot_key = e._Annot_key 
            and a._Term_key = t._Term_key 
            and a._Term_key = ac._Object_key 
            and ac._MGIType_key = 13 
            and ac.preferred = 1
            ''' % (humanDOannotationKey), None)
        db.sql('create index idx9 on dohuman1(_Marker_key)', None)

        #
        # resolve marker symbol
        #
        db.sql('''select o.*, m._Organism_key, m.symbol as markerSymbol, m._Marker_Type_key 
                INTO TEMPORARY TABLE dohuman2 
                from dohuman1 o, MRK_Marker m 
                where o._Marker_key = m._Marker_key 
                ''', None)
        db.sql('create index idx10 on dohuman2(_Marker_key)', None)

        #
        # cache all terms annotated to human markers
        # cache all human markers annotated to terms
        #
        results = db.sql('select o._Marker_key, o.termID from dohuman2 o order by o._Marker_key', 'auto')
        for r in results:
            key = r['_Marker_key']
            value = r['termID']
            if key not in humanToDO:
                humanToDO[key] = []
            humanToDO[key].append(value)

            key = r['termID']
            value = r['_Marker_key']
            if key not in DOToHuman:
                DOToHuman[key] = []
            DOToHuman[key].append(value)

        #
        # resolve Jnumber
        #
        db.sql('''select o.*, a.accID as jnumID 
                INTO TEMPORARY TABLE dohuman3 
                from dohuman2 o, ACC_Accession a 
                where o._Refs_key = a._Object_key 
                and a._MGIType_key = 1 
                and a._LogicalDB_key = 1 
                and a.prefixPart = 'J:' 
                and a.preferred = 1 
                union 
                select o.*, null as jnumID from dohuman2 o where _Refs_key = -1
                ''', None)

        #
        # resolve mouse ortholog
        #
        results = db.sql('''select distinct o._Marker_key, cm2._Marker_key as orthologKey,
                    m2.symbol as orthologSymbol
                from domouse1 o, MRK_Cluster c1, MRK_ClusterMember cm1, MRK_Cluster c2,
                    MRK_ClusterMember cm2, MRK_Marker m2
                where o._Marker_key = cm1._Marker_key
                and cm1._Cluster_key = c1._Cluster_key
                and c1._ClusterType_key = 9272150
                and c1._ClusterSource_key = 13764519
                and c1._Cluster_key = c2._Cluster_key
                and c2._Cluster_key = cm2._Cluster_key
                and cm2._Marker_key = m2._Marker_key
                and m2._Organism_key = %s
                ''' % (mouseOrganismKey), 'auto')

        for r in results:
            key = r['_Marker_key']
            value = r
            mouseOrtholog[key] = value

def processDeleteReload():
        #
        # Purpose:  processes data for BCP-type processing; aka delete/reload
        # Returns:
        # Assumes:
        # Effects:  initializes global file pointers:  doBCP
        # Throws:
        #

        global doBCP

        print('%s' % mgi_utils.date())

        doBCP = open(outDir + '/' + table + '.bcp', 'w')

        #
        # select all mouse genotypes annotated to DO Disease Terms
        #

        db.sql('''select g._Marker_key, g._Allele_key, g._Genotype_key, 
                a._Term_key, q.term as qualifier, a._Qualifier_key, e._Refs_key 
                INTO TEMPORARY TABLE domouse1 
                from GXD_AlleleGenotype g, VOC_Annot a, VOC_Evidence e, VOC_Term q 
                where g._Genotype_key = a._Object_key 
                and a._Qualifier_key = q._Term_key 
                and a._AnnotType_key = %s
                and a._Annot_key = e._Annot_key
                ''' % (mouseDOannotationKey), None)

        selectMouse()
        selectHuman()
        cacheGenotypeDisplay3()
        processMouse()
        doBCP.close()

        print('%s' % mgi_utils.date())

#
# Main Routine
#

print('%s' % mgi_utils.date())

try:
        optlist, args = getopt.getopt(sys.argv[1:], 'S:D:U:P:K:')
except:
        showUsage()

server = None
database = None
user = None
password = None

for opt in optlist:
        if opt[0] == '-S':
                server = opt[1]
        elif opt[0] == '-D':
                database = opt[1]
        elif opt[0] == '-U':
                user = opt[1]
        elif opt[0] == '-P':
                password = str.strip(open(opt[1], 'r').readline())
        else:
                showUsage()

if server is None or \
   database is None or \
   user is None or \
   password is None:
        showUsage()

db.set_sqlLogin(user, password, server, database)
db.useOneConnection(1)

#
# term key for 'not' qualifier
#

results = db.sql('''select _Term_key from VOC_Term where _Vocab_key = 53 and term like 'NOT%' ''', 'auto')
for r in results:
    notQualifier.append(r['_Term_key'])

processDeleteReload()

db.useOneConnection(0)

print('%s' % mgi_utils.date())
