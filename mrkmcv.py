#!/usr/local/bin/python

'''
#
# Purpose: 
#
# Create bcp file for MRK_MCV_Cache. This is a cache of Marker Category
# Vocab term associations to markerS
#
# If markerkey is non-zero, then only create the bcp file for that marker only
#
# Usage: mrkmcv.py -Sdbserver -Ddatabase -Uuser -Ppasswordfile -Kmarkerkey
#
# Processing:
#
# History
#
# sc	05/05/2010 - created
#	TR6839; Marker Types
#
'''

import sys
import os
import getopt
import string
import mgi_utils
import db

db.setAutoTranslate(False)
db.setAutoTranslateBE(False)

try:
	COLDELIM = os.environ['COLDELIM']
	table = os.environ['TABLE']
	outDir = os.environ['MRKCACHEBCPDIR']
	curatorLog = os.environ['CURATORLOG']
except:
	COLDELIM = '|'
	table = 'MRK_MCV_Cache'
	outDir = './'
	curatorLog = './mrkmcv.log'

# qualifier column values
DIRECT='D'
INDIRECT='I'

# for reports
TAB='\t'
CRT='\n'

# date and created by column values
date = mgi_utils.date("%m/%d/%Y")
createdBy = '1000'

# report annotations to grouping term ids then
# annotate to the marker type feature instead
groupingTermIds = "MCV:0000029, MCV:0000001"

# file descriptor for the bcp file
mcvFp = None

# file descriptor for marker conflict curator log
rptFp = None

# Marker type mismatch header for rptFp
rptHeader1 = 'Markers  with conflict between the Marker Type and the MCV Marker Type%s%s' % (CRT, CRT)
rptHeader2 = 'MGI ID%sMarker Type%sMCV Term%sMCV Marker Type Term %s Web Display MCV Term%s' % \
    (TAB, TAB, TAB, TAB, CRT)

# true if there is at least one marker type mismatch
# Used to tell if we need to create a curator log
hasMkrTypeMismatch = 0

# list of mismatches to report
mismatchList = []

# grouping term annotation header for rptFp
rptHeader3 =  'Markers Annotated to Grouping Terms%s%s' % (CRT, CRT)
rptHeader4 = 'MGI ID%s Grouping Term%s' % (TAB, CRT)

# true if there is at least one marker annotated
# to a grouping term
hasGroupingAnnot = 0

# list of grouping annotations to report
groupingAnnotList = []

# list of grouping Ids from configuration
groupingIdList = []

# Markers w/multi MCV annotations header for rptFp
rptHeader5 = 'Markers with Multiple MCV Annotations%s%s' % (CRT, CRT)
rptHeader6 = 'MGI ID%s MCV IDs%s MCV Term%s' % (TAB, TAB, CRT) 

# true if there is at least one marker annotated
# to  multiple MCV terms
hasMultiMCVAnnot = 0

# list of markers with multiple MCV annotations to report
multiMCVList = []

# delete and insert statements
deleteSQL='delete from MRK_MCV_Cache where _Marker_key = %s'
insertSQL='insert into MRK_MCV_Cache values(%s,%s,"%s","%s","%s", %s,%s,"%s","%s")'

#
# map marker keys to their set of MCV annotations from VOC_Annot
# looks like {markerKey:[mcvTermKey1, ...], ...}
mkrKeyToMCVAnnotDict = {}

#
# map MCV Key to its parent term that represents a marker type (could be itself)
# looks like {mcvTermKey:mcvTermKey, ...}
mcvKeyToParentMkrTypeTermKeyDict = {}

# map mcvKey to its term
# looks like {mcvTermKey:term, ...}
mcvKeyToTermDict = {}

#
# map mcv term to mcv ID
# looks like (term:ID, ...}
mcvTermToIdDict = {}
#
# map marker type key to the MCV Term Key associated with the marker type
#  looks like {mTypeKey:mcvTermKey, ...}
mkrTypeKeyToAssocMCVTermKeyDict = {}

# The inverse of above looks like {mcvTermKey:mTypeKey}
mcvTermKeyToMkrTypeKeyDict = {}

#
# map marker key to its MGI ID
# looks like {mkrKey:mgiID, ...}
mkrKeyToIdDict = {}

#
# map marker type key to its name
# looks like {mkrTypeKey:mkrType, ...}
mkrTypeKeyToTypeDict = {}

#
# map descendent keys from the MCV closure to all of its ancestors
#
descKeyToAncKeyDict = {}

#
# map marker key to its marker type; used by processByMarker()
#
mkrKeyToMkrTypeKeyDict = {}

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
                '-K markerkey\n'

        sys.stderr.write(usage)
        sys.exit(1)

def init (mkrKey):
    global mkrKeyToMCVAnnotDict, mcvKeyToParentMkrTypeTermKeyDict 
    global mcvKeyToTermDict, mkrTypeKeyToAssocMCVTermKeyDict 
    global descKeyToAncKeyDict, mcvTermKeyToMkrTypeKeyDict
    global mkrKeyToMkrTypeKeyDict, groupingIdList
    global mcvTermToIdDict

    #
    # Purpose: load various lookups 
    #
   
    # Get the MCV vocab terms and their notes from the database
    # Notes tell us the term's MGI marker type if term maps directly to a 
    # marker type
    db.sql('''select n._Object_key, rtrim(nc.note) as chunk, nc.sequenceNum
	INTO TEMPORARY TABLE notes
	from MGI_Note n, MGI_NoteChunk nc
	where n._MGIType_key = 13
            and n._NoteType_key = 1001
            and n._Note_key = nc._Note_key''', None)

    db.sql('''create index notes_idx1 on notes(_Object_key)''', None)

    results = db.sql('''select t._Term_key, t.term, n.chunk
	    from VOC_Term t left outer join
	    notes n on (n._object_key = t._term_key)
	    where t._vocab_key = 79
	    order by t._Term_key, n.sequenceNum''', 'auto')

    notes = {} # map the terms to their note chunks
    for r in results:
	mcvKey = r['_Term_key']
	term = r['term']
	chunk = r['chunk']
	# if there is a note chunk add it to the notes dictionary
        # we'll pull all the chunks together later
	if chunk != None:
	    if not notes.has_key(mcvKey):
		notes[mcvKey] = []
	    notes[mcvKey].append(chunk)
	# add mapping of the key to the term 
        mcvKeyToTermDict[mcvKey] = term
    # parse the marker type from the note
    for mcvKey in notes.keys():
	note = string.join(notes[mcvKey], '')
	if not note[0:11] == 'Marker_Type':
            continue
	# parse the note
	# marker type part of the note is first and delimited
	# from any remaining note by ';' e.g. Marker_Type=1; remaining note
	tokens = string.split(note, ';')
	#print tokens
	mType = tokens[0]
	tokens = string.split(mType, '=')
	# 2nd token is the marker type key
	markerTypeKey = int(string.strip(tokens[1]))
	#print markerTypeKey
	# There is only 1  MCV term per MGI Mkr type?
	mkrTypeKeyToAssocMCVTermKeyDict[markerTypeKey]= mcvKey
	mcvTermKeyToMkrTypeKeyDict[mcvKey] = markerTypeKey
    #print 'mcvTermKeyToMkrTypeKeyDict %s' % mcvTermKeyToMkrTypeKeyDict

    #
    # verify that all MCV database marker types have a MTO term
    #
    results = db.sql('''select _Marker_Type_key
		from MRK_Types''', 'auto')
    mcvMarkerTypeKeys = mkrTypeKeyToAssocMCVTermKeyDict.keys()
    #print 'mcvMarkerTypeKeys: %s' % mcvMarkerTypeKeys 
    for r in results:
	mTypeKey = r['_Marker_Type_key']
	if mTypeKey not in mcvMarkerTypeKeys:
	    print 'marker type key %s not represented in MCV' % mTypeKey
	    sys.exit(1)

    #
    # now get all MCV marker annotations
    # and load into a dictionary
    #
    cmd = '''select distinct a._Term_key, a._Object_key as _Marker_key
        from VOC_Annot a
        where a._AnnotType_key = 1011
        and a._Qualifier_key = 1614158'''
    if mkrKey != 0:
	cmd = cmd + ' and _Object_key = %s' % mkrKey

    results = db.sql(cmd, 'auto')

    for r in results:
        mKey = r['_Marker_key']
	termKey = r['_Term_key']
        if not mkrKeyToMCVAnnotDict.has_key(mKey):
            mkrKeyToMCVAnnotDict[mKey]= []
        mkrKeyToMCVAnnotDict[mKey].append(termKey)

    # 
    # now map all mcv terms to their parent term representing a marker type
    # for all children in the closure table - find the parent 
    # which is a marker type parent
    #
    results = db.sql('''select _AncestorObject_key, _DescendentObject_key
            from DAG_Closure
            where _DAG_key = 9 
            and _MGIType_key = 13
            order by _DescendentObject_key''', 'auto')

    # the mcv term keys that represent marker types
    mcvMarkerTypeValues  = mkrTypeKeyToAssocMCVTermKeyDict.values() 
    #print 'the mcv term keys that represent marker types: %s' \
	#% mcvMarkerTypeValues
    for r in results:
        aKey = r['_AncestorObject_key']
        dKey = r['_DescendentObject_key']
        #print 'aKey: %s' % aKey
        #print 'dKey: %s' % dKey
        if mcvKeyToParentMkrTypeTermKeyDict.has_key(dKey):
	    # we've already mapped this descendent to its marker type parent
	    #print 'dKey %s already in dict mapped to %s' \
		#% (dKey, mcvKeyToParentMkrTypeTermKeyDict[dKey])
	    continue
	# dKey may be a marker type term
	elif dKey in mcvMarkerTypeValues:
	    #print 'adding dKey %s as parent rep for itself' % dKey
	    mcvKeyToParentMkrTypeTermKeyDict[dKey] = dKey
        # if the ancestor of this descendent term is a 
        # marker type term load it into the dict
        elif aKey in mcvMarkerTypeValues:
	    #print 'adding aKey %s as parent rep marker type for dkey %s' \
		#% (aKey, dKey)
	    mcvKeyToParentMkrTypeTermKeyDict[dKey] = aKey

    # map descendent keys from the MCV Closure to their ancestor keys
    # we'll use these to add the 'Indirect' annotations to the Cache
    results = db.sql('''select distinct _AncestorObject_key, _DescendentObject_key
            from DAG_Closure
            where _DAG_key = 9
            and _MGIType_key = 13
            order by _DescendentObject_key''', 'auto')
    for r in results:
        aKey = r['_AncestorObject_key']
        dKey = r['_DescendentObject_key']
        #print '%s\t%s\n' % (aKey, dKey)
        if not descKeyToAncKeyDict.has_key(dKey):
	    descKeyToAncKeyDict[dKey] = []
	descKeyToAncKeyDict[dKey].append(aKey)

    # map marker keys to their marker type
    cmd = ''' select _Marker_Type_key, _Marker_key
		from MRK_Marker
		where _Marker_Status_key in (1,3)
		and _Organism_key = 1'''
    if mkrKey != 0:
        cmd = cmd + ' and _Marker_key = %s' % mkrKey

    results = db.sql(cmd, 'auto')
    for r in results:
	mkrTypeKey = r['_Marker_Type_key']
	mkrKey = r['_Marker_key']
	mkrKeyToMkrTypeKeyDict[mkrKey] = mkrTypeKey

    # map mcvTerms to their IDs
    cmd = '''select a.accid, t.term
		from VOC_Term t, ACC_Accession a
		where t._Vocab_key = 79
		and t._Term_key = a._Object_key
		and a._MGIType_key = 13
		and a._LogicalDB_key = 146
		and preferred = 1'''
    results = db.sql(cmd, 'auto')
    for r in results:
 	mcvId = r['accid']
	term = r['term']
	mcvTermToIdDict[term] = mcvId

    # init the grouping term id list
    if groupingTermIds != None:
	tokens = string.split(groupingTermIds, ',')
	for t in tokens:
	    groupingIdList.append(string.strip(t))
	    
def writeRecord (mkrKey, mcvKey, directTerms, qualifier):
    # Purpose: write a record the the bcp file
    # Returns: nothing
    # Assumes: nothing
    # Effects: writes to a file in the file system 
    # Throws: nothing
    global mcvFp

    # get the term corresponding the mcvKey
    if  mcvKeyToTermDict.has_key(mcvKey):
	term = mcvKeyToTermDict[mcvKey]
    else:
	print 'term does not exist for mcvKey %s' % mcvKey
	sys.exit(1)

    mcvFp.write(mgi_utils.prvalue(mkrKey) + COLDELIM + \
                mgi_utils.prvalue(mcvKey) + COLDELIM + \
                mgi_utils.prvalue(term) + COLDELIM + \
		mgi_utils.prvalue(qualifier) + COLDELIM + \
		mgi_utils.prvalue(directTerms) + COLDELIM + \
                createdBy + COLDELIM + \
                createdBy + COLDELIM + \
                date + COLDELIM + \
                date + CRT)

def insertCache (mkrKey, mcvKey, directTerms, qualifier):
    # Purpose: insert a record in the cache
    # Returns: nothing
    # Assumes: nothing
    # Effects: creates a record in the database
    # Throws: nothing

    #print "%s %s %s %s" % (mkrKey, mcvKey, directTerms, qualifier)
    # get the term corresponding the mcvKey
    if  mcvKeyToTermDict.has_key(mcvKey):
        term = mcvKeyToTermDict[mcvKey]
    else:
        print 'term does not exist for mcvKey %s' % mcvKey
	sys.exit(1)
    db.sql(insertSQL % ( 
	mgi_utils.prvalue(mkrKey), \
	mgi_utils.prvalue(mcvKey), \
	mgi_utils.prvalue(term), \
	mgi_utils.prvalue(qualifier), \
        mgi_utils.prvalue(directTerms), \
	createdBy, \
	createdBy, \
	date, \
	date), None)
	
def processDirectAnnot(annotList, mTypeKey, mkrKey):
    global mismatchList, hasMkrTypeMismatch, groupingAnnotList, hasGroupingAnnot
    global multiMCVList, hasMultiMCVAnnot

    # Purpose: determine the set of direct annotations for this marker
    # Marker_Type_and_Sequence_Ontology_Implementation
    # Returns: The set of annotations for this marker
    # Assumes: nothing
    # Effects: nothing
    # Throws: nothing

    mcvMkrTypeKey = '' # default  if there isn't one
    annotateToList = []
    #print 'annotList: %s, mTypeKey: %s, mkrKey: %s' % (annotList, mTypeKey, mkrKey)
    if len(annotList) > 0: # there are annotations
	curatedAnnot = 0   # curated annots where the term matches the mkr type
	for mcvKey in annotList:
	    annotateKey = mcvKey # default; we may not annot to this term
	    # if the term is a grouping term, report it and annotate
	    # to term representing the marker's type
	    term = string.strip(mcvKeyToTermDict[mcvKey])
	    id = mcvTermToIdDict[term]
	    #print 'processDirect id: %s groupingIdList: %s' % (id, groupingIdList)
	    # if the marker has multiple mcv annotations, report it
            if len(annotList) > 1:
		hasMultiMCVAnnot = 1
                multiMCVList.append('%s%s%s%s%s%s' % (mkrKey, TAB, id, TAB, term, CRT))
	    if id in groupingIdList:
		#print 'id in groupingIdList'
		annotateKey = mkrTypeKeyToAssocMCVTermKeyDict[mTypeKey]	
                hasGroupingAnnot = 1
                groupingAnnotList.append('%s%s%s%s%s%s' % (mkrKey, TAB, id, TAB, term, CRT))
		# since we are mapping this annotation to the marker type mcv 
		# term we may already have this annotation in the list
		    
	    # if no parent mkr type for mcv term annotate to incoming term
	    else:
		if mcvKeyToParentMkrTypeTermKeyDict.has_key(mcvKey):
		    # get the the mcv term key representing the mkr type for the
		    # mcv term, note that it could be the same mcv term
		    mkrTypeMcvKey = mcvKeyToParentMkrTypeTermKeyDict[mcvKey]
		    # now get the marker type value for the MCV term
		    mcvMkrTypeKey = mcvTermKeyToMkrTypeKeyDict[mkrTypeMcvKey]
		    #print 'mcvMkrTypeKey %s' % mcvMkrTypeKey
		    #print 'MGI mTypeKey  %s' % mTypeKey
		# if the marker's type and the mcv marker type don't match then
		# annotate to the term representing the marker's type so all 
		# markers have an mcv annotation
		if mcvMkrTypeKey != '' and mcvMkrTypeKey != mTypeKey:
		    # report error; marker types don't match
		    # We've already checked that all mkr types have mcv terms, 
		    # so don't need to test that the key is in the dictionary
		    annotateKey = mkrTypeKeyToAssocMCVTermKeyDict[mTypeKey]
		    hasMkrTypeMismatch = 1
		    mismatchList.append('%s%s%s%s%s%s%s%s%s%s' % (mkrKey, TAB, mTypeKey, TAB, mcvKey, TAB, mkrTypeMcvKey, TAB, annotateKey, CRT) )
	    if annotateKey not in annotateToList:
		annotateToList.append(annotateKey)
    else: # no annotations; find mcv term for the marker's type
          # and annotate to that
        annotateToList.append(mkrTypeKeyToAssocMCVTermKeyDict[mTypeKey])
    #print 'annotateToList: %s' % annotateToList
    return annotateToList

def createBCPfile():
    global mcvFp, rptFp, mismatchList, groupingAnnotList
    global mkrKeyToIdDict, mkrTypeKeyToTypeDict
    '''
    # Purpose: create bcp file for MRK_MCV_Cache which
    # is a cache table of marker category terms
    # annotated to markers 
    # Returns: nothing
    # Assumes: nothing
    # Effects: writes to a bcp file
    # Throws: nothing
    '''
    # full path to th bcp file
    mcvBCP = '%s/%s.bcp' % (outDir, table)

    #print 'Creating %s and %s ...' % (mcvBCP, curatorLog)
    mcvFp = open(mcvBCP, 'w')
    rptFp = open(curatorLog, 'w')

    # get all official and interim mouse markers
    results = db.sql('''select _Marker_key, _Marker_Type_key
	    from MRK_Marker
	    where _Organism_key = 1
	    and _Marker_Status_key in (1, 3)''', 'auto')
    for r in results:
	mkrKey = r['_Marker_key']
        mTypeKey = r['_Marker_Type_key']

        # list of VOC_Annot keys for current marker
	annotList = [] # default if there are none
	# list of VOC_Annot terms for current marker
	if mkrKeyToMCVAnnotDict.has_key(mkrKey):
	    annotList = mkrKeyToMCVAnnotDict[mkrKey]
	annotateToList = processDirectAnnot(annotList, mTypeKey, mkrKey)

	# get the terms for the direct annotations, every annotation in the
	# cache will have a list of direct terms
	directTermList = []
	for mcvKey in annotateToList:
	    if mcvKeyToTermDict.has_key(mcvKey):
		term = mcvKeyToTermDict[mcvKey]
		directTermList.append(term)
	    else:
		print 'term does not exist for mcvKey %s' % mcvKey
		sys.exit(1)

	# annotations already made so we don't create dups
	annotMadeList = []

	# create a comma delimited string of direct terms for the marker
	directTerms = string.join(directTermList, ',')
	for a in annotateToList:
	    writeRecord(mkrKey, a, directTerms, DIRECT)
	    annotMadeList.append(a)
	    # Now add indirect associations from the closure
	    ancList = descKeyToAncKeyDict[a]
	    #print 'INDIRECT annotations  %s mkrKey %s ' % (ancList, mkrKey)
	    
	    for ancKey in ancList:
		if ancKey not in annotMadeList:
		    annotMadeList.append(ancKey)
		    writeRecord(mkrKey, ancKey, directTerms, INDIRECT)
    mcvFp.close()

    # create the curator log if there are mismatches and/or there are 
    # annotations to grouping terms
    if hasMkrTypeMismatch == 1 or hasGroupingAnnot == 1:
	createReportLookups()
    rptFp.write(rptHeader1)
    rptFp.write(rptHeader2)
    rptFp.write(100*'-' + CRT)
    if hasMkrTypeMismatch == 1:
	for m in mismatchList:
	    l = string.split(m, TAB)
	    mkrKey = int(l[0])
	    mkrTypeKey = int(l[1])
	    mcvTermKey = int(l[2])
	    mcvMkrTypeTermKey = int(l[3])
	    loadAssignedTermKey = int(l[4])
	    mgiID = mkrKeyToIdDict[mkrKey]
	    mkrType = mkrTypeKeyToTypeDict[mkrTypeKey]
	    mcvTerm = mcvKeyToTermDict[mcvTermKey]
	    mcvMkrTypeTerm = mcvKeyToTermDict[mcvMkrTypeTermKey]
	    loadAssignedTerm = mcvKeyToTermDict[loadAssignedTermKey]
	    rptFp.write('%s%s%s%s%s%s%s%s%s%s' % (mgiID, TAB, mkrType, TAB, mcvTerm, TAB, mcvMkrTypeTerm, TAB, loadAssignedTerm, CRT))
    rptFp.write('%sTotal: %s%s%s%s' % (CRT, len(mismatchList), CRT, CRT, CRT ))

    rptFp.write(rptHeader3)
    rptFp.write(rptHeader4)
    rptFp.write(100*'-' + CRT)
    if hasGroupingAnnot == 1:
	for g in groupingAnnotList:
	    l = string.split(g, TAB)
	    mkrKey = int(l[0])
	    gID =  l[1]
	    gTerm = l[2]
	    mgiID =  mkrKeyToIdDict[mkrKey]
	    rptFp.write('%s%s%s%s%s%s' % (mgiID, TAB, gID, TAB, gTerm, CRT))
    
    rptFp.write('%sTotal: %s%s%s%s' % \
	(CRT, len(groupingAnnotList), CRT, CRT, CRT ))

    rptFp.write(rptHeader5)
    rptFp.write(rptHeader6)
    rptFp.write(100*'-' + CRT)
    if hasMultiMCVAnnot == 1:
	for m in multiMCVList:
	    l = string.split(m, TAB)
	    mkrKey = int(l[0])
            mID =  l[1]
            mTerm = l[2]
            mgiID =  mkrKeyToIdDict[mkrKey]
            rptFp.write('%s%s%s%s%s%s' % (mgiID, TAB, mID, TAB, mTerm, CRT))
    rptFp.write('%sTotal: %s%s' % \
        (CRT, len(multiMCVList), CRT ))
    rptFp.close()

def createReportLookups():
    # Purpose: Create lookups for generating reports, only called when creating bcp
    #    not called when updating cache by marker
    # Returns: nothing
    # Assumes: nothing
    # Effects: queries a database
    # Throws: nothing

    global mkrKeyToIdDict, mkrTypeKeyToTypeDict

    results = db.sql('''select a.accid, a._Object_key
	from MRK_Marker m, ACC_Accession a
	where m._Marker_Status_key in (1,3)
	and m._Organism_key = 1
	and m._Marker_key = a._Object_key
	and a._MGIType_key = 2
	and a._LogicalDB_key = 1
	and a.prefixPart = 'MGI:'
	and a.preferred = 1''', 'auto')
    for r in results:
	mkrKey = r['_Object_key']
	mgiID = r['accid']
	mkrKeyToIdDict[mkrKey] = mgiID

    results = db.sql('''select _Marker_Type_key, name
	from MRK_Types''', 'auto')
    for r in results:
	mkrTypeKey = r['_Marker_Type_key']
	mkrType = r['name']
	mkrTypeKeyToTypeDict[mkrTypeKey] = mkrType

def processByMarker(mkrKey):
    # Purpose: Update MCV annotations for a given markeR
    # Returns: nothing
    # Assumes: nothing
    # Effects: queries a database, inserts into a database
    # Throws: nothing

    # get the marker type; needed to determine the MCV annotation
    mTypeKey = mkrKeyToMkrTypeKeyDict[mkrKey]

    # select all annotations in MRK_MCV_Cache for the specified marker
    results = db.sql('''select *
	into #toprocess
	from MRK_MCV_Cache
	where _Marker_key = %s''' % mkrKey, 'auto')

    db.sql('''create index toprocess_idx1 on #toprocess(_Marker_key)''', None)

    #
    # delete existing cache records for this marker
    #

    db.sql(deleteSQL % mkrKey, None)

    #
    # select all annotations for the specified marker
    # determine the direct and indirect annotations
    # and add them to the cache
    results = db.sql('''select distinct a._Term_key
        from VOC_Annot a
        where a._AnnotType_key = 1011
        and a._Qualifier_key = 1614158
	and a._Object_key = %s''' % mkrKey, 'auto')
    annotList = [] # default if there are none
    for r in results:
	annotList.append(r['_Term_key'])

    # get the set of direct annotations which includes inferred from marker 
    # type where applicable
    annotateToList = processDirectAnnot(annotList, mTypeKey, mkrKey)
    #print 'annotateToList: %s' % annotateToList

    # get the terms  for the direct annotations, every annotation in the cache
    # will have a list of direct terms
    directTermList = []
    for mcvKey in annotateToList:
	if mcvKeyToTermDict.has_key(mcvKey):
	    term = mcvKeyToTermDict[mcvKey]
	    directTermList.append(term)
	else:
	    sys.exit(1)
	    print 'term does not exist for mcvKey %s' % mcvKey
    #print 'directTermList: %s' % directTermList
    # annotations already made so we don't create dups
    annotMadeList = []

    directTerms = string.join(directTermList, ',')
    for a in annotateToList:
        #print 'insertCache(mkrKey: %s, annotTo:%s, directTerms: %s DIRECT)' % (mkrKey, a, directTerms)
	insertCache(mkrKey, a, directTerms, DIRECT)
	annotMadeList.append(a)
	# Now add indirect associations from the closure
	if not descKeyToAncKeyDict.has_key(a):
	    continue
	ancList = descKeyToAncKeyDict[a]
	for ancKey in ancList:
	    if ancKey not in annotMadeList:
		#print 'insertCache(mkrKey: %s, ancKey:%s, INDIRECT) ancKey is type: %s' % (mkrKey, ancKey, type(ancKey))
	 	annotMadeList.append(ancKey)
		#print 'insertCache(mkrKey: %s, ancKey: %s, directTerms: %s, INDIRECT)' % (mkrKey, ancKey, directTerms)
		insertCache(mkrKey, ancKey, directTerms, INDIRECT)

	db.commit()

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
mkrKey = None

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
                mkrKey = int(opt[1])
        else:
                showUsage()

if server is None or \
    database is None or \
    user is None or \
    password is None or \
    mkrKey is None:
	showUsage()

db.set_sqlLogin(user, password, server, database)
db.useOneConnection(1)

init(mkrKey)

if mkrKey == 0:
    createBCPfile()
else:
    processByMarker(mkrKey)

db.useOneConnection(0)

print '%s' % mgi_utils.date()

