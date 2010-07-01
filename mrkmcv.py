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
import db
import mgi_utils

try:
    COLDL = os.environ['COLDELIM']
    LINEDL = '\n'
    outDir = os.environ['MRKCACHEBCPDIR']
    table = os.environ['TABLE']
except:
    table = 'MRK_MCV_Cache'

# qualifier column values
DIRECT='D'
INDIRECT='I'

# date and created by column values
date = mgi_utils.date("%m/%d/%Y")
createdBy = '1000'

# file descriptor for the bcp file
mcvFp = None

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
# map marker type key to the MCV Term Key associated with the marker type
#  looks like {mTypeKey:mcvTermKey, ...}
mkrTypeKeyToAssocMCVTermKeyDict = {}

# The inverse of above looks like {mcvTermKey:mTypeKey}
mcvTermKeyToMkrTypeKeyDict = {}

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

def init (markerKey):
    global mkrKeyToMCVAnnotDict, mcvKeyToParentMkrTypeTermKeyDict 
    global mcvKeyToTermDict, mkrTypeKeyToAssocMCVTermKeyDict 
    global descKeyToAncKeyDict, mcvTermKeyToMkrTypeKeyDict
    global mkrKeyToMkrTypeKeyDict

    #
    # parse the MCV Note and load 
    # mcvMkrTypeDict and mcvKeyToParentMkrTypeTermKeyDict
    #
    # we store the association of a marker type to a MCV
    # term in the term Note. Only MCV terms which correspond to 
    # marker types have these notes
    # note looks like:
    # Marker_Type=N
    #
    # _Vocab_key = 79 = Marker Category Vocab
    # _NoteType_key = 1001 = Private Vocab Term Comment'
   
    # Get the MCV vocab terms and their notes from the database
    # Notes tell us the term's MGI marker type if term maps directly to a 
    # marker type
    db.sql('''select n._Object_key, rtrim(nc.note) as chunk, nc.sequenceNum
	into #notes
	from MGI_Note n, MGI_NoteChunk nc
	where n._MGIType_key = 13
            and n._NoteType_key = 1001
            and n._Note_key = nc._Note_key''', None)

    db.sql('''create index idx1 on #notes(_Object_key)''', None)

    results = db.sql('''select t._Term_key, t.term, n.chunk
	    from VOC_Term t, #notes n
	    where t._Vocab_key = 79 
	    and t._Term_key *= n._Object_key
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
	# parse the note
	tokens = string.split(note, '=')
	# 2nd token is the marker type key
	markerTypeKey = int(string.strip(tokens[1]))
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
    if markerKey != 0:
	cmd = cmd + ' and _Object_key = %s' % markerKey

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
    #print 'the mcv term keys that represent marker types: %s' % mcvMarkerTypeValues
    for r in results:
        aKey = r['_AncestorObject_key']
        dKey = r['_DescendentObject_key']
        #print 'aKey: %s' % aKey
        #print 'dKey: %s' % dKey
        if mcvKeyToParentMkrTypeTermKeyDict.has_key(dKey):
	    # we've already mapped this descendent to its marker type parent
	    #print 'dKey %s already in dict mapped to %s' % (dKey, mcvKeyToParentMkrTypeTermKeyDict[dKey])
	    continue
	# dKey may be a marker type term
	elif dKey in mcvMarkerTypeValues:
	    #print 'adding dKey %s as parent rep for itself' % dKey
	    mcvKeyToParentMkrTypeTermKeyDict[dKey] = dKey
        # if the ancestor of this descendent term is a 
        # marker type term load it into the dict
        elif aKey in mcvMarkerTypeValues:
	    #print 'adding aKey %s as parent rep marker type for dkey %s' % (aKey, dKey)
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
		where _Marker_Status_key = 1'''
    if markerKey != 0:
        cmd = cmd + ' and _Marker_key = %s' % markerKey

    results = db.sql(cmd, 'auto')
    for r in results:
	mkrTypeKey = r['_Marker_Type_key']
	mkrKey = r['_Marker_key']
	mkrKeyToMkrTypeKeyDict[mkrKey] = mkrTypeKey

def writeRecord (markerKey, mcvKey, directTerms, qualifier):
    global mcvFp

    if  mcvKeyToTermDict.has_key(mcvKey):
	term = mcvKeyToTermDict[mcvKey]
    else:
	print 'term does not exist for mcvKey %s' % mcvKey
	sys.exit(1)

    mcvFp.write(mgi_utils.prvalue(markerKey) + COLDL + \
                mgi_utils.prvalue(mcvKey) + COLDL + \
                mgi_utils.prvalue(term) + COLDL + \
		mgi_utils.prvalue(qualifier) + COLDL + \
		mgi_utils.prvalue(directTerms) + COLDL + \
                createdBy + COLDL + \
                createdBy + COLDL + \
                date + COLDL + \
                date + LINEDL)

def insertCache (markerKey, mcvKey, directTerms, qualifier):
    #print "%s %s %s %s" % (markerKey, mcvKey, directTerms, qualifier)
    if  mcvKeyToTermDict.has_key(mcvKey):
        term = mcvKeyToTermDict[mcvKey]
    else:
        print 'term does not exist for mcvKey %s' % mcvKey
	sys.exit(1)
# insert into MRK_MCV_Cache values(%s,%s,"%s","%s",%s,%s,%s,%s)
    db.sql(insertSQL % ( 
	mgi_utils.prvalue(markerKey), \
	mgi_utils.prvalue(mcvKey), \
	mgi_utils.prvalue(term), \
        mgi_utils.prvalue(directTerms), \
	mgi_utils.prvalue(qualifier), \
	createdBy, \
	createdBy, \
	date, \
	date), None)
	
def processDirectAnnot(annotList, mTypeKey):

    mcvMkrTypeKey = '' # default  if there isn't one
    annotateToList = []
    
    if len(annotList) > 0: # there are annotations
	for mcvKey in annotList:
	    annotateKey = mcvKey # default; we may not annot to this term
	    # get the marker type of this mcv
	    if mcvKeyToParentMkrTypeTermKeyDict.has_key(mcvKey):
		# get the the mcv term key representing the marker type for this mcv, note it could
		# be the same mcv term
		mkrTypeMtoKey = mcvKeyToParentMkrTypeTermKeyDict[mcvKey]
		# now get the marker type value for the MCV term
		mcvMkrTypeKey = mcvTermKeyToMkrTypeKeyDict[mkrTypeMtoKey]
		#print 'mcvMkrTypeKey %s' % mcvMkrTypeKey
		#print 'MGI mTypeKey  %s' % mTypeKey
	    # if the marker's type and the mcv marker type don't match then
	    # annotate to the term representing the marker's type so all markers have an mcv annotation
	    if mcvMkrTypeKey != '' and mcvMkrTypeKey != mTypeKey:
		# report error; marker types don't match
		# We've already checked that all marker types have mcv terms, so don't need to test
		# that the key is in the dictionary
		annotateKey = mkrTypeKeyToAssocMCVTermKeyDict[mTypeKey]
	        #print 'marker type %s != mcvMkrType %s for mcvKey %s annotating to %s' \
		    #% (mTypeKey, mcvMkrTypeKey, mcvKey, annotateKey)
	    annotateToList.append(annotateKey)
    else: # no annotations; find mcv term for the marker's type
	  # and annotate to that
	annotateToList.append(mkrTypeKeyToAssocMCVTermKeyDict[mTypeKey])
    return annotateToList

def createBCPfile():
    global mcvFp
    '''
    #
    # MRK_MCV_Cache is a cache table of marker category terms
    # annotated to markers via marker type or sequence ontology terms
    #
    '''
    # full path to th bcp file
    mcvBCP = '%s/%s.bcp' % (outDir, table)

    print 'Creating %s ...' % mcvBCP
    mcvFp = open(mcvBCP, 'w')
    # get all official mouse markers
    results = db.sql('''select _Marker_key, _Marker_Type_key
	    from MRK_Marker
	    where _Organism_key = 1
	    and _Marker_Status_key = 1''', 'auto')
   # For a marker M
   #  let M.mt be M's marker type
   #  let M.mcv be the list of M's curated MCV annotations (could be 0 or more)
   # 
   #  if M.mcv is not empty # have curated annotations for this marker
   #	 for each term T in M.mcv  # for each annotation 
   #         # do type checking
   #         let T.mt be the associated marker type of the ancestor of T 
   #		(including T itself) that is associated w/ a marker type
   #	     if there is no such ancestor of T, then let T.mt be empty
   #	     if T.mt not empty and T.mt != M.mt   # M not the right marker type
   #						  # for this MCV term
   #		 report error 
   #		 # Now add an annotation to M's actual marker type so every 
   #		 # marker has an MCV annotation
   #		 find MCV term T2 that is associated with M.mt
   #		 add M annotated to T2 to MRK_MCV_Cache
   #
   #	    else           # no error, add to cache
   #		 add M annotated to T to MRK_MCV_Cache
   #
   #  else   # no curated annotations for this marker
   #	 find MCV term T that is associated with M.mt
   #     add M annotated to T to MRK_MCV_Cache
    for r in results:
	mkrKey = r['_Marker_key']
        mTypeKey = r['_Marker_Type_key']

        # list of VOC_Annot keys for current marker
	annotList = [] # default if there are none
	# list of VOC_Annot terms for current marker
	if mkrKeyToMCVAnnotDict.has_key(mkrKey):
	    annotList = mkrKeyToMCVAnnotDict[mkrKey]
	    #print 'DIRECT annotations %s mkrKey %s mTypeKey %s' % (annotList, mkrKey, mTypeKey)
	annotateToList = processDirectAnnot(annotList, mTypeKey)

	# get the terms  for the direct annotations, every annotation in the cache
	# will have a list of direct terms
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

def processByMarker(markerKey):
    # get the marker type; needed to determine the MCV annotation
    mTypeKey = mkrKeyToMkrTypeKeyDict[markerKey]

    # select all annotations in MRK_MCV_Cache for the specified marker
    results = db.sql('''select *
	into #toprocess
	from MRK_MCV_Cache
	where _Marker_key = %s''' % markerKey, 'auto')
    db.sql('''create index idx1 on #toprocess(_Marker_key)''', None)

    #
    # delete existing cache records for this marker
    #

    db.sql(deleteSQL % markerKey, None)

    #
    # select all annotations for the specified marker
    # determine the direct and indirect annotations
    # and add them to the cache
    results = db.sql('''select distinct a._Term_key
        from VOC_Annot a
        where a._AnnotType_key = 1011
        and a._Qualifier_key = 1614158
	and a._Object_key = %s''' % markerKey, 'auto')
    annotList = [] # default if there are none
    for r in results:
	annotList.append(r['_Term_key'])

    # get the set of direct annotations which includes inferred from marker type where
    # applicable
    annotateToList = processDirectAnnot(annotList, mTypeKey)
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

    # annotations already made so we don't create dups
    annotMadeList = []

    directTerms = string.join(directTermList, ',')
    for a in annotateToList:
	insertCache(markerKey, a, directTerms, DIRECT)
	# Now add indirect associations from the closure
	ancList = descKeyToAncKeyDict[a]
	for ancKey in ancList:
	    if ancKey not in annotMadeList:
		#print 'insertCache(markerKey: %s, ancKey:%s, INDIRECT) ancKey is type: %s' % (markerKey, ancKey, type(ancKey))
	 	annotMadeList.append(ancKey)
		insertCache(markerKey, ancKey, directTerms, INDIRECT)
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
markerKey = None

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
                markerKey = int(opt[1])
        else:
                showUsage()

if server is None or \
    database is None or \
    user is None or \
    password is None or \
    markerKey is None:
	showUsage()

db.set_sqlLogin(user, password, server, database)
db.useOneConnection(1)
db.set_sqlLogFunction(db.sqlLogAll)

init(markerKey)
if markerKey == 0:
    createBCPfile()
else:
    processByMarker(markerKey)
db.useOneConnection(0)

print '%s' % mgi_utils.date()

