#!/usr/local/bin/python

'''
#
# Purpose:
#
# Create bcp file for MRK_Homology_Cache; a cache table of Marker Homology data
#
# Uses environment variables to determine Server and Database
#
# Usage:
#	mrkhomology.py [markerkey]
#
# If markerkey is provided, then only create the bcp file for that marker.
#
# Processing:
#
# History
#
# 04/04/2011	lec
#	- TR10658/add _Cache_key
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
    table = os.environ['TABLE']
    outDir = os.environ['MRKCACHEBCPDIR']
except:
    table = 'MRK_Homology_Cache'

insertSQL = 'insert into MRK_Homology_Cache values (%s,%s,%s,%s,%s)'

nextMaxKey = 0		# max(_Cache_key)

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
 
def processDeleteReload():
	#
	# Purpose:  processes data for BCP-type processing; aka delete/reload
	# Returns:
	# Assumes:
	# Effects:
	# Throws:
	#

	global nextMaxKey

	print '%s' % mgi_utils.date()

	cacheBCP = open(outDir + '/%s.bcp' % (table), 'w')

	results = db.sql('select h._Class_key, h._Homology_key, h._Refs_key, hm._Marker_key, m._Organism_key ' + \
		'from HMD_Homology h, HMD_Homology_Marker hm, MRK_Marker m ' + \
		'where h._Homology_key = hm._Homology_key ' + \
		'and hm._Marker_key = m._Marker_key ', 'auto')

	for r in results:

	    nextMaxKey = nextMaxKey + 1

	    cacheBCP.write(
		     str(nextMaxKey) + COLDL + \
		     mgi_utils.prvalue(r['_Class_key']) + COLDL + \
		     mgi_utils.prvalue(r['_Homology_key']) + COLDL + \
		     mgi_utils.prvalue(r['_Refs_key']) + COLDL + \
		     mgi_utils.prvalue(r['_Marker_key']) + COLDL + \
		     mgi_utils.prvalue(r['_Organism_key']) + LINEDL)
	    cacheBCP.flush()

	cacheBCP.close()

	print '%s' % mgi_utils.date()

def processByClass(classKey):
	#
	# Purpose:  processes data for a specific homology Class
	# Returns:
	# Assumes:
	# Effects:
	# Throws:
	#

	global nextMaxKey

	#
	# delete existing cache records for this Class
	#

	db.sql('delete from %s where _Class_key = %s' % (table, classKey), None)

	#
	# select all records of specified Class
	#

	results = db.sql('select h._Class_key, h._Homology_key, h._Refs_key, hm._Marker_key, m._Organism_key ' + \
		'from HMD_Homology h, HMD_Homology_Marker hm, MRK_Marker m ' + \
		'where h._Class_key = %s ' % (classKey) + \
		'and h._Homology_key = hm._Homology_key ' + \
		'and hm._Marker_key = m._Marker_key ', 'auto')

	for r in results:

	    nextMaxKey = nextMaxKey + 1

	    db.sql(insertSQL % (
		str(nextMaxKey), \
                mgi_utils.prvalue(r['_Class_key']), \
                mgi_utils.prvalue(r['_Homology_key']), \
                mgi_utils.prvalue(r['_Refs_key']), \
                mgi_utils.prvalue(r['_Marker_key']), \
                mgi_utils.prvalue(r['_Organism_key'])), None)

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
classKey = None

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
		classKey = opt[1]
	else:
		showUsage()

if server is None or \
   database is None or \
   user is None or \
   password is None or \
   classKey is None:
	showUsage()

db.set_sqlLogin(user, password, server, database)
db.useOneConnection(1)
db.set_sqlLogFunction(db.sqlLogAll)

scriptName = os.path.basename(sys.argv[0])

#
# next available primary key
#
    
results = db.sql('select cacheKey = max(_Cache_key) from %s' % (table), 'auto')
for r in results:
    nextMaxKey = r['cacheKey']

if nextMaxKey == None:
    nextMaxKey = 0


# call functions based on the way the program is invoked

if scriptName == 'mrkhomology.py':
    processDeleteReload()

# all of these invocations will only affect a certain subset of data

elif scriptName == 'mrkhomologyByClass.py':
    processByClass(classKey)

db.useOneConnection(0)

print '%s' % mgi_utils.date()

