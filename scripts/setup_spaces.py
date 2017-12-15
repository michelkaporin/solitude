#!/usr/bin/python
import sys

setupArgName = "setup"
destroyArgName = "destroy"

if len(sys.argv) < 1:
    print "Please provide arguments to control hyperdex spaces"
    sys.exit(0)

# Link the Python library
sys.path.append('/usr/local/lib/python2.7/site-packages') # Mac OS
sys.path.append('/usr/local/lib/python2.7/dist-packages') # Linux

import hyperdex.admin
a = hyperdex.admin.Admin('127.0.0.1', 1982)

if sys.argv[1] == setupArgName:
    a.add_space('''
    space chunked
    key time
    attributes data
    tolerate 2 failures
    ''')
    a.add_space('''
    space compressed_c
    key time
    attributes data
    tolerate 2 failures
    ''')
    a.add_space('''
    space encrypted_cc
    key time
    attributes data
    tolerate 2 failures
    ''')


    a.add_space('''
    space chunked2
    key time
    attributes data, int temp_skin
    subspace temp_skin
    tolerate 2 failures
    ''')
    a.add_space('''
    space compressed_c2
    key time
    attributes data, int temp_skin
    subspace temp_skin
    tolerate 2 failures
    ''')
    a.add_space('''
    space encrypted_cc2
    key time
    attributes data, int temp_skin
    subspace temp_skin
    tolerate 2 failures
    ''')
elif sys.argv[1] == destroyArgName:
    print "I'll destroy everything"
else:
    print "Provided argument is not valid"
    sys.exit(0)