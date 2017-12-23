#!/usr/bin/python
import sys
# Link the Python library
sys.path.append('/usr/local/lib/python2.7/site-packages') # Mac OS
sys.path.append('/usr/local/lib/python2.7/dist-packages') # Linux

import hyperdex.admin
A = hyperdex.admin.Admin('127.0.0.1', 1982)

try:
    A.rm_space('chunked')
    A.rm_space('compressed_c')
    A.rm_space('encrypted_cc')
    A.rm_space('chunked2')
    A.rm_space('compressed_c2')
    A.rm_space('encrypted_cc2')
except:
    print "spaces do not exist"

try:
    A.add_space('''
    space chunked
    key time
    attributes data
    tolerate 2 failures
    ''')
    A.add_space('''
    space compressed_c
    key time
    attributes data
    tolerate 2 failures
    ''')
    A.add_space('''
    space encrypted_cc
    key time
    attributes data
    tolerate 2 failures
    ''')

    A.add_space('''
    space chunked2
    key time
    attributes data, int temp_skin
    subspace temp_skin
    tolerate 2 failures
    ''')
    A.add_space('''
    space compressed_c2
    key time
    attributes data, int temp_skin
    subspace temp_skin
    tolerate 2 failures
    ''')
    A.add_space('''
    space encrypted_cc2
    key time
    attributes data, int temp_skin
    subspace temp_skin
    tolerate 2 failures
    ''')
    print "Hyperdex spaces were created"
except:
    print "Hyperdex spaces could not be created"
    