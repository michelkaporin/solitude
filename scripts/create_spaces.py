#!/usr/bin/python
import sys
sys.path.append('/usr/local/lib/python2.7/site-packages') # Mac OScd ..
import hyperdex.admin
A = hyperdex.admin.Admin('127.0.0.1', 1982)

if len(sys.argv) < 1:
    print "false"
    sys.exit(0)

SPACE_NAMES = sys.argv[1].split(",")
try:
    for space in SPACE_NAMES:
        A.rm_space(space) # remove if exist

        template = '''
        space {0}
        key time
        attributes data, int temp_skin
        tolerate 2 failures
        '''
        A.add_space(template.format(space))

    print "true"
except:
    print "false"