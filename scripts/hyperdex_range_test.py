#!/usr/bin/python
import sys
sys.path.append('/usr/local/lib/python2.7/site-packages') # Mac OS
import hyperdex.admin, hyperdex.client, timeit
a = hyperdex.admin.Admin('127.0.0.1', 1982)
c = hyperdex.client.Client('127.0.0.1', 1982)

print 'Testing if the range query from Python API is faster than retrieval of chunk of 1000 entries.' # Test showed that range queries take longer time. Same is confirmed by executing similar test with Java bindings

def firstTest():
    # get timestamp 1473903071 from temp_l3306_h3399 
    return c.get('chunked', 1473903071)
def secondTest():
    # get from chunked2 3306..3399
    return [x for x in c.search('chunked2', {'temp_skin': (3306, 3399)})]

# print firstTest()
# print secondTest()

timeTaken = timeit.repeat(firstTest, "pass", timeit.default_timer, 3, 1)
print 'First test with time taken:' + str(timeTaken) + 's'

timeTaken = timeit.repeat(secondTest, "pass", timeit.default_timer, 3, 1)
print 'Second test with time taken:' + str(timeTaken) + 's'