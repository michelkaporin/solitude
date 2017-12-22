#!/usr/bin/python
import sys
sys.path.append('/usr/local/lib/python2.7/site-packages') # Mac OS
import hyperdex.admin, hyperdex.client, timeit
a = hyperdex.admin.Admin('127.0.0.1', 1982)
c = hyperdex.client.Client('127.0.0.1', 1982)

print 'Testing if search with 1 chunk result takes the same time to retrieve as with many chunk results.'

# Create space, if needed
# a.add_space('''
# space chunked
# key time
# attributes data, int temp_skin
# tolerate 2 failures
# ''')

for i in range(1, 500): 
    c.put('chunked', str(i), { 'data': 'test' + str(i), 'temp_skin': 30})

c.put('chunked', '501', { 'data': 'test501', 'temp_skin': 29})

def firstTest():
    return [x for x in c.search('chunked', {'temp_skin': 30})]
def secondTest():
    return [x for x in c.search('chunked', {'temp_skin': 29})]

# print firstTest()
# print secondTest()

timeTaken = timeit.timeit(firstTest, "pass", timeit.default_timer, 100)
print 'First test with time taken:' + str(timeTaken) + 's'

timeTaken = timeit.timeit(secondTest, "pass", timeit.default_timer, 100)
print 'Second test with time taken:' + str(timeTaken) + 's'