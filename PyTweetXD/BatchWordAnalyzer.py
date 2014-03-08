'''
Created on Dec 5, 2013
@author: markito
'''
import DB2
from ConfigParser import ConfigParser
from collections import Counter

config = ConfigParser()
config.read('config')

conn = DB2.connect(config.get('GemFireXD', 'database'), config.get('GemFireXD', 'user'), config.get('GemFireXD', 'password'))

SQL = 'SELECT text FROM tweets'
cursor = conn.cursor()
cursor.execute(SQL)

BATCH_SIZE = 1000
result = cursor.fetchmany(BATCH_SIZE)
wordCounter = Counter()

while result:
    # token list with every word with more than 3 letters on every tweet
    words=[ w
            for t in result
                for w in t[0].split()
                    if len(w) >= 3]
    # count frequency on token list
    for item in [words]:
        c = Counter(item)
        wordCounter = wordCounter + c
        
    try:
        result = cursor.fetchmany(BATCH_SIZE)
    except Exception as e:
        result = None
        print e
        
print wordCounter.most_common()[:10] # top 10 print

if __name__ == '__main__':
    pass