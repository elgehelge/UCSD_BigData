#!/usr/bin/python
"""
count the number of measurements of each type
"""
import sys
sys.path.append('/usr/lib/python2.6/dist-packages')
from mrjob.job import MRJob
import re
from sys import stderr

class count_measurements(MRJob):
    
    def mapper(self, _, line):
        try:
            self.increment_counter('MrJob Counters','mapper',1)
            # Parse the line
            elements=line.split(',')
            station = elements[0]
            measure_type = elements[1]
            year = elements[2]
            measurements = elements[3:]
            no_of_measurements = sum([1 for m in measurements if m != ""])
            
            # Emit the key-value pair
            if measure_type == 'PRCP' and station != 'station': #filter out the unwanted measurements and the header line
                self.increment_counter('MrJob Counters','usefull lines',1)
                yield (station, no_of_measurements)
                
        except Exception, e:
            stderr.write('Error in line:\n'+line)
            stderr.write(e)
            self.increment_counter('MrJob Counters','mapper-error',1)
            yield (('error','mapper', str(e)), 1)
            
    def reducer(self, word, counts):
        try:
            if word[0] == 'error':
                yield(key,sum(data))
                return
            self.increment_counter('MrJob Counters','reducer',1)
            yield (word, sum(counts))
        except Exception, e:
            yield (('error','reducer', str(e)), 1)
        
    def combiner(self, word, counts):
        try:
            self.increment_counter('MrJob Counters','combiner',1)
            yield (word, sum(counts))
        except Exception, e:
            yield (('error','combiner', str(e)), 1)
        
if __name__ == '__main__':
    count_measurements.run()