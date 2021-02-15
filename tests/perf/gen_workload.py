import random 
import bisect 
import math

import argparse
from functools import reduce

latest_percentile = 90

class ZipfGenerator: 

    def __init__(self, n, alpha): 
        # Calculate Zeta values from 1 to n: 
        tmp = [1. / (math.pow(float(i), alpha)) for i in range(1, n+1)] 
        zeta = reduce(lambda sums, x: sums + [sums[-1] + x], tmp, [0]) 

        # Store the translation map: 
        self.distMap = [x / zeta[-1] for x in zeta] 

    def next(self): 
        # Take a uniform 0-1 pseudo-random value: 
        u = random.random()  

        # Translate the Zipf variable: 
        return bisect.bisect(self.distMap, u) - 1


def main():
    print("hi")
    parser = argparse.ArgumentParser()
    parser.add_argument('-d', '--distribution', dest='distribution', help="distribution from: zipf, uniform, sequential, latest", required=True)
    parser.add_argument('-r', '--read', dest='read_percent', type=int, help="read percentile from 0 to 100. Write percentile will be 100-read percentile", required=True)
    parser.add_argument('-a', '--alpha', dest='alpha', type=int, default=100, help="alpha value for zipf distribution")
    parser.add_argument('-o', '--out', dest='out', type=argparse.FileType('w', encoding='ascii'), help="output file name", required=True)
    parser.add_argument('-n', '--number', dest='n', type=int, help="number of requests in 100s, i.e. n=10 means 1k requests", required=True)
    args = parser.parse_args()
    print(args)
    
    if (args.distribution='zipf'):
        for i in range(0,args.n):
            args.out.write('abcd')
    elif (args.distribution='uniform'):
    elif (args.distribution='sequential'):
    elif (args.distribution='latest'):
    else:
        print('invalid distribution')
    
    
    args.out.write('abcd')
    args.out.close()


if __name__ == "__main__":
    main()
