import random 
import bisect 
import math

import argparse

latest_percentile = 90
# reads are 0s, writes are 1s
read = 0
write = 1

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

def get_keys(fp):
    keys = []
    lines = [line for line in fp.readlines()]
    return lines

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('-d', '--distribution', dest='distribution', help="distribution from: zipf, uniform, sequential, latest", required=True)
    parser.add_argument('-r', '--read', dest='read_percent', type=int, help="read percentile from 0 to 100. Write percentile will be 100-read percentile", required=True)
    parser.add_argument('-a', '--alpha', dest='alpha', type=int, default=100, help="alpha value for zipf distribution")
    parser.add_argument('-k', '--keyfile', dest='keyfile', type=argparse.FileType('r', encoding='ascii'), help="key file name containing all files in the db", required=True)
    parser.add_argument('-o', '--out', dest='out', type=argparse.FileType('w', encoding='ascii'), help="output file name", required=True)
    parser.add_argument('-n', '--number', dest='n', type=int, help="number of requests in 100s, i.e. n=10 means 1k requests", required=True)
    args = parser.parse_args()
    print(args)
    
    #get keys
    keys = get_keys(args.keyfile)
    queries = [None] * args.n * 100
    #print(keys)
    
    if (args.distribution=='zipf'):
        print("generating zipfan distribution...")
        #for i in range(0,args.n):
        #    args.out.write('abcd')
    elif (args.distribution=='uniform'):
        print("generating uniform distribution...")
        for i in range(0, args.n*100):
            j = random.randint(0,len(keys)-1)
            req_type = int(random.randint(0,100) >= args.read_percent)
            queries[i] = str(req_type)+'['+keys[j]
        for i in range(0, args.n*100):
            args.out.write(queries[i])
            
    elif (args.distribution=='sequential'):
        print("generating sequential distribution...")
        #for i in range(0, args.n*100):
    elif (args.distribution=='latest'):
        print("generating latest distribution...")
    else:
        print('invalid distribution...')
    
    
    #args.out.write('abcd')
    args.out.close()


if __name__ == "__main__":
    main()
