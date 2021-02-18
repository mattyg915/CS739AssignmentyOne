import random
import argparse
import string

#1 ascii char is 1 byte long
keylenMax=128
vallenMax=2048

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('-o', '--out', dest='out', help="output file name. A file containing all keys and one containing all key-value pairs will be generated", required=True)
    parser.add_argument('-n', '--number', dest='n', type=int, help="number of requests in 100s, i.e. n=10 means 1k requests", required=True)
    args = parser.parse_args()
    print(args)
    
    #open files
    f_key = open(args.out+'_key.lst', 'wb')
    f = open(args.out+'.lst', 'wb')
    #setup key value space
    kvspace=string.digits+string.ascii_letters+string.punctuation#+' '+'\t'
    kvspace=kvspace.replace('[', '').replace(']', '')
    print(kvspace)

    #generate DB
    keys = [None] * args.n*100
    values = [None] * args.n*100
    for i in range(0, args.n*100):
        key = None
        while key in keys:
            key = ''.join(random.choices(kvspace, k = random.randint(1,keylenMax)))
        keys[i] = key
        
        '''value = None
        while value in values:
            value = ''.join(random.choices(kvspace, k = random.randint(1,vallenMax)))'''
        values[i] = ''.join(random.choices(kvspace, k = random.randint(1,vallenMax)))
    #print(str(len(keys))+' keys created...')
    #print(str(len(values))+' values created...')
        
    for i in range(0, args.n*100):
        record = keys[i] + '['+values[i]+'\n'
        
        f_key.write((keys[i]+'\n').encode('ascii'))
        f.write(record.encode('ascii'))
    
    #close files
    f.close()
    f_key.close()


if __name__ == "__main__":
    main()
