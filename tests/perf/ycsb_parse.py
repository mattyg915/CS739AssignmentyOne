import string
import random

#1 ascii char is 1 byte long
keylenMax=128
vallenMax=2048

# reads are 0s, writes are 1s
read = 0
write = 1

ycsb_dir = 'ycsbworloads/'

loads = ['ycsba_load',
        'ycsbb_load',
        'ycsbc_load',
        'ycsbd_load',
        'ycsbf_load']

runs = ['ycsba_run',
        'ycsbb_run',
        'ycsbc_run',
        'ycsbd_run',
        'ycsbf_run']


def main():
    
    #setup key value space
    kvspace=string.digits+string.ascii_letters+string.punctuation#+' '+'\t'
    kvspace=kvspace.replace('[', '').replace(']', '')
    print(kvspace)
    print(''.join(random.choices(kvspace, k = random.randint(1,keylenMax))))
    '''key_dicts = [dict()] * len(loads)

    for i in range(len(loads)):
        #open files
        f = open(ycsb_dir+loads[i], 'r')
        lines = [line for line in f.readlines()]
        keys = [None] * len(lines)
        types = [1] * len(lines)
        for j in range(len(lines)):
            parts = lines[j].split(' ')
            key_num = parts[2]
            
            if key_num not in key_dicts[i]:
                #generate a random key
                keys[j] = ''.join(random.choices(kvspace, k = random.randint(1,keylenMax)))
                key_dicts[key_num] = keys[j]
            else:
                keys[j] = key_dicts[key_num]
        f.close()
        #write workload
        loadf = open(loads[i]+'parsed', 'w')
        for i in range(len(keys)):
            value = ''.join(random.choices(kvspace, k = random.randint(1,vallenMax)))
            loadf.write(types[i]+'['+keys[i]+'['+value+'\n')
        loadf.close()
        #write all keys in sorted order
        keyf = open(loads[i]+'keys', 'w')
        for i in range(len(keys)):
            value = ''
            keyf.write(keys[i]+'\n')
        keyf.close()
        
    for i in range(len(runs)):
        #open files
        f = open(ycsb_dir+runs[i], 'r')
        lines = [line for line in f.readlines()]
        keys = [None] * len(lines)
        types = [None] * len(lines)
        for j in range(len(lines)):
            parts = lines[j].split(' ')
            type = parts[0]
            key_num = parts[2]
            
            types[j] = str(int(type != 'READ'))
            if key_num not in key_dicts[i]:
                #generate a random key
                keys[j] = ''.join(random.choices(kvspace, k = random.randint(1,keylenMax)))
                key_dicts[i][key_num] = keys[j]
                print('error! have not seen this key before!')
            else:
                keys[j] = key_dicts[i][key_num]
        f.close()
        #write workload
        runf = open(runs[i]+'parsed', 'w')
        for i in range(len(keys)):
            value = ''
            if types[i] == '1':
                value = ''.join(random.choices(kvspace, k = random.randint(1,vallenMax)))
            runf.write(types[i]+'['+keys[i]+'['+value+'\n')
        runf.close()'''

if __name__ == "__main__":
    main()
