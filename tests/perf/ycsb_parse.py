import random
import string

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

runs = ['ycsba_load',
        'ycsbb_load',
        'ycsbc_load',
        'ycsbd_load',
        'ycsbf_load',
        'ycsba_run',
        'ycsbb_run',
        'ycsbc_run',
        'ycsbd_run',
        'ycsbf_run']

def main():
    
    #setup key value space
    kvspace=string.digits+string.ascii_letters+string.punctuation#+' '+'\t'
    kvspace=kvspace.replace('[', '').replace(']', '')
    print(kvspace)

    for i in range(len(runs)):
        #open files
        f = open(ycsb_dir+runs[i], 'r')
        lines = [line for line in f.readlines()]
        key_dict = dict()
        keys = [None] * len(lines)
        types = [None] * len(lines)
        for j in range(len(lines)):
            parts = lines[j].split(' ')
            type = parts[0]
            key_num = parts[2]
            
            types[j] = str(int(type != 'READ'))
            if key_num not in key_dict:
                #generate a random key
                keys[j] = ''.join(random.choices(kvspace, k = random.randint(1,keylenMax)))
                key_dict[key_num] = keys[j]
            else:
                keys[j] = key_dict[key_num]
        f.close()
        #write workload
        keyf = open(runs[i]+'keys', 'w')
        for i in range(len(keys)):
            value = ''
            if types[i]:
                value = ''.join(random.choices(kvspace, k = random.randint(1,vallenMax)))
            keyf.write(types[i]+'['+keys[i]+'['+value+'\n')
        keyf.close()
        #write all keys in sorted order

if __name__ == "__main__":
    main()
