To generate db
```
python3 gen_db.py -o outfile -n 10
```
Number of entires=n*100. Run without args to see argument details.

To gen trace


To send generated db to server
```
gcc -o test_prog perftest.c -L. -l:lib739kv.so
./test_prog db.lst
```
Default server connection at localhost:5000
