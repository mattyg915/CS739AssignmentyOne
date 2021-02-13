### Key value library
- `go get -u github.com/valyala/fasthttp`
- `go build -o lib739kv.so -buildmode=c-shared go_lib.go`
- `gcc -o test_prog test_prog.c -L. -l:lib739kv.so`
- `export LD_LIBRARY_PATH=$PWD`
- `./test_prog`
