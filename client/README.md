Setup
You need grpc installed. Please follow the setup (everything down to before Build the Example) here: https://grpc.io/docs/languages/python/quickstart/

Server
```
python3 -m grpc_tools.protoc -I./protos --python_out=. --grpc_python_out=. ./protos/keyvaluestore.proto # generate necessary files
python3 kvstore_server.py # start the server
```
Client
