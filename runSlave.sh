docker run -it --rm --name lab1-slave -v /home/bouyu/git/CMPE273-Assignment2:/usr/src/myapp -w /usr/src/myapp ubuntu-python3.6-rocksdb-grpc:1.0 python3.6 slave.py 192.168.0.1
