# verlog

This repository includes code implementing of primary-backup replication in the in-memory datastore. In backup, we implement Verlog, Eager(C5), Lazy (Query Fresh). These approaches support strongly consistent reads on backups.



# Compiling & Running

You can compile and run primary and backup as follows: 

```sh
$ cd primary (backup)
$ mkdir build
$ cd build
$ cmake ..
$ make -j
$ ./PRIMARY (BACKUP)
```


# Requirements

- Hardware
  - Mellanox InfiniBand NIC (e.g., ConnectX-5) that supports RDMA
  - Mellanox InfiniBand Switch

- Software
  - Operating System: cnetos 7.9
  - Programming Language: C++ 11
  - CMake: 2.8 or above
  - Libraries: ibverbs, pthread, jemalloc
