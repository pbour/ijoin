# ijoin
Compute interval overlap joins

## Usage
       $ ij [OPTION]... [FILE1] [FILE2]

## Description
       Mandatory arguments

       -a
              join algorithm
       -s
              pre-sort input files; mandatory only for single-threaded processing
       -b
              number of buckets; default value is 1000; mandatory only for bgFS algorithm
       -h
              use hash-based partitioning for parallel processing
       -d
              use domain-based partitioning for parallel processing
       -t
              number of threads available; mandatory for parallel processing

       Other arguments

       -u
              loop unrolling enforced; by default is deactivated
       -g
              greedy scheduling activated; valid only with -d option
       -m
              mini-joins breakdown activated; valid only with -d option
       -v
              adaptive partitioning activated; valid only with -d option
       -?
              display this help message and exit

## Examples
       Original forward scan-based plane sweep from BrinkhoffKS@SIGMOD96, single-threaded processing
<<<<<<< HEAD
              /ij -a FS -u -s FILE1 FILE2
       Optimized FS with grouping, hash-based parallel processing
              /ij -a gFS -u -h -t 16 FILE1 FILE2
       Optimized FS with grouping and bucket indexing (1000 buckets), domain-based parallel processing under mj+greedy/adaptive setup
              /ij -a bgFS -b 1000 -u -d -m -g -v -t 16 FILE1 FILE2
=======
              ij -a FS -u -s FILE_R FILE_S
       Optimized FS with grouping, hash-based parallel processing
              ij -a gFS -u -h -t 16 FILE_R FILE_S
       Optimized FS with grouping and bucket indexing (1000 buckets), domain-based parallel processing under mj+greedy/adaptive setup
              ij -a bgFS -b 1000 -u -d -m -g -v -t 16 FILE_R FILE_S
>>>>>>> b03dbbe8b254d74f41d79248f8cdbce06175379a
