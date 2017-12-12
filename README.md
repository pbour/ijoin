NAME
       ij - compute interval overlap join

USAGE
       ij [OPTION]... [FILE1] [FILE2]

DESCRIPTION
       Mandatory arguments

       -a
              join algorithm
       -s
              pre-sorting input files; mandatory only for single-threaded processing
       -b
              number of buckets; default value is 1000; mandatory only for bgFS algorithm
       -h
              use hash-based partitioning for parallel processing
       -d
              use domain-based partitioning for parallel processing
       -t
              number of threads available

       Other arguments

       -u
              looping unrolling enforced; by default is deactivted
       -g
              greedy scheduling activated; valid only with -d option
       -m
              mini-joins breakdown activated; valid only with -d option
       -v
              adaptive partitioning activated; valid only with -d option
       -?
              display this help message and exit

PREREQUISITES
       OpenMP
