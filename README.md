# histos
A tool to quickly draw equi-width histograms on screen

## Usage
       $ ./histos.pl [OPTION]... [FILE]

## Description
       No mandatory arguments besides input FILE

       -h
              display this help message and exit
       -c
              column to build the histogram; default value is 1
       -d
              spliting delimeter in quotes to define columns; by default is set to ','
       -b
              number of bins to partition the domain; default value is 10
       -s
              display scale; default value is 1
       -m
              minimum value contained inside input; computed internally if not given
       -M
              maximum value contained inside input; computed internally if not given
