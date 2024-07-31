all:libnt

libnt: nt.o
	ar rcs libnt.a nt.o

nt:nt.c
	gcc -c nt.c -L. -msse4.1