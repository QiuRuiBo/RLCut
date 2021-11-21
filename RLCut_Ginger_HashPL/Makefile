#!/usr/bin/make
# Makefile: makefile for LA cut Model
# (c) Juanyun Luo, 2021

objects= main.o lib.o libgraph.o math.o
all : $(objects)
# g++ -o main -pthread -O3 -fopenmp $(objects)
	g++ -pthread -O3 -o main $(objects)

lib_train.o : libgraph.h lib.h math.h
main.o : libgraph.h lib.h math.h
lib.o: libgraph.h lib.h math.h
libgraph.o : libgraph.h math.h lib.h
math.o : libgraph.h lib.h

clean:
	rm *.o
