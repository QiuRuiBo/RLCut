CC=g++
CFLAGS=-I ../boost_1_50_0/boost_1_50_0 -std=c++11 -fopenmp -g -fno-stack-protector
DEPS=Optimizer.cpp GraphEngine.cpp GeoDistrWorkflow.cpp GraphConstruction.cpp CompAlgorithms.cpp Streaming.cpp

GraphRun: $(DEPS)
	$(CC) $(CFLAGS) $(DEPS) -o GraphRun
