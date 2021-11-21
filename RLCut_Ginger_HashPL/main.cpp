//#define CRTDBG_MAP_ALLOC
#include <stdio.h>
#include <stdlib.h>
#include <chrono>
//#include<crtdbg.h>
#include <iostream>
#include <fstream>
#include <cstring>
#include <cmath>
#include "libgraph.h"
#include "lib.h"
#include "math.h"
using namespace std;
#define N 999
#pragma warning(disable : 4996)

int main(int argc, char *argv[])
{
	if (!graph)
		return -1;
	if (argc != 15)
	{
		cout << "The number of parameters entered is incorrect, please re-enter!" << endl; // ./main Model app input network dcnum budget theta threads outputfilename randpro trainedfile bsp batchsize L n(app_iter*called) ginger_cost k c
		return -1;
	}

	printf("*	Model: %s\n", argv[1]);
	printf("*	App: %s\n", argv[2]);
	printf("*	Graph file: %s\n", argv[3]);
	printf("*	Network file: %s\n", argv[4]);
	printf("*	DC num: %s\n", argv[5]);
	printf("*	Budget(percent): %s\n", argv[6]);
	printf("*	Theta: %s\n", argv[7]);
	printf("*	Threads: %s\n", argv[8]);
	printf("*	Output file: %s\n", argv[9]);
	printf("*	Randpro: %s\n", argv[10]);
	printf("*	Trained file: %s\n", argv[11]);
	printf("*	BSP: %s\n", argv[12]);
	printf("*	Batchsize: %s\n", argv[13]);
	printf("*	Overhead: %s\n", argv[14]);

	if (!(strcmp(argv[1], "RLcut") == 0 || strcmp(argv[1], "HashPL") == 0 || strcmp(argv[1], "Ginger") == 0))
	{
		cout << "Model error!" << endl;
		exit(-1);
	}

	strcpy(outputfile, "");
	strcat(outputfile, argv[9]);

	char filename_graph[100];
	char filename_network[100];
	char application[100];
	bool directed = true;
	strcpy(filename_graph, argv[3]);
	strcpy(filename_network, argv[4]);
	strcpy(application, argv[2]);
	budget = atof(argv[6]);
	// ginger_cost = atof(argv[16]);

	L = atof(argv[14]);
	if (budget == 0)
	{
		cout << "Sorry, the budget is too low!" << endl;
		return 0;
	}
	stepbudget = 0;
	addbudget = budget / MAX_ITER;

	randpro = atof(argv[10]);
	;

	if (strcmp(application, "pagerank") == 0)
		dataunit = 0.000008;
	else if (strcmp(application, "sssp") == 0)
		dataunit = 0.000004;
	else if (strcmp(application, "subgraph") == 0)
	{
		dataunit = 0.001;
		directed = false;
	}
	else
	{
		cout << "application error!" << endl;
		exit(-1);
	}
	int status = read_input_file(filename_graph, directed);
	if (status == -1)
	{
		cout << "Graph initial is error!" << endl;
		exit(-1);
	}

	theta = atoi(argv[7]);
	bsp = atoi(argv[12]);
	// mvpercents = atoi(argv[15]);
	batchsize = atoi(argv[13]);
	bspvec = new vector<Pthread_args>[batchsize];
	MAX_THREADS_NUM = atoi(argv[8]);
	int numDCs = atoi(argv[5]);
	initAlgorithm("RLcut-hybrid", numDCs);
	graph->initMirrors(numDCs, graph->getNum_Nodes());
	double *upload = new double[algorithm->getDCNums()];
	double *download = new double[algorithm->getDCNums()];
	double *upprice = new double[algorithm->getDCNums()];
	ug = new double[algorithm->DCnums];
	dg = new double[algorithm->DCnums];
	ua = new double[algorithm->DCnums];
	da = new double[algorithm->DCnums];

	mirror_mutex = new pthread_mutex_t *[algorithm->getDCNums()];
	for (int i = 0; i < algorithm->getDCNums(); i++)
		mirror_mutex[i] = new pthread_mutex_t[graph->getNum_Nodes()];

	status = read_input_network(filename_network, upload, download, upprice);
	if (status)
	{
		cout << "network definition is error!" << endl;
		exit(-1);
	}
	network->initNetwork(upload, download, upprice);
	optimize = new int[MAX_ITER];
	for (int i = 0; i < MAX_ITER; i++)
		optimize[i] = 1;

	Vertex *v = graph->getVertexs();
	long long *mapped = graph->getMapped();
	if (strcmp(argv[1], "HashPL") == 0)
	{
		time_t start = time(NULL);
		cout << "--------------------HashPL---------------------" << endl;
		for (int i = 0; i < graph->getNum_Nodes(); i++)
		{
			int dcnum = algorithm->getDCNums();
			int label = i % dcnum;
			v[i].setLabel(label);
		}
		overhead = time(NULL) - start;
		initPartitioning();
		powerlyrareCalTimeAndCost(data_transfer_time, data_transfer_cost);
		cout << "#data_transfer_time: " << data_transfer_time << ";  #data_transfer_cost: " << data_transfer_cost << endl;

		Simulate();
		cout << "Overhead: " << overhead << " seconds!" << endl;
	}
	else if (strcmp(argv[1], "Ginger") == 0)
	{
		//clock_t start=clock();
		// ofstream out;
		// out.open(outputfile);
		// out << filename_graph << endl;

		outfile.open(outputfile);
		if (!outfile.is_open())
		{
			cout << "can not open output file!";
			exit(-1);
		}

		time_t start = time(NULL);

		cout << "--------------------Ginger---------------------" << endl;
		algorithm->InitLabel();
		int dc[algorithm->getDCNums()];
		for (int i = 0; i < algorithm->getDCNums(); i++)
			dc[i] = 0;
		for (int i = 0; i < graph->getNum_Nodes(); i++)
		{
			dc[v[i].label]++;
		}
		for (int i = 0; i < algorithm->getDCNums(); i++)
			printf("DC %d --- node : %d\tuG : %.16f\tuA : %.16f\tdG : %.16f\tdA : %.16f\n",
				   i, dc[i], ug[i], ua[i], dg[i], da[i]);
		double *score = new double[algorithm->getDCNums()];
		double *load = new double[algorithm->getDCNums()];
		double *eload = new double[algorithm->getDCNums()];
		double *locality = new double[algorithm->getDCNums()];
		double *proc_balance = new double[algorithm->getDCNums()];
		list<Edge *> oedges;
		list<Edge *> iedges;
		list<Edge *>::iterator it;
		double vtoe = (double)graph->getNum_Nodes() / (double)graph->getNum_Edges();
		int low = 0, high = 0;
		double gamma = 1.5;
		double alpha = sqrt(algorithm->getDCNums()) * double(graph->getNum_Edges()) / pow(graph->getNum_Nodes(), gamma);
		for (int i = 0; i < graph->getNum_Nodes(); i++)
		{
			if (v[i].getIngoingEdges().size() >= theta)
			{
				v[i].setType(0);
				high++;
			}
			else
			{
				v[i].setType(1);
				low++;
			}
		}
		start = time(NULL);
		for (int j = 0; j < algorithm->getDCNums(); j++)
		{
			load[j] = 0;
			eload[j] = 0;
			proc_balance[j] = 0;
		}
		for (int i = 0; i < (graph->getNum_Nodes()) / 1 && (rand() % (N + 1) / (float)(N + 1) <= 1); i++) //lalalal
		{
			int followedges = 0;
			for (int j = 0; j < algorithm->getDCNums(); j++)
			{
				score[j] = 0;
				locality[j] = 0;
			}
			if (v[i].getType() == 1)
			{
				iedges = v[i].getIngoingEdges();
				for (it = iedges.begin(); it != iedges.end(); it++)
				{
					followedges++;
					long long sourceId = mapped[(*it)->getsourceID()];
					if (sourceId < i)
						locality[v[sourceId].getLabel()]++;
				}
				oedges = v[i].getOutgoingEdges();
				for (it = oedges.begin(); it != oedges.end(); it++)
				{
					long long destId = mapped[(*it)->getdestID()];
					if (v[destId].getType() == 0)
						followedges++;
					if (v[destId].getType() == 0 && destId < i)
						locality[v[destId].getLabel()]++;
				}
			}
			else
			{
				oedges = v[i].getOutgoingEdges();
				for (it = oedges.begin(); it != oedges.end(); it++)
				{
					long long destId = mapped[(*it)->getdestID()];
					if (v[destId].getType() == 0)
						followedges++;
					if (v[destId].getType() == 0 && destId < i)
						locality[v[destId].getLabel()]++;
				}
			}
			double localitysum = 0, balancesum = 0;
			for (int j = 0; j < algorithm->getDCNums(); j++)
			{
				localitysum += locality[j];
				balancesum += (load[j] + vtoe * eload[j]) / 2;
			}
			for (int j = 0; j < algorithm->getDCNums(); j++)
			{
				double p1 = localitysum == 0 ? 0 : locality[j] / localitysum;
				double p2 = balancesum == 0 ? 0 : ((load[j] + vtoe * eload[j]) / 2) / balancesum;
				score[j] = locality[j] - alpha * gamma * pow(proc_balance[j], (gamma - 1));
			}
			int index = max_value_index(score, algorithm->getDCNums());
			v[i].setLabel(index);
			load[index]++;
			eload[index] += followedges;
			proc_balance[index]++;
			proc_balance[index] += followedges * vtoe;
		}
		overhead = time(NULL) - start;

		initPartitioning();
		powerlyrareCalTimeAndCost(data_transfer_time, data_transfer_cost);

		Simulate();
		for (int i = 0; i < algorithm->getDCNums(); i++)
			dc[i] = 0;
		for (int i = 0; i < graph->getNum_Nodes(); i++)
		{
			dc[v[i].label]++;
		}
		for (int i = 0; i < algorithm->getDCNums(); i++)
			printf("DC %d --- node : %d\tuG : %.16f\tuA : %.16f\tdG : %.16f\tdA : %.16f\n",
				   i, dc[i], ug[i], ua[i], dg[i], da[i]);
		delete[] score;
		delete[] load;
		delete[] eload;
		delete[] locality;
		return 0;
	}
	else
	{

		strcpy(FN, argv[11]);

		trained.clear();
		srand(time(NULL));
		Seed = rand();
		srand(Seed);

		for (int i = 0; i < graph->getNum_Nodes(); i++)
		{
			double random = 1. * rand() / RAND_MAX;
			if (random <= randpro)
			{
				trained.insert(i);
			}
		}

		RLcut();
		//RLcut_dynamic()

		delete[] ug;
		delete[] dg;
		delete[] ua;
		delete[] da;
	}
	return 0;
}
