#include <iostream>
#include <string>
#include <cstdlib>
#include <fstream>
#include <fstream>
#include <time.h>
#include <cstring>
#include <iomanip>
#include <chrono>
#include <queue>
#include <cmath>
#include <vector>
#include <algorithm>
#include <sstream>
#include "libgraph.h"
#include "lib.h"
#include "math.h"
#include <atomic>
#include <chrono>
// #include <omp.h>

using namespace std;
using std::chrono::high_resolution_clock;
using std::chrono::milliseconds;
using std::chrono::nanoseconds;
#pragma warning(disable : 4996)
Graph *graph = new Graph();
vector<Pthread_args> *bspvec = NULL;
Algorithm *algorithm = new Algorithm();
Network *network = new Network();
double budget = 8.70E+03;
double overhead = 0.0;
int Seed = 0;
double alphaT = 0;
double L = 0;
double betaC = 0;
double ginger_cost = 0;
atomic_int bsplock(0);
atomic_int ready_for_move_thread(0);
atomic_int finish_thread(0);
// int ready_for_move_thread = 0;
// int finish_thread = 0;
atomic_int ready_thread(0);
long mvpercents = 1;
double t1 = 0;
double t3 = 0;
double t4 = 0;
int bsp = 0;
int F1 = 0;
int F2 = 0;
double global_mvcost = 0;
double current_mvcost = 0;
double moverand = 0;
int batchsize = 0;
float raw_data_size = 10; //MB;
double *ug = NULL;
double *dg = NULL;
double *ua = NULL;
double *da = NULL;
int optitime = 1;
char *FN = new char[100];
int MAX_ITER = 10;
int MAX_THREADS_NUM = 50;
double data_transfer_time = 0;
double data_transfer_cost = 0;
double timeOld = 0;
double priceOld = 0;
double dataunit = 0.000008;
int theta = 0;
double t5 = 0;
double t2 = 0;
double t6 = 0;
ofstream outfile;
unordered_set<int> trained = unordered_set<int>();
unordered_set<int> retrain_node;

unordered_map<int, vector<int>> PSS = unordered_map<int, vector<int>>();
double stepbudget = 0;
double addbudget = 0;
double randpro = 1;
int *optimize = NULL;
char *outputfile = new char[200];
pthread_barrier_t barrier;
pthread_mutex_t mutex;
pthread_mutex_t mutex1;
pthread_mutex_t *DC_mutex;
pthread_mutex_t **mirror_mutex;
pthread_mutex_t *sum_mutex;

vector<vector<double>> bsp_uploadG;
vector<vector<double>> bsp_uploadA;
vector<vector<double>> bsp_downloadG;
vector<vector<double>> bsp_downloadA;
vector<vector<pair<int, int>>> bsp_assign;

vector<atomic_int> atomic_uploadG;
vector<atomic_int> atomic_uploadA;
vector<atomic_int> atomic_downloadG;
vector<atomic_int> atomic_downloadA;

int k_degree = -1;
double train_edge_rate = 0.4;

double my_sp_rate = 0;

#include <semaphore.h>
sem_t ready;
sem_t bsp_begin;
sem_t bsp_batch;

double c = 1.6;

struct A
{
	int key;
	int value;
};
struct cmp1
{
	bool operator()(A &a, A &b)
	{
		return a.value > b.value; //min first
	}
};

void multiprocessing_pool(int *eachThreadsNum)
{
	Vertex *v = graph->getVertexs();
	int k = graph->getNum_Nodes() % MAX_THREADS_NUM;
	int num = floor((double)(graph->getNum_Nodes() / MAX_THREADS_NUM));
	for (int i = 0; i < MAX_THREADS_NUM; i++)
		if (i < k)
			eachThreadsNum[i] = num + 1;
		else
			eachThreadsNum[i] = num;

	int sum = 0;
	for (int i = 0; i < MAX_THREADS_NUM; i++)
	{
		int low = 0, high = 0;
		for (int j = sum; j < sum + eachThreadsNum[i]; j++)
		{
			if (v[j].getType() == 0)
				high++;
			else
				low++;
		}
		sum += eachThreadsNum[i];
	}
	vector<int> *vec = new vector<int>[MAX_THREADS_NUM];
	int *veclow = new int[MAX_THREADS_NUM];
	int *vechigh = new int[MAX_THREADS_NUM];
	for (int i = 0; i < MAX_THREADS_NUM; i++)
	{
		veclow[i] = 0;
		vechigh[i] = 0;
	}
	time_t start = time(NULL);
	for (int i = 0; i < graph->getNum_Nodes(); i++)
	{
		if (v[i].getType() == 0)
		{
			int min_index = min_value_index(vechigh, MAX_THREADS_NUM);
			vechigh[min_index]++;
			vec[min_index].push_back(i);
		}
		else
		{
			int min_index = min_value_index(veclow, MAX_THREADS_NUM);
			veclow[min_index]++;
			vec[min_index].push_back(i);
		}
	}
	double end = time(NULL) - start;
	cout << "overhead on addressing straggle is: " << end * MAX_ITER << endl;
	for (int i = 0; i < MAX_THREADS_NUM; i++)
	{
		PSS.insert(pair<int, vector<int>>(i, vec[i]));
	}

	delete[] vec;
	delete[] veclow;
	delete[] vechigh;
}

char *input_format_specifier(char *filename, char *delimiter)
{
	string line;
	char *data;
	int count = 0;
	ifstream in(filename);
	getline(in, line);
	data = (char *)line.data();
	char *token = strtok(data, delimiter);
	while (token)
	{
		token = strtok(NULL, delimiter);
		count++;
	}
	if (count == 1)
	{
		return NULL;
	}
	char *format = new char[15];
	switch (count)
	{
	case 2:
		strcpy(format, "%u %u");
		format[5] = '\0';
		break;
	case 3:
		strcpy(format, "%u %u %f");
		format[8] = '\0';
		break;
	}
	return format;
}

void initialNode(FILE *file_descriptor, char *format) //��������ļ�ֻ������
{
	int offset = ftell(file_descriptor);
	int maxVertexID = 0;
	unsigned int source = 0;
	unsigned int max_source = 0;
	unsigned int destination = 0;
	unsigned int max_destination = 0;
	while (fscanf(file_descriptor, format, &source, &destination) == 2)
	{
		graph->addEdge();
		if (source > max_source)
			max_source = source;
		if (destination > max_destination)
			max_destination = destination;
	}
	if (max_source > max_destination)
		maxVertexID = max_source;
	else
		maxVertexID = max_destination;
	cout << "max: " << maxVertexID << endl;
	//getchar();

	fseek(file_descriptor, offset, SEEK_SET);

	int *active = new int[maxVertexID + 1];
	for (int i = 0; i <= maxVertexID; i++)
		active[i] = 0;

	while (fscanf(file_descriptor, format, &source, &destination) == 2)
	{
		if (active[source] == 0)
		{
			graph->addNode();
			active[source] = 1;
		}
		if (active[destination] == 0)
		{
			graph->addNode();
			active[destination] = 1;
		}
	}

	fseek(file_descriptor, offset, SEEK_SET);
	int num_nodes = graph->getNum_Nodes();
	Vertex *v = new Vertex[num_nodes];
	int count = 0;
	long long *mapped = new long long[maxVertexID + 1];
	while (fscanf(file_descriptor, format, &source, &destination) == 2)
	{
		if (active[source] == 1)
		{
			v[count].setSignal(0);
			v[count++].setVertexID(source);
			mapped[source] = count - 1;
			active[source] = 0;
		}
		if (active[destination] == 1)
		{
			v[count].setSignal(0);
			v[count++].setVertexID(destination);
			mapped[destination] = count - 1;
			active[destination] = 0;
		}
	}
	graph->setMapped(mapped);
	fseek(file_descriptor, offset, SEEK_SET);
	graph->setNodes(v);

	cout << "#num_nodes: " << graph->getNum_Nodes() << endl;
	cout << "#num_edges: " << graph->getNum_Edges() << endl;
	delete[] active;
}
void initialNode(FILE *file_descriptor, char *format, double edgeValue)
{
	int offset = ftell(file_descriptor);
	int maxVertexID = 0;
	unsigned int source = 0;
	unsigned int max_source = 0;
	unsigned int destination = 0;
	unsigned int max_destination = 0;
	double edge_value = 0;
	while (fscanf(file_descriptor, format, &source, &destination, &edgeValue) == 3)
	{
		graph->addEdge();
		if (source > max_source)
			max_source = source;
		if (destination > max_destination)
			max_destination = destination;
	}
	if (max_source > max_destination)
		maxVertexID = max_source;
	else
		maxVertexID = max_destination;

	fseek(file_descriptor, offset, SEEK_SET);

	int *active = new int[maxVertexID + 1];
	for (int i = 0; i <= maxVertexID; i++)
		active[i] = 0;

	while (fscanf(file_descriptor, format, &source, &destination, &edgeValue) == 3)
	{
		if (active[source] == 0)
		{
			graph->addNode();
			active[source] = 1;
		}
		if (active[destination] == 0)
		{
			graph->addNode();
			active[destination] = 1;
		}
	}

	fseek(file_descriptor, offset, SEEK_SET);
	int num_nodes = graph->getNum_Nodes();
	Vertex *v = new Vertex[num_nodes];
	int count = 0;
	long long *mapped = new long long[maxVertexID + 1];
	while (fscanf(file_descriptor, format, &source, &destination, &edgeValue) == 3)
	{
		if (active[source] == 1)
		{
			v[count].setSignal(0);
			v[count++].setVertexID(source);
			mapped[source] = count - 1;
			active[source] = 0;
		}
		if (active[destination] == 1)
		{
			v[count].setSignal(0);
			v[count++].setVertexID(destination);
			mapped[destination] = count - 1;
			active[destination] = 0;
		}
	}
	graph->setMapped(mapped);
	fseek(file_descriptor, offset, SEEK_SET);
	graph->setNodes(v);

	cout << "#num_nodes: " << graph->getNum_Nodes() << endl;
	cout << "#num_edges: " << graph->getNum_Edges() << endl;
	delete[] active;
}
void initialEdge(FILE *file_descriptor, char *format, bool directed)
{
	int offset = ftell(file_descriptor);
	unsigned int source = 0;
	unsigned int destination = 0;
	long long *mapped = graph->getMapped();
	Vertex *v = graph->getVertexs();
	double edgeValue = 0;
	int a = 1;
	int b = 100;
	double random = 0;
	while (fscanf(file_descriptor, format, &source, &destination) == 2)
	{
		random = (rand() % (b - a + 1)) + a;
		Edge *e = new Edge(source, destination, random);

		/*if you want to use dynamic part, please turn on here*/

		// if (1. * rand() / RAND_MAX < 0.30)
		// {
		// 	not_load_edge.push_back(e);
		// 	continue;	
		// }

		v[mapped[source]].addOutgoingEdge(e);
		v[mapped[destination]].addIngoingEdge(e);

		if (!directed)
		{
			e = new Edge(destination, source, random);
			v[mapped[source]].addIngoingEdge(e);
			v[mapped[destination]].addOutgoingEdge(e);
		}
	}
	fseek(file_descriptor, offset, SEEK_SET);
}
void initialEdge(FILE *file_descriptor, char *format, double edgeWeight, bool directed)
{
	int offset = ftell(file_descriptor);
	unsigned int source = 0;
	unsigned int destination = 0;
	long long *mapped = graph->getMapped();
	Vertex *v = graph->getVertexs();
	double edgeValue = 0;
	int a = 1;
	int b = 100;
	while (fscanf(file_descriptor, format, &source, &destination, &edgeValue) == 3)
	{
		Edge *e = new Edge(source, destination, edgeValue);
		v[mapped[source]].addOutgoingEdge(e);
		v[mapped[destination]].addIngoingEdge(e);

		if (!directed)
		{
			e = new Edge(destination, source, edgeValue);
			v[mapped[source]].addIngoingEdge(e);
			v[mapped[destination]].addOutgoingEdge(e);
		}
	}
	fseek(file_descriptor, offset, SEEK_SET);
}
int read_input_network(char *filename, double *upload, double *download, double *upprice)
{
	FILE *file_descriptor = fopen(filename, "r");
	if (!file_descriptor)
	{
		cout << "Error : can't open the input file!" << endl;
		return -1;
	}
	float uload = 0;
	float dload = 0;
	float uprice = 0;
	char format[] = "%f %f %f";
	int count = 0;
	while (fscanf(file_descriptor, format, &uload, &dload, &uprice) == 3)
	{
		upload[count] = uload;
		download[count] = dload;
		upprice[count++] = uprice;
	}
	return 0;
}
int read_input_file(char *filename, bool directed)
{
	FILE *file_descriptor = fopen(filename, "r");
	double edgeValue = 0;
	if (!file_descriptor)
	{
		cout << "Error : can't open the input file!" << endl;
		return -1;
	}

	char delimiter[] = " ,'\t'";
	char *format = input_format_specifier(filename, delimiter);

	if (!format)
	{
		cout << "input file format error" << endl;
		return -1;
	}
	if (strcmp(format, "%u %u") == 0)
	{
		initialNode(file_descriptor, format);
	}
	else if (strcmp(format, "%u %u %f") == 0)
	{
		initialNode(file_descriptor, format, edgeValue);
	}

	int offset = ftell(file_descriptor);
	if (strcmp(format, "%u %u") == 0)
	{
		initialEdge(file_descriptor, format, directed);
	}
	else if (strcmp(format, "%u %u %f") == 0)
	{
		initialEdge(file_descriptor, format, edgeValue, directed);
	}
	return 0;
}
void initAlgorithm(char *algorithmName, int numDCs)
{
	algorithm->InitAlgorithm(algorithmName, numDCs); //����ָ��ռ�
	algorithm->InitLabel();							 //�������
	algorithm->InitProbability();					 //��ʼ������
}
void initPartitioning()
{
	list<Edge *> oedges;
	list<Edge *> iedges;
	list<Edge *>::iterator it;
	long long *mapped = graph->getMapped();
	Vertex *v = graph->getVertexs();
	double *Upprice = network->getUpprice();
	Mirror **mirrors = graph->getMirrors();
	double max_price_dc = max_value_index(Upprice, algorithm->DCnums);
	for (int i = 0; i < algorithm->getDCNums(); i++)
		for (int j = 0; j < graph->getNum_Nodes(); j++)
		{
			mirrors[i][j].reset();
		}

	for (int i = 0; i < graph->getNum_Nodes(); i++)
	{
		if (v[i].getIngoingEdges().size() >= theta)
			v[i].setType(0);
		else
			v[i].setType(1);

		if (v[i].iniLabel != max_price_dc)
			global_mvcost += raw_data_size * Upprice[v[i].iniLabel];
	}

	for (int i = 0; i < algorithm->getDCNums(); i++)
	{
		for (int j = 0; j < graph->getNum_Nodes(); j++)
		{
			//cout<<j<<endl;
			if (v[j].getLabel() == i && v[j].getType() == 1)
			{
				iedges = v[j].getIngoingEdges();
				for (it = iedges.begin(); it != iedges.end(); it++)
				{
					if (v[mapped[(*it)->getsourceID()]].getLabel() != i)
					{
						mirrors[i][mapped[(*it)->getsourceID()]].setID(mapped[(*it)->getsourceID()]);
						mirrors[i][mapped[(*it)->getsourceID()]].addOutD();
					}
				}
				oedges = v[j].getOutgoingEdges();
				for (it = oedges.begin(); it != oedges.end(); it++)
				{
					if (v[mapped[(*it)->getdestID()]].getLabel() != i && v[mapped[(*it)->getdestID()]].getType() == 0)
					{
						mirrors[i][mapped[(*it)->getdestID()]].setID(mapped[(*it)->getdestID()]);
						mirrors[i][mapped[(*it)->getdestID()]].addInD();
					}
				}
			}
			else if (v[j].getLabel() == i && v[j].getType() == 0)
			{
				oedges = v[j].getOutgoingEdges();
				for (it = oedges.begin(); it != oedges.end(); it++)
				{
					if (v[mapped[(*it)->getdestID()]].getLabel() != i && v[mapped[(*it)->getdestID()]].getType() == 0)
					{
						mirrors[i][mapped[(*it)->getdestID()]].setID(mapped[(*it)->getdestID()]);
						mirrors[i][mapped[(*it)->getdestID()]].addInD();
					}
				}
			}
		}
	}
	for (int j = 0; j < graph->getNum_Nodes(); j++)
	{
		if (v[j].type == 1) //low-degree
		{
			v[j].idegreeinlabel = v[j].ingoingEdges.size();
			v[j].odegreeinlabel = 0;

			oedges = v[j].outgoingEdges;
			for (it = oedges.begin(); it != oedges.end(); it++)
			{
				if (v[mapped[(*it)->destID]].type == 1 && v[mapped[(*it)->destID]].label == v[j].label)
					v[j].odegreeinlabel++;
				else if (v[mapped[(*it)->destID]].type == 0)
					v[j].odegreeinlabel++;
			}
		}
		else
		{
			iedges = v[j].ingoingEdges;
			v[j].idegreeinlabel = 0;
			v[j].odegreeinlabel = 0;
			for (it = iedges.begin(); it != iedges.end(); it++)
			{

				long long sourceId = mapped[(*it)->sourceID];
				if (v[sourceId].label == v[j].label)
					v[j].idegreeinlabel++;
			}
			oedges = v[j].outgoingEdges;
			for (it = oedges.begin(); it != oedges.end(); it++)
			{
				long long destId = mapped[(*it)->destID];
				if (v[destId].type == 0 || (v[destId].type == 1 && v[destId].label == v[j].label))
					v[j].odegreeinlabel++;
			}
		}
	}
}

void Simulate()
{

	list<Edge *> oedges;
	list<Edge *> iedges;
	list<Edge *>::iterator it;
	long long *mapped = graph->getMapped();
	double *Upprice = network->getUpprice();
	Vertex *v = graph->getVertexs();
	int *numOfVIM = new int[algorithm->getDCNums()]; //The number of vertices in each DC, including mirror
	int *numOfV = new int[algorithm->getDCNums()];	 //The number of vertices per DC, excluding mirror
	int *numOfE = new int[algorithm->getDCNums()];	 //The number of edges per DC
	double *up_gather = new double[algorithm->getDCNums()];
	double *down_gather = new double[algorithm->getDCNums()];
	double *up_apply = new double[algorithm->getDCNums()];
	double *down_apply = new double[algorithm->getDCNums()];
	double movecost = 0;

	Mirror **mirrors = graph->getMirrors();
	for (int i = 0; i < algorithm->getDCNums(); i++)
	{
		numOfVIM[i] = 0;
		numOfV[i] = 0;
		numOfE[i] = 0;
		up_gather[i] = 0;
		up_apply[i] = 0;
		down_gather[i] = 0;
		down_apply[i] = 0;
	}
	double total_move_cost = 0;
	double max_price_dc = max_value_index(Upprice, algorithm->DCnums);

	for (int i = 0; i < graph->getNum_Nodes(); i++) //Count the number of masters and edges of each DC
	{
		numOfVIM[v[i].getLabel()]++;
		numOfV[v[i].getLabel()]++;
		if (v[i].getType() == 1) //Handling low-degree edges
		{
			numOfE[v[i].getLabel()] += v[i].getIngoingEdges().size();
			oedges = v[i].getOutgoingEdges();
			for (it = oedges.begin(); it != oedges.end(); it++)
			{
				long long destId = mapped[(*it)->getdestID()];
				if (v[destId].getType() == 0)
					numOfE[v[i].getLabel()]++;
			}
		}
		else //Handling high-degree points
		{
			oedges = v[i].getOutgoingEdges();
			for (it = oedges.begin(); it != oedges.end(); it++)
			{
				long long destId = mapped[(*it)->getdestID()];
				if (v[destId].getType() == 0)
					numOfE[v[i].getLabel()]++;
			}
		}
		if (v[i].iniLabel != v[i].label)
			movecost += raw_data_size * Upprice[v[i].iniLabel];

		if (v[i].iniLabel != max_price_dc)
			total_move_cost += raw_data_size * Upprice[v[i].iniLabel];
	}

	for (int i = 0; i < algorithm->getDCNums(); i++)
		for (int j = 0; j < graph->getNum_Nodes(); j++)
		{
			if (mirrors[i][j].getID() != -1)
				numOfVIM[i]++;
		}
	long numofver = 0;
	for (int i = 0; i < algorithm->getDCNums(); i++)
		numofver += numOfVIM[i];
	//cout<<"-----------------------------------------------"<<endl;
	outfile << "-----------------------------------------------" << endl;
	cout << "#vertex replication: " << (double)numofver / graph->getNum_Nodes() << endl;
	cout << "#edge replication: " << 1 << endl;

	outfile << "#vertex replication: " << (double)numofver / graph->getNum_Nodes() << endl;
	outfile << "#edge replication: " << 1 << endl;

	long maxV = numOfV[0], minV = numOfV[0];
	long maxE = numOfE[0], minE = numOfE[0];

	for (int i = 0; i < algorithm->getDCNums(); i++)
	{
		if (maxV < numOfV[i])
			maxV = numOfV[i];
		if (minV > numOfV[i])
			minV = numOfV[i];
		if (maxE < numOfE[i])
			maxE = numOfE[i];
		if (minE > numOfE[i])
			minE = numOfE[i];
	}

	cout << "#vertex diff: " << maxV - minV << endl;
	cout << "#edge diff: " << maxE - minE << endl;

	outfile << "#vertex diff: " << maxV - minV << endl;
	outfile << "#edge diff: " << maxE - minE << endl;

	double wan_usage = 0;
	if (budget < 0) //for powerlyra
	{
		/**************Gather**************************/
		for (int i = 0; i < algorithm->getDCNums(); i++)
		{
			for (int j = 0; j < graph->getNum_Nodes(); j++)
				if (mirrors[i][j].getID() != -1 && v[j].getType() == 0)
				{
					wan_usage += dataunit;
					wan_usage += dataunit;
					up_gather[i] += dataunit;
					down_gather[v[j].getLabel()] += dataunit;
				}
		}
		/**************Apply**************************/
		for (int i = 0; i < algorithm->getDCNums(); i++)
		{
			for (int j = 0; j < graph->getNum_Nodes(); j++)
			{
				if (mirrors[i][j].getID() != -1)
				{
					wan_usage += dataunit;
					wan_usage += dataunit;
					down_apply[i] += dataunit;
					up_apply[v[j].getLabel()] += dataunit;
				}
			}
		}
	}
	else //for LA-cut
	{
		/**************Gather**************************/
		for (int i = 0; i < algorithm->getDCNums(); i++)
		{
			for (int j = 0; j < graph->getNum_Nodes(); j++)
				if (mirrors[i][j].getID() != -1 && v[j].getType() == 0 && mirrors[i][j].getInD() != 0)
				{
					wan_usage += dataunit;
					wan_usage += dataunit;
					up_gather[i] += dataunit;
					down_gather[v[j].getLabel()] += dataunit;
				}
		}
		/**************Apply**************************/
		for (int i = 0; i < algorithm->getDCNums(); i++)
		{
			for (int j = 0; j < graph->getNum_Nodes(); j++)
			{
				if (mirrors[i][j].getID() != -1 && mirrors[i][j].getOutD() != 0)
				{
					wan_usage += dataunit;
					wan_usage += dataunit;
					down_apply[i] += dataunit;
					up_apply[v[j].getLabel()] += dataunit;
				}
			}
		}
	}
	cout << "-----------------------------------------------" << endl;
	outfile << "-----------------------------------------------" << endl;
	if (budget >= 0) //for LA-cut
	{
		for (int i = 0; i < algorithm->getDCNums(); i++)
		{
			cout << "DC  " << i << " : uploadG: " << up_gather[i] << endl;
			outfile << "DC  " << i << " : uploadG: " << up_gather[i] << endl;
		}
		cout << "-----------------------------------------------" << endl;
		outfile << "-----------------------------------------------" << endl;
		for (int i = 0; i < algorithm->getDCNums(); i++)
		{
			cout << "DC  " << i << " : downloadG: " << down_gather[i] << endl;
			outfile << "DC  " << i << " : downloadG: " << down_gather[i] << endl;
		}
		cout << "-----------------------------------------------" << endl;
		outfile << "-----------------------------------------------" << endl;
		for (int i = 0; i < algorithm->getDCNums(); i++)
		{
			cout << "DC  " << i << " : uploadA: " << up_apply[i] << endl;
			outfile << "DC  " << i << " : uploadA: " << up_apply[i] << endl;
		}
		cout << "-----------------------------------------------" << endl;
		outfile << "-----------------------------------------------" << endl;
		for (int i = 0; i < algorithm->getDCNums(); i++)
		{
			cout << "DC  " << i << " : downloadA: " << down_apply[i] << endl;
			outfile << "DC  " << i << " : downloadA: " << down_apply[i] << endl;
		}
	}
	else //for powerlyra
	{
		int *mapped = new int[algorithm->DCnums];
		int *map = new int[algorithm->DCnums];
		for (int i = 0; i < algorithm->DCnums; i++)
		{
			mapped[i] = -1;
			map[i] = -1;
		}
		int max_index_upG = max_value_index(up_gather, algorithm->DCnums);
		int min_index_upload = min_value_index_double(network->upload, algorithm->DCnums);
		int max_index_downG = max_value_index(down_gather, algorithm->DCnums);
		int min_index_download = min_value_index_double(network->download, algorithm->DCnums);
		int loadnum, dcnum;
		int loadnumA, dcnumA;
		if (up_gather[max_index_upG] / network->upload[min_index_upload] > down_gather[max_index_downG] / network->download[min_index_download])
		{
			mapped[max_index_upG] = min_index_upload;
			map[min_index_upload] = max_index_upG;
			loadnum = max_index_upG;
			dcnum = min_index_upload;
		}
		else
		{
			mapped[max_index_downG] = min_index_download;
			map[min_index_download] = max_index_downG;
			loadnum = max_index_downG;
			dcnum = min_index_download;
		}

		double max = up_apply[loadnum] / network->upload[dcnum] > down_apply[loadnum] / network->download[dcnum] ? up_apply[loadnum] / network->upload[dcnum] : down_apply[loadnum] / network->download[dcnum];
		int max_index_upA = max_value_index_besides(up_apply, algorithm->DCnums, loadnum);
		min_index_upload = min_value_index_double_besides(network->upload, algorithm->DCnums, dcnum);
		if (max > up_apply[max_index_upA] / network->upload[min_index_upload])
		{
			loadnumA = loadnum;
			dcnumA = dcnum;
		}
		else
		{
			loadnumA = max_index_upA;
			dcnumA = min_index_upload;
			max = up_apply[max_index_upA] / network->upload[min_index_upload];
		}
		int max_index_downA = max_value_index_besides(down_apply, algorithm->DCnums, loadnum);
		min_index_download = min_value_index_double_besides(network->download, algorithm->DCnums, dcnum);
		if (max < down_apply[max_index_downA] / network->download[min_index_download])
		{
			loadnumA = max_index_downA;
			dcnumA = min_index_download;
		}
		mapped[loadnumA] = dcnumA;
		map[dcnumA] = loadnumA;
		int start = 0;
		for (int m = 0; m < algorithm->DCnums; m++)
		{
			if (map[m] == -1)
			{
				for (int n = start; n < algorithm->DCnums; n++)
				{
					if (mapped[n] == -1)
					{
						map[m] = n;
						mapped[n] = m;
						start = n + 1;
						break;
					}
				}
			}
		}

		double maxArray[algorithm->DCnums];
		double maxArray2[algorithm->DCnums];
		double uploadnumsum[algorithm->DCnums];

		for (int j = 0; j < algorithm->DCnums; j++)
		{
			double divide = up_gather[map[j]] / network->upload[j];
			double divided = down_gather[map[j]] / network->download[j];
			maxArray[j] = divide > divided ? divide : divided;

			double divide2 = up_apply[map[j]] / network->upload[j];
			double divided2 = down_apply[map[j]] / network->download[j];
			maxArray2[j] = divide2 > divided2 ? divide2 : divided2;

			uploadnumsum[j] = up_gather[map[j]] + up_apply[map[j]];
		}

		data_transfer_time = max_value(maxArray) + max_value(maxArray2);
		data_transfer_cost = sumWeights(uploadnumsum, network->upprice);

		for (int i = 0; i < algorithm->getDCNums(); i++)
		{
			cout << "DC  " << i << " : uploadG: " << up_gather[map[i]] << endl;
			outfile << "DC  " << i << " : uploadG: " << up_gather[map[i]] << endl;
		}
		cout << "-----------------------------------------------" << endl;
		outfile << "-----------------------------------------------" << endl;
		for (int i = 0; i < algorithm->getDCNums(); i++)
		{
			cout << "DC  " << i << " : downloadG: " << down_gather[map[i]] << endl;
			outfile << "DC  " << i << " : downloadG: " << down_gather[map[i]] << endl;
		}
		cout << "-----------------------------------------------" << endl;
		outfile << "-----------------------------------------------" << endl;
		for (int i = 0; i < algorithm->getDCNums(); i++)
		{
			cout << "DC  " << i << " : uploadA: " << up_apply[map[i]] << endl;
			outfile << "DC  " << i << " : uploadA: " << up_apply[map[i]] << endl;
		}
		cout << "-----------------------------------------------" << endl;
		outfile << "-----------------------------------------------" << endl;
		for (int i = 0; i < algorithm->getDCNums(); i++)
		{
			cout << "DC  " << i << " : downloadA: " << down_apply[map[i]] << endl;
			outfile << "DC  " << i << " : downloadA: " << down_apply[map[i]] << endl;
		}
		delete[] mapped;
		delete[] map;
	}

	cout << "-----------------------------------------------" << endl;
	outfile << "-----------------------------------------------" << endl;
	cout << "#wan for communication: " << wan_usage << endl;
	cout << "#data transfer time: " << data_transfer_time << endl;
	cout << "#data transfer cost: " << data_transfer_cost << endl;
	cout << "#budget: " << budget << endl;
	cout << "#movecost: " << movecost << endl;
	cout << "#movecost is " << movecost / total_move_cost << " of the total_move_cost" << endl;
	cout << "#monetary cost: " << movecost + data_transfer_cost * mvpercents << endl;
	cout << "#overhead: " << overhead << endl;

	outfile << "#wan for communication: " << wan_usage << endl;
	outfile << "#data transfer time: " << data_transfer_time << endl;
	outfile << "#data transfer cost: " << data_transfer_cost << endl;
	outfile << "#budget: " << budget << endl;
	outfile << "#movecost: " << movecost << endl;
	outfile << "# movecost is " << movecost / total_move_cost << " of the total_move_cost" << endl;
	outfile << "#monetary cost: " << movecost + data_transfer_cost * mvpercents << endl;
	outfile << "#overhead: " << overhead << endl;
	outfile.close();
	delete[] numOfE;
	delete[] numOfV;
	delete[] numOfVIM;
	delete[] up_gather;
	delete[] up_apply;
	delete[] down_gather;
	delete[] down_apply;
}

int iter = 0;

void RLcut()
{

	int flag = 0;
	double *signal_ = NULL;
	double timeOld = 0;
	Vertex *v = graph->getVertexs();
	double *uploadnumG = new double[algorithm->getDCNums()];
	double *downloadnumG = new double[algorithm->getDCNums()];
	double *uploadnumA = new double[algorithm->getDCNums()];
	double *downloadnumA = new double[algorithm->getDCNums()];
	double *maxArray = new double[algorithm->getDCNums()];
	double *maxArray2 = new double[algorithm->getDCNums()];
	double *uploadnumsum = new double[algorithm->getDCNums()];
	double *Upload = network->getUpload();
	double *Download = network->getDownload();
	double *Upprice = network->getUpprice();
	double *SR = new double[MAX_ITER + 1];
	double *TOEI = new double[MAX_ITER + 1];

	DC_mutex = new pthread_mutex_t[algorithm->DCnums];

	initPartitioning();
	calculateTimeAndPrice(data_transfer_time, data_transfer_cost);
	Mirror **mirrors = graph->getMirrors();
	Partition *partition = network->getPartition();
	pthread_t tid[MAX_THREADS_NUM];
	pthread_t tidd[MAX_THREADS_NUM];
	pthread_t tid_bsp[batchsize];
	int *eachThreadsNum = new int[MAX_THREADS_NUM];
	cout << "batchsize: " << batchsize << endl;
	cout << "before executing the algorithm:" << endl;
	cout << "#data_transfer_time: " << data_transfer_time << endl;
	cout << "#data_transfer_cost: " << data_transfer_cost << endl;
	cout << "The seed is: " << Seed << endl;
	//ofstream outfile(outputfile);
	outfile = ofstream(outputfile);
	//outfile<<"asdf"<<endl;
	ofstream outtrain(FN);
	if (data_transfer_cost <= budget)
	{
		F1 = 0;
		F2 = 1;
		flag = 0;
	}
	else
	{
		F1 = 1;
		F2 = 0;
		flag = 1;
	}
	for (int i = 0; i < graph->getNum_Nodes(); i++)
		v[i].initSignal_();
	multiprocessing_pool(eachThreadsNum);

	for (int i = 0; i < graph->getNum_Nodes(); i++)
		v[i].action = v[i].label;

	budget = global_mvcost * budget + mvpercents * ginger_cost;
	cout << "The budget is " << budget << endl;

	map<int, int> cnt;
	for (int i = 0; i < graph->getNum_Nodes(); i++)
		cnt[v[i].getIngoingEdges().size()]++;

	int sum = 0;
	int train_num = 0;
	for (auto &x : cnt)
	{
		sum += x.second * x.first;
		train_num += x.second;

		if (1. * sum / graph->getNum_Edges() >= train_edge_rate)
		{
			k_degree = x.first;
			cout << "k = " << k_degree << ", training " << 1. * sum / graph->getNum_Edges() << " of edges" << endl;
			outfile << "train_edge_rate = " << train_edge_rate << ", k = " << k_degree << ", training " << 1. * sum / graph->getNum_Edges() << " of edges" << endl;

			;
			break;
		}
	}

	cout << "-----------------------------------------------------------------" << endl;

	while (iter <= MAX_ITER)
	{
		/********************************Init*************************************/
		for (int i = 0; i < algorithm->DCnums; i++)
		{
			uploadnumG[i] = ug[i];
			downloadnumG[i] = dg[i];
			uploadnumA[i] = ua[i];
			downloadnumA[i] = da[i];
			maxArray[i] = 0;
		}

		time_t start = time(NULL);

		if (iter > 0)
		{
			/************************************MyActionSelect***************************************/
			int low1 = 0;
			int high1 = eachThreadsNum[0] - 1;
			int status1 = 0;
			start = time(NULL);
			high_resolution_clock::time_point beginTime = high_resolution_clock::now();

			for (int j = 0; j < MAX_THREADS_NUM; j++)
			{
				Pthread_args *args = new Pthread_args();
				args->setLow(low1);
				args->setHigh(high1);
				args->setIter(iter);
				status1 = pthread_create(&tid[j], NULL, MyActionSelect, (void *)args);
				low1 = high1 + 1;
				if (j < MAX_THREADS_NUM - 1)
					high1 = low1 + eachThreadsNum[j + 1] - 1;
			}

			for (int j = 0; j < MAX_THREADS_NUM; j++)
				pthread_join(tid[j], NULL);

			// t1=time(NULL)-start;
			high_resolution_clock::time_point endTime = high_resolution_clock::now();
			milliseconds timeInterval = std::chrono::duration_cast<milliseconds>(endTime - beginTime);
			t1 = timeInterval.count();
			timeOld = data_transfer_time;
			priceOld = data_transfer_cost;

			/*-------------------------------------------Vertex Migrate------------------------------------------*/
			if (bsp == 0)
			{

				int percent = 0;
				int decidedtomove = 0;
				start = time(NULL);
				unordered_set<int>::iterator itora = trained.begin();
				while (itora != trained.end())
				{
					int i = *itora;
					int ori = v[i].label;
					//start=time(NULL);
					if (v[i].label != v[i].action)
					{
						decidedtomove++;
						vertexMigrate(i, downloadnumG, uploadnumG, downloadnumA, uploadnumA, v[i].action);

						/************************Gather***********************************/

						for (int j = 0; j < algorithm->DCnums; j++)
						{
							double divide = uploadnumG[j] / Upload[j];
							double divided = downloadnumG[j] / Download[j];
							maxArray[j] = divide > divided ? divide : divided;
						}

						double time = max_value(maxArray);

						/************************Apply***********************************/
						for (int j = 0; j < algorithm->DCnums; j++)
						{
							double divide = uploadnumA[j] / Upload[j];
							double divided = downloadnumA[j] / Download[j];
							maxArray[j] = divide > divided ? divide : divided;
						}
						time += max_value(maxArray);

						for (int j = 0; j < algorithm->DCnums; j++)
						{
							uploadnumsum[j] = uploadnumG[j] + uploadnumA[j];
						}
						double cost = sumWeights(uploadnumsum, Upprice);

						if (time <= data_transfer_time)
						{
							data_transfer_time = time;
							data_transfer_cost = cost;
						}
						else
						{
							percent++;
							vertexMigrate(i, downloadnumG, uploadnumG, downloadnumA, uploadnumA, ori);
						}
					}
					itora++;
				}

				for (int i = 0; i < algorithm->DCnums; i++)
				{
					ug[i] = uploadnumG[i];
					dg[i] = downloadnumG[i];
					ua[i] = uploadnumA[i];
					da[i] = downloadnumA[i];
				}

				t2 = time(NULL) - start;
			}
			else if (bsp == 1)
			{

				for (int j = 0; j < batchsize; j++)
				{
					Pthread_args *args = new Pthread_args();
					args->id = j;
					status1 = pthread_create(&tid_bsp[j], NULL, my_moveVertexBsp, (void *)args);
				}
				unordered_set<int>::iterator itora = trained.begin();
				int *verID = new int[batchsize];

				double **ulg = new double *[batchsize];
				double **dlg = new double *[batchsize];
				double **ula = new double *[batchsize];
				double **dla = new double *[batchsize];
				for (int i = 0; i < batchsize; i++)
				{
					ulg[i] = new double[algorithm->DCnums];
					dlg[i] = new double[algorithm->DCnums];
					ula[i] = new double[algorithm->DCnums];
					dla[i] = new double[algorithm->DCnums];
				}
				vector<int> k_degree_trained_2, k_degree_trained;
				int t = 10;
				while (itora != trained.end())
				{
					if (v[*itora].label != v[*itora].action)
					{
						k_degree_trained.push_back(*itora);
					}
					//if(t-- >= 0)	cout << itora->first << " " ;
					itora++;
				}
				cout << endl;

				// int size = k_degree_trained_2.size();
				// for (int i = 0; i < size / batchsize; i++)
				// 	for (int j = i; j < k_degree_trained_2.size(); j += size / batchsize)
				// 		k_degree_trained.push_back(k_degree_trained_2[j]);

				start = time(NULL);
				atomic_uploadG = vector<atomic_int>(algorithm->getDCNums());
				atomic_uploadA = vector<atomic_int>(algorithm->getDCNums());
				atomic_downloadG = vector<atomic_int>(algorithm->getDCNums());
				atomic_downloadA = vector<atomic_int>(algorithm->getDCNums());
				high_resolution_clock::time_point beginTime = high_resolution_clock::now();
				double o = 0;
				time_t s = time(NULL);
				int vertype = 0;
				int trained_cnt = 0;

				while (trained_cnt < k_degree_trained.size())
				{
					int numver = 0;
					sem_init(&ready, 0, 0);

					for (int i = 0; i < algorithm->DCnums; i++)
					{
						atomic_uploadG[i] = 0;
						atomic_uploadA[i] = 0;
						atomic_downloadG[i] = 0;
						atomic_downloadA[i] = 0;
					}

					while (trained_cnt < k_degree_trained.size() && numver < batchsize)
					{

						verID[numver] = k_degree_trained[trained_cnt];
						// sig[numver] = 0;
						numver++;

						trained_cnt++;
					}

					for (int i = 0; i < numver; i++)
					{
						for (int j = 0; j < algorithm->DCnums; j++)
						{
							ulg[i][j] = ug[j];
							dlg[i][j] = dg[j];
							ula[i][j] = ua[j];
							dla[i][j] = da[j];
						}
					}

					s = time(NULL);
					high_resolution_clock::time_point parallel_begin = high_resolution_clock::now();

					for (int j = 0; j < numver; j++)
					{
						// cout<<"j: "<<j<<endl;
						Pthread_args args = Pthread_args();
						args.id = j;
						args.iteration = iter;
						args.verindex = j;
						args.ver = verID[j];
						args.downloadnumG = dlg[j];
						args.uploadnumG = ulg[j];
						args.downloadnumA = dla[j];
						args.uploadnumA = ula[j];
						args.destdc = v[verID[j]].action;

						bspvec[j].push_back(args);
					}

					while (ready_thread != numver)
						;
					ready_thread = 0;

					for (int i = 0; i < numver; i++)
						sem_post(&ready);

					while (bsplock != numver)
					{
					}
					bsplock = 0;
					for (int i = 0; i < algorithm->getDCNums(); i++)
					{
						ua[i] += atomic_uploadA[i] * dataunit;
						ug[i] += atomic_uploadG[i] * dataunit;
						da[i] += atomic_downloadA[i] * dataunit;
						dg[i] += atomic_downloadG[i] * dataunit;
					}

					o += time(NULL) - s;

					/************************Time***********************************/

					for (int j = 0; j < algorithm->DCnums; j++)
					{
						double divide = ug[j] / Upload[j];
						double divided = dg[j] / Download[j];
						maxArray[j] = divide > divided ? divide : divided;

						double divide2 = ua[j] / Upload[j];
						double divided2 = da[j] / Download[j];
						maxArray2[j] = divide2 > divided2 ? divide2 : divided2;

						uploadnumsum[j] = ug[j] + ua[j];
					}

					double time = max_value(maxArray) + max_value(maxArray2);
					double cost = sumWeights(uploadnumsum, Upprice);
					data_transfer_time = time;
					data_transfer_cost = cost;
				}

				for (int j = 0; j < batchsize; j++)
					pthread_cancel(tid_bsp[j]);

				for (int j = 0; j < batchsize; j++)
					pthread_join(tid_bsp[j], NULL);

				high_resolution_clock::time_point endTime = high_resolution_clock::now();
				milliseconds timeInterval = std::chrono::duration_cast<milliseconds>(endTime - beginTime);
				t2 = timeInterval.count();

				delete[] verID;
				// delete[] sig;
				for (int i = 0; i < batchsize; i++)
				{
					delete[] ulg[i];
					delete[] dlg[i];
					delete[] ula[i];
					delete[] dla[i];
				}
				delete[] ulg;
				delete[] dlg;
				delete[] ula;
				delete[] dla;
			}
			TOEI[iter - 1] = (t1 + t2 + t3 + t4 + t5) / 1000;
			SR[iter - 1] = randpro;
			double usedtime = 0;
			for (int p = 0; p < iter; p++)
				usedtime += TOEI[p];
			double lefteach = (L - usedtime) / (MAX_ITER - iter);
			int recent = 3;
			// int recent = 1;
			if (lefteach <= 0)
				randpro = 0;
			else
			{
				if (iter >= recent)
				{
					double total_sr = 0;
					for (int p = iter - 1; p >= iter - recent; p--)
						total_sr += lefteach / TOEI[p] * SR[p];
					randpro = total_sr / recent;
				}
				else
				{
					double total_sr = 0;
					for (int p = 0; p <= iter - 1; p++)
						total_sr += lefteach / TOEI[p] * SR[p];
					randpro = total_sr / recent;
				}
			}
			//randpro=0.01;
			if (randpro > 1)
				randpro = 1;

			trained.clear();
			{
				int low1 = 0;
				int high1 = eachThreadsNum[0] - 1;
				int status1 = 0;
				start = time(NULL);
				high_resolution_clock::time_point beginTime = high_resolution_clock::now();
				for (int j = 0; j < MAX_THREADS_NUM; j++)
				{
					Pthread_args *args = new Pthread_args();
					args->setLow(low1);
					args->setHigh(high1);
					args->setIter(iter);
					status1 = pthread_create(&tid[j], NULL, Sampling, (void *)args);
					low1 = high1 + 1;
					if (j < MAX_THREADS_NUM - 1)
						high1 = low1 + eachThreadsNum[j + 1] - 1;
				}

				for (int j = 0; j < MAX_THREADS_NUM; j++)
					pthread_join(tid[j], NULL);

				high_resolution_clock::time_point endTime = high_resolution_clock::now();
				milliseconds timeInterval = std::chrono::duration_cast<milliseconds>(endTime - beginTime);
				// t3 = timeInterval.count();
				t3 = time(NULL) - start;
				// t3 = 0;
			}

			cout << "trained size: " << trained.size() << endl;

			cout << "the " << iter << " iteration :" << endl;
			//cout << " #the sampling rate: " << randpro << endl;
			cout << fixed << setprecision(15) << " #the sampling rate: " << randpro << endl;
			//cout << "stepbudget: " << stepbudget << endl;
			cout << "#data_transfer_time: " << data_transfer_time << ";#data_transfer_cost: " << data_transfer_cost << endl;
			cout << "#mvcost: " << current_mvcost << " ;#monetary cost: " << current_mvcost + mvpercents * data_transfer_cost << endl;

			outfile << data_transfer_time << '\t' << data_transfer_cost << '\t' << trained.size() << '\t';

			if (iter == 0)
				outfile << endl;

			if (iter % 4 == 0)
			{
				betaC = (iter / 4) * 0.25;
				alphaT = 1 - betaC;
			}
		}

		/**********************************Score And Objective Function**********************************/

		int low = 0;
		int high = eachThreadsNum[0] - 1;
		int status = 0;
		high_resolution_clock::time_point beginTime = high_resolution_clock::now();
		start = time(NULL);
		for (int j = 0; j < MAX_THREADS_NUM; j++)
		{
			Pthread_args *args = new Pthread_args();
			args->setId(j);
			args->setLow(low);
			args->setHigh(high);
			args->setIter(iter);
			status = pthread_create(&tidd[j], NULL, Parallel_Score_Signal_function, (void *)args);
			low = high + 1;
			if (j < MAX_THREADS_NUM - 1)
				high = low + eachThreadsNum[j + 1] - 1;
		}

		for (int j = 0; j < MAX_THREADS_NUM; j++)
			pthread_join(tidd[j], NULL);
		high_resolution_clock::time_point endTime = high_resolution_clock::now();
		milliseconds timeInterval = std::chrono::duration_cast<milliseconds>(endTime - beginTime);
		t5 = timeInterval.count();

		/**********************************Probability Update**********************************/
		low = 0;
		high = eachThreadsNum[0] - 1;
		status = 0;
		start = time(NULL);
		beginTime = high_resolution_clock::now();
		for (int j = 0; j < MAX_THREADS_NUM; j++)
		{
			Pthread_args *args = new Pthread_args();
			args->setLow(low);
			args->setHigh(high);
			args->setIter(iter);
			status = pthread_create(&tid[j], NULL, probability_Update, (void *)args);
			low = high + 1;
			if (j < MAX_THREADS_NUM - 1)
				high = low + eachThreadsNum[j + 1] - 1;
		}
		for (int j = 0; j < MAX_THREADS_NUM; j++)
			pthread_join(tid[j], NULL);
		endTime = high_resolution_clock::now();
		timeInterval = std::chrono::duration_cast<milliseconds>(endTime - beginTime);
		t4 = timeInterval.count();

		if (iter >= 1)
		{
			cout << "Action Select: " << t1 << endl;
			cout << "Vertex Move: " << t2 << endl;
			cout << "Sampling: " << t3 << endl;
			cout << "Probability Update: " << t4 << endl;
			cout << "Score Function: " << t5 << endl;
			double T = t1 + t2 + t3 + t4 + t5;
			overhead += T;

			outfile << TOEI[iter - 1] << '\t' << randpro << endl;
			cout << "******************************************************************" << endl;
			cout << "The overhead is: " << TOEI[iter - 1] << endl;
			cout << "******************************************************************" << endl;
		}

		iter++;
	}

	outtrain = ofstream(FN);
	for (int i = 0; i < graph->getNum_Nodes(); i++)
		outtrain << i << " " << v[i].label << endl;
	outtrain.close();

	outfile << data_transfer_cost << '\t' << current_mvcost << '\t' << current_mvcost + mvpercents * data_transfer_cost;

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

	overhead = 0;
	for (int p = 0; p < MAX_ITER; p++)
		overhead += TOEI[p];
	//outfile.close();
	outtrain.close();
	delete[] eachThreadsNum;
	delete[] uploadnumA;
	delete[] uploadnumG;
	delete[] downloadnumA;
	delete[] downloadnumG;
	delete[] maxArray;
	delete[] maxArray2;
	delete[] uploadnumsum;
}

void *Parallel_Score_Signal_function(void *arguments)
{
	Pthread_args *args = (Pthread_args *)arguments;
	unsigned int low = args->low;
	unsigned int high = args->high;
	int thread_id = args->id;
	srand(time(NULL) + thread_id);
	int iter = args->iteration;
	double uploadnumG[algorithm->DCnums];
	double downloadnumG[algorithm->DCnums];
	double uploadnumA[algorithm->DCnums];
	double downloadnumA[algorithm->DCnums];
	double uploadnumsum[algorithm->DCnums];
	long long *mapped = graph->mapped;
	double maxArray[algorithm->DCnums];
	double maxArray2[algorithm->DCnums];
	double *Upload = network->upload;
	double *Download = network->download;
	double *Upprice = network->upprice;
	Vertex *v = graph->nodes;
	list<Edge *>::iterator it;
	Mirror **mirrors = graph->mirrors;
	list<Edge *> oedges;
	list<Edge *> iedges;
	double temp_score_array[algorithm->DCnums];

	for (int i = 0; i < algorithm->DCnums; i++)
	{
		uploadnumG[i] = ug[i];
		downloadnumG[i] = dg[i];
		uploadnumA[i] = ua[i];
		downloadnumA[i] = da[i];
	}

	double totalwan = 0;
	for (int i = 0; i < algorithm->DCnums; i++)
	{
		totalwan = totalwan + ug[i] + dg[i] + ua[i] + da[i];
	}

	vector<int> vec = PSS[thread_id];
	int vecsize = vec.size();

	for (int p = 0; p < vecsize; p++)
	{
		int ver = vec[p];
		unordered_set<int>::iterator itora = trained.find(ver);
		if (itora != trained.end())
		{
			for (int i = 0; i < algorithm->DCnums; i++)
			{
				double rr = 0;
				if (v[ver].iniLabel == v[ver].label && v[ver].label != i)
					rr = raw_data_size * Upprice[v[ver].iniLabel];
				else if (v[ver].iniLabel != v[ver].label && v[ver].label != i && v[ver].iniLabel == i)
					rr = -raw_data_size * Upprice[v[ver].iniLabel];

				if (i == v[ver].label)
				{
					temp_score_array[i] = 0;
				}
				else
				{
					//---------------------------------move verto DC i---------------------------------
					//moveVertex(ver,downloadnumG,uploadnumG,downloadnumA,uploadnumA,i);
					moveVertex(ver, downloadnumG, uploadnumG, downloadnumA, uploadnumA, i);
					//计算score
					/************************Gather Time***********************************/
					for (int j = 0; j < algorithm->DCnums; j++)
					{
						double divide = uploadnumG[j] / Upload[j];
						double divided = downloadnumG[j] / Download[j];
						maxArray[j] = divide > divided ? divide : divided;

						double divide2 = uploadnumA[j] / Upload[j];
						double divided2 = downloadnumA[j] / Download[j];
						maxArray2[j] = divide2 > divided2 ? divide2 : divided2;

						uploadnumsum[j] = uploadnumA[j] + uploadnumG[j];
					}

					double time = max_value(maxArray) + max_value(maxArray2);
					double cost = sumWeights(uploadnumsum, Upprice);
					double cost_A = data_transfer_cost * mvpercents + current_mvcost;
					double cost_B = cost * mvpercents + current_mvcost + rr;
					double wan = 0;
					for (int j = 0; j < algorithm->DCnums; j++)
					{
						wan = wan + uploadnumG[j] + downloadnumG[j] + uploadnumA[j] + downloadnumA[j];
					}
					//temp_score_array[i] = data_transfer_time - time;
					int overbudget = 0;
					if (data_transfer_cost > budget)
						overbudget = 1;
					double downbudget = overbudget == 1 ? 0 : 1;
					double costdown = cost > budget ? cost : budget;
					double over = cost > budget ? 1 : 0;
					double belta = (cost_A - budget) / cost_A;
					double alpha = 1 - belta;
					if (belta <= 0)
					{
						belta = 0;
						alpha = 1;
					}
					if (alpha == 1)
					{
					}
					if (iter >= MAX_ITER - MAX_ITER / 2 && cost_A > budget)
					{
						alpha = 0;
						belta = 1;
					}
					if (alpha != 1)
						temp_score_array[i] = alpha * (data_transfer_time - time) / data_transfer_time + belta * (cost_A - cost_B) / cost_A;
					else
					{

						double a, b;
						if (iter == 0)
						{
							a = 1;
							b = 0;
							optitime = 1;
						}
						else if (optitime == 0)
						{
							a = 1;
							b = 0;
							optitime = 1;
						}
						else if (optitime == 1 && (timeOld - data_transfer_time) / timeOld >= 0.3)
						{
							a = 1;
							b = 0;
							optitime = 1;
						}
						else
						{
							a = 0;
							b = 1;
							optitime = 0;
						}
						temp_score_array[i] = a * (data_transfer_time - time) / data_transfer_time + b * (totalwan - wan) / totalwan;
					}
				}

				for (int k = 0; k < algorithm->DCnums; k++)
				{
					uploadnumG[k] = ug[k];
					downloadnumG[k] = dg[k];
					uploadnumA[k] = ua[k];
					downloadnumA[k] = da[k];
				}
			}

			int max_index = my_max_value_index(temp_score_array, algorithm->DCnums);
			double *signal_ = v[ver].signal_;
			signal_[max_index] += 1;
		}
	}
	delete args;
}
void vertexMigrate(int ver, double *downloadnumG, double *uploadnumG, double *downloadnumA, double *uploadnumA, int destdc) //改变loadnum以及mirrors
{
	long long *mapped = graph->mapped;
	Vertex *v = graph->nodes;
	list<Edge *>::iterator it;
	//int * labels=new int[graph->getNum_Nodes()];
	Mirror **mirrors = graph->mirrors;
	list<Edge *> oedges;
	list<Edge *> iedges;
	int originL = v[ver].label;

	//v[ver].setLabel(destdc);
	v[ver].label = destdc;

	if (v[ver].type == 1) //low-degree
	{
		for (int i = 0; i < algorithm->DCnums; i++)
			if (mirrors[i][ver].outD > 0)
				uploadnumA[originL] -= dataunit;

		mirrors[originL][ver].inD = v[ver].idegreeinlabel;
		mirrors[originL][ver].outD = v[ver].odegreeinlabel;

		if (mirrors[originL][ver].outD > 0 || mirrors[originL][ver].inD > 0)
			mirrors[originL][ver].id = ver;

		if (mirrors[originL][ver].inD > 0)
			uploadnumG[originL] += dataunit;
		if (mirrors[originL][ver].outD > 0)
			downloadnumA[originL] += dataunit;

		if (mirrors[destdc][ver].outD > 0)
			downloadnumA[destdc] -= dataunit;

		if (mirrors[originL][ver].inD > 0)
			downloadnumG[destdc] += dataunit;

		for (int i = 0; i < algorithm->DCnums; i++)
		{
			if (i != destdc && mirrors[i][ver].outD > 0)
				uploadnumA[destdc] += dataunit;
		}

		if (mirrors[destdc][ver].id != -1)
		{
			v[ver].idegreeinlabel = mirrors[destdc][ver].inD;
			v[ver].odegreeinlabel = mirrors[destdc][ver].outD;
		}
		else
		{
			v[ver].idegreeinlabel = 0;
			v[ver].odegreeinlabel = 0;
		}

		iedges = v[ver].ingoingEdges;
		for (it = iedges.begin(); it != iedges.end(); it++)
		{
			v[ver].idegreeinlabel++;
			long long sourceId = mapped[(*it)->sourceID];
			if (v[sourceId].type == 1 && v[sourceId].label == originL) //low-degree master
			{
				mirrors[originL][ver].inD--;
				v[sourceId].odegreeinlabel--;
				if (mirrors[originL][ver].inD == 0)
				{
					uploadnumG[originL] -= dataunit;
					downloadnumG[destdc] -= dataunit;
					if (mirrors[originL][ver].outD == 0)
					{
						mirrors[originL][ver].inD = 0;
						mirrors[originL][ver].outD = 0;
						mirrors[originL][ver].id = -1;
					}
				}
				if (mirrors[destdc][sourceId].id == -1)
				{
					mirrors[destdc][sourceId].id = sourceId;
					mirrors[destdc][sourceId].outD++;
					uploadnumA[originL] += dataunit;
					downloadnumA[destdc] += dataunit;
				}
				else
				{
					mirrors[destdc][sourceId].outD++;
				}
			}
			else if (v[sourceId].type == 1 && v[sourceId].label != originL) //low-degree Mirror
			{
				mirrors[originL][ver].inD--;
				mirrors[originL][sourceId].outD--;
				if (mirrors[originL][ver].inD == 0)
				{
					uploadnumG[originL] -= dataunit;
					downloadnumG[destdc] -= dataunit;
					if (mirrors[originL][ver].outD == 0)
					{
						mirrors[originL][ver].inD = 0;
						mirrors[originL][ver].outD = 0;
						mirrors[originL][ver].id = -1;
					}
				}
				if (mirrors[originL][sourceId].outD == 0)
				{
					downloadnumA[originL] -= dataunit;
					uploadnumA[v[sourceId].label] -= dataunit;
					mirrors[originL][sourceId].inD = 0;
					mirrors[originL][sourceId].outD = 0;
					mirrors[originL][sourceId].id = -1;
				}
				if (v[sourceId].label != destdc)
				{
					if (mirrors[destdc][sourceId].id == -1)
					{
						mirrors[destdc][sourceId].id = sourceId;
						mirrors[destdc][sourceId].outD++;
						downloadnumA[destdc] += dataunit;
						uploadnumA[v[sourceId].label] += dataunit;
					}
					else
					{
						mirrors[destdc][sourceId].outD++;
					}
				}
				else
				{
					v[sourceId].odegreeinlabel++;
				}
			}
			else if (v[sourceId].type == 0 && v[sourceId].label == originL) //high-degree master
			{
				mirrors[originL][ver].inD--;
				v[sourceId].odegreeinlabel--;
				if (mirrors[originL][ver].inD == 0)
				{
					uploadnumG[originL] -= dataunit;
					downloadnumG[destdc] -= dataunit;
					if (mirrors[originL][ver].outD == 0)
					{
						mirrors[originL][ver].inD = 0;
						mirrors[originL][ver].outD = 0;
						mirrors[originL][ver].id = -1;
					}
				}
				if (mirrors[destdc][sourceId].id != -1)
				{
					mirrors[destdc][sourceId].outD++;
					if (mirrors[destdc][sourceId].outD == 1)
					{
						downloadnumA[destdc] += dataunit;
						uploadnumA[originL] += dataunit;
					}
				}
				else
				{
					mirrors[destdc][sourceId].id = sourceId;
					mirrors[destdc][sourceId].outD++;
					downloadnumA[destdc] += dataunit;
					uploadnumA[originL] += dataunit;
				}
			}
			else if (v[sourceId].type == 0 && v[sourceId].label != originL) //high-degree mirror
			{
				mirrors[originL][ver].inD--;
				mirrors[originL][sourceId].outD--;
				if (mirrors[originL][ver].inD == 0)
				{
					uploadnumG[originL] -= dataunit;
					downloadnumG[destdc] -= dataunit;
					if (mirrors[originL][ver].outD == 0)
					{
						mirrors[originL][ver].inD = 0;
						mirrors[originL][ver].outD = 0;
						mirrors[originL][ver].id = -1;
					}
				}
				if (mirrors[originL][sourceId].outD == 0)
				{
					downloadnumA[originL] -= dataunit;
					uploadnumA[v[sourceId].label] -= dataunit;
					if (mirrors[originL][sourceId].inD == 0)
					{
						mirrors[originL][sourceId].inD = 0;
						mirrors[originL][sourceId].outD = 0;
						mirrors[originL][sourceId].id = -1;
					}
				}
				if (v[sourceId].label != destdc)
				{
					if (mirrors[destdc][sourceId].id == -1)
					{
						mirrors[destdc][sourceId].id = sourceId;
						mirrors[destdc][sourceId].outD++;
						uploadnumA[v[sourceId].label] += dataunit;
						downloadnumA[destdc] += dataunit;
					}
					else
					{
						mirrors[destdc][sourceId].outD++;
						if (mirrors[destdc][sourceId].outD == 1)
						{
							uploadnumA[v[sourceId].label] += dataunit;
							downloadnumA[destdc] += dataunit;
						}
					}
				}
				else
				{
					v[sourceId].odegreeinlabel++;
				}
			}
		}

		oedges = v[ver].outgoingEdges;
		for (it = oedges.begin(); it != oedges.end(); it++)
		{
			long long destId = mapped[(*it)->destID];
			if (v[destId].type == 0 && v[destId].label == originL) //high-degree master
			{
				v[destId].idegreeinlabel--;
				v[ver].odegreeinlabel++;
				mirrors[originL][ver].outD--;
				if (mirrors[originL][ver].outD == 0)
				{
					mirrors[originL][ver].inD = 0;
					mirrors[originL][ver].outD = 0;
					mirrors[originL][ver].id = -1;
					downloadnumA[originL] -= dataunit;
					uploadnumA[destdc] -= dataunit;
				}
				if (mirrors[destdc][destId].id == -1)
				{
					mirrors[destdc][destId].id = destId;
					mirrors[destdc][destId].inD++;
					uploadnumG[destdc] += dataunit;
					downloadnumG[originL] += dataunit;
				}
				else
				{
					mirrors[destdc][destId].inD++;
					if (mirrors[destdc][destId].inD == 1)
					{
						uploadnumG[destdc] += dataunit;
						downloadnumG[originL] += dataunit;
					}
				}
			}
			else if (v[destId].type == 0 && v[destId].label != originL) //high-degree mirror
			{
				v[ver].odegreeinlabel++;
				mirrors[originL][ver].outD--;
				mirrors[originL][destId].inD--;
				if (mirrors[originL][ver].outD == 0)
				{
					mirrors[originL][ver].inD = 0;
					mirrors[originL][ver].outD = 0;
					mirrors[originL][ver].id = -1;
					downloadnumA[originL] -= dataunit;
					uploadnumA[destdc] -= dataunit;
				}
				if (mirrors[originL][destId].inD == 0)
				{
					uploadnumG[originL] -= dataunit;
					downloadnumG[v[destId].label] -= dataunit;
					if (mirrors[originL][destId].outD == 0)
					{
						mirrors[originL][destId].inD = 0;
						mirrors[originL][destId].outD = 0;
						mirrors[originL][destId].id = -1;
					}
				}
				if (v[destId].label != destdc && mirrors[destdc][destId].id == -1)
				{
					mirrors[destdc][destId].id = destId;
					mirrors[destdc][destId].inD++;
					uploadnumG[destdc] += dataunit;
					downloadnumG[v[destId].label] += dataunit;
				}
				else if (v[destId].label != destdc && mirrors[destdc][destId].id != -1)
				{
					mirrors[destdc][destId].inD++;
					if (mirrors[destdc][destId].inD == 1)
					{
						uploadnumG[destdc] += dataunit;
						downloadnumG[v[destId].label] += dataunit;
					}
				}
				if (v[destId].label == destdc)
					v[destId].idegreeinlabel++;
			}
		}
		mirrors[destdc][ver].inD = 0;
		mirrors[destdc][ver].outD = 0;
		mirrors[destdc][ver].id = -1;
	}
	else if (v[ver].type == 0) //high-degree
	{
		for (int i = 0; i < algorithm->DCnums; i++)
		{
			if (mirrors[i][ver].id != -1 && mirrors[i][ver].inD > 0)
				downloadnumG[originL] -= dataunit;
			if (mirrors[i][ver].id != -1 && mirrors[i][ver].outD > 0)
				uploadnumA[originL] -= dataunit;
		}

		mirrors[originL][ver].inD = v[ver].idegreeinlabel;
		mirrors[originL][ver].outD = v[ver].odegreeinlabel;

		if (mirrors[originL][ver].inD != 0 || mirrors[originL][ver].outD != 0)
			mirrors[originL][ver].id = ver;

		if (mirrors[originL][ver].inD > 0)
			uploadnumG[originL] += dataunit;
		if (mirrors[originL][ver].outD > 0)
			downloadnumA[originL] += dataunit;

		if (mirrors[destdc][ver].inD > 0)
			uploadnumG[destdc] -= dataunit;
		if (mirrors[destdc][ver].outD > 0)
			downloadnumA[destdc] -= dataunit;
		for (int i = 0; i < algorithm->DCnums; i++)
		{
			if (i != destdc && mirrors[i][ver].inD > 0)
				downloadnumG[destdc] += dataunit;
			if (i != destdc && mirrors[i][ver].outD > 0)
				uploadnumA[destdc] += dataunit;
		}

		if (mirrors[destdc][ver].id != -1)
		{
			v[ver].idegreeinlabel = mirrors[destdc][ver].inD;
			v[ver].odegreeinlabel = mirrors[destdc][ver].outD;
		}
		else
		{
			v[ver].idegreeinlabel = 0;
			v[ver].odegreeinlabel = 0;
		}

		oedges = v[ver].outgoingEdges;
		for (it = oedges.begin(); it != oedges.end(); it++)
		{
			long long destId = mapped[(*it)->destID];
			if (v[destId].type == 0 && v[destId].label == originL) //high-degree master
			{
				v[ver].odegreeinlabel++;
				v[destId].idegreeinlabel--;
				mirrors[originL][ver].outD--;
				if (mirrors[originL][ver].outD == 0)
				{
					downloadnumA[originL] -= dataunit;
					uploadnumA[destdc] -= dataunit;
					if (mirrors[originL][ver].inD == 0)
					{
						mirrors[originL][ver].inD = 0;
						mirrors[originL][ver].outD = 0;
						mirrors[originL][ver].id = -1;
					}
				}
				if (mirrors[destdc][destId].id == -1) //---------
				{
					mirrors[destdc][destId].id = destId;
					mirrors[destdc][destId].inD++;
					uploadnumG[destdc] += dataunit;
					downloadnumG[originL] += dataunit;
				}
				else
				{
					mirrors[destdc][destId].inD++;
					if (mirrors[destdc][destId].inD == 1)
					{
						uploadnumG[destdc] += dataunit;
						downloadnumG[originL] += dataunit;
					}
				}
			}
			else if (v[destId].type == 0 && v[destId].label != originL) //high-degree mirror
			{
				v[ver].odegreeinlabel++;
				mirrors[originL][ver].outD--;
				mirrors[originL][destId].inD--;
				if (mirrors[originL][ver].outD == 0)
				{
					downloadnumA[originL] -= dataunit;
					uploadnumA[destdc] -= dataunit;
					if (mirrors[originL][ver].inD == 0)
					{
						mirrors[originL][ver].inD = 0;
						mirrors[originL][ver].outD = 0;
						mirrors[originL][ver].id = -1;
					}
				}
				if (mirrors[originL][destId].inD == 0)
				{
					uploadnumG[originL] -= dataunit;
					downloadnumG[v[destId].label] -= dataunit;
					if (mirrors[originL][destId].outD == 0)
					{
						mirrors[originL][destId].inD = 0;
						mirrors[originL][destId].outD = 0;
						mirrors[originL][destId].id = -1;
					}
				}
				if (v[destId].label != destdc && mirrors[destdc][destId].id == -1)
				{
					mirrors[destdc][destId].id = destId;
					mirrors[destdc][destId].inD++;
					uploadnumG[destdc] += dataunit;
					downloadnumG[v[destId].label] += dataunit;
				}
				else if (v[destId].label != destdc && mirrors[destdc][destId].id != -1)
				{
					mirrors[destdc][destId].inD++;
					if (mirrors[destdc][destId].inD == 1)
					{
						uploadnumG[destdc] += dataunit;
						downloadnumG[v[destId].label] += dataunit;
					}
				}
				if (v[destId].label == destdc)
					v[destId].idegreeinlabel++;
			}
		}
		//mirrors[destdc][ver].reset();
		mirrors[destdc][ver].inD = 0;
		mirrors[destdc][ver].outD = 0;
		mirrors[destdc][ver].id = -1;
	}
}
void moveVertex(int ver, double *downloadnumG, double *uploadnumG, double *downloadnumA, double *uploadnumA, int destdc) //只改变loadnum不改变mirrors
{
	long long *mapped = graph->mapped;
	Vertex *v = graph->nodes;
	list<Edge *>::iterator it;
	Mirror **mirrors = graph->mirrors;
	list<Edge *> oedges;
	list<Edge *> iedges;
	int originL = v[ver].label;
	int masterin = 0;
	int mirrorin = 0;
	int masterout = 0;
	int mirrorout = 0;

	if (v[ver].type == 1) //low-degree
	{
		for (int i = 0; i < algorithm->DCnums; i++)
			if (mirrors[i][ver].outD > 0)
				uploadnumA[originL] -= dataunit;

		masterin = v[ver].idegreeinlabel;
		masterout = v[ver].odegreeinlabel;

		if (masterin > 0)
			uploadnumG[originL] += dataunit;
		if (masterout > 0)
			downloadnumA[originL] += dataunit;

		mirrorin = mirrors[destdc][ver].inD;
		mirrorout = mirrors[destdc][ver].outD;

		if (mirrorout > 0)
			downloadnumA[destdc] -= dataunit;
		if (masterin > 0)
			downloadnumG[destdc] += dataunit;

		for (int i = 0; i < algorithm->DCnums; i++)
		{
			if (i != destdc && i != originL && mirrors[i][ver].outD > 0)
				uploadnumA[destdc] += dataunit;
		}

		if (masterout > 0)
			uploadnumA[destdc] += dataunit;

		iedges = v[ver].ingoingEdges;
		for (it = iedges.begin(); it != iedges.end(); it++)
		{
			long long sourceId = mapped[(*it)->sourceID];
			if (v[sourceId].type == 1 && v[sourceId].label == originL) //low-degree master
			{
				masterin--;
				if (masterin == 0)
				{
					uploadnumG[originL] -= dataunit;
					downloadnumG[destdc] -= dataunit;
				}
				if (mirrors[destdc][sourceId].id == -1)
				{
					uploadnumA[originL] += dataunit;
					downloadnumA[destdc] += dataunit;
				}
				else
				{
					//mirrors[destdc][sourceId].addOutD();
				}
			}
			else if (v[sourceId].type == 1 && v[sourceId].label != originL) //low-degree Mirror
			{
				masterin--;
				if (masterin == 0)
				{
					uploadnumG[originL] -= dataunit;
					downloadnumG[destdc] -= dataunit;
				}
				if (mirrors[originL][sourceId].outD == 1)
				{
					downloadnumA[originL] -= dataunit;
					uploadnumA[v[sourceId].label] -= dataunit;
				}
				if (v[sourceId].label != destdc)
				{
					if (mirrors[destdc][sourceId].id == -1)
					{
						downloadnumA[destdc] += dataunit;
						uploadnumA[v[sourceId].label] += dataunit;
					}
				}
			}
			else if (v[sourceId].type == 0 && v[sourceId].label == originL) //high-degree master
			{
				masterin--;
				if (masterin == 0)
				{
					uploadnumG[originL] -= dataunit;
					downloadnumG[destdc] -= dataunit;
				}
				if (mirrors[destdc][sourceId].id != -1)
				{
					if (mirrors[destdc][sourceId].outD == 0)
					{
						downloadnumA[destdc] += dataunit;
						uploadnumA[originL] += dataunit;
					}
				}
				else
				{
					downloadnumA[destdc] += dataunit;
					uploadnumA[originL] += dataunit;
				}
			}
			else if (v[sourceId].type == 0 && v[sourceId].label != originL) //high-degree mirror
			{
				masterin--;
				if (masterin == 0)
				{
					uploadnumG[originL] -= dataunit;
					downloadnumG[destdc] -= dataunit;
				}
				if (mirrors[originL][sourceId].outD == 1)
				{
					downloadnumA[originL] -= dataunit;
					uploadnumA[v[sourceId].label] -= dataunit;
				}
				if (v[sourceId].label != destdc)
				{
					if (mirrors[destdc][sourceId].id == -1)
					{
						uploadnumA[v[sourceId].label] += dataunit;
						downloadnumA[destdc] += dataunit;
					}
					else
					{
						if (mirrors[destdc][sourceId].outD == 0)
						{
							uploadnumA[v[sourceId].label] += dataunit;
							downloadnumA[destdc] += dataunit;
						}
					}
				}
			}
		}
		oedges = v[ver].outgoingEdges;
		for (it = oedges.begin(); it != oedges.end(); it++)
		{
			long long destId = mapped[(*it)->destID];
			if (v[destId].type == 0 && v[destId].label == originL) //high-degree master
			{
				masterout--;
				if (masterout == 0)
				{
					downloadnumA[originL] -= dataunit;
					uploadnumA[destdc] -= dataunit;
				}
				if (mirrors[destdc][destId].id == -1)
				{
					uploadnumG[destdc] += dataunit;
					downloadnumG[originL] += dataunit;
				}
				else
				{
					if (mirrors[destdc][destId].inD == 0)
					{
						uploadnumG[destdc] += dataunit;
						downloadnumG[originL] += dataunit;
					}
				}
			}
			else if (v[destId].type == 0 && v[destId].label != originL) //high-degree mirror
			{
				masterout--;
				if (masterout == 0)
				{
					downloadnumA[originL] -= dataunit;
					uploadnumA[destdc] -= dataunit;
				}
				if (mirrors[originL][destId].inD == 1)
				{
					uploadnumG[originL] -= dataunit;
					downloadnumG[v[destId].label] -= dataunit;
				}
				if (v[destId].label != destdc && mirrors[destdc][destId].id == -1)
				{
					uploadnumG[destdc] += dataunit;
					downloadnumG[v[destId].label] += dataunit;
				}
				else if (v[destId].label != destdc && mirrors[destdc][destId].id != -1)
				{
					if (mirrors[destdc][destId].inD == 0)
					{
						uploadnumG[destdc] += dataunit;
						downloadnumG[v[destId].label] += dataunit;
					}
				}
			}
		}
	}
	else if (v[ver].type == 0) //high-degree
	{

		for (int i = 0; i < algorithm->DCnums; i++)
		{
			if (mirrors[i][ver].id != -1 && mirrors[i][ver].inD > 0)
				downloadnumG[originL] -= dataunit;
			if (mirrors[i][ver].id != -1 && mirrors[i][ver].outD > 0)
				uploadnumA[originL] -= dataunit;
		}
		masterin = v[ver].idegreeinlabel;
		masterout = v[ver].odegreeinlabel;

		if (masterin > 0)
			uploadnumG[originL] += dataunit;
		if (masterout > 0)
			downloadnumA[originL] += dataunit;

		if (mirrors[destdc][ver].inD > 0)
			uploadnumG[destdc] -= dataunit;
		if (mirrors[destdc][ver].outD > 0)
			downloadnumA[destdc] -= dataunit;
		for (int i = 0; i < algorithm->DCnums; i++)
		{
			if (i != destdc && i != originL && mirrors[i][ver].inD > 0)
				downloadnumG[destdc] += dataunit;
			if (i != destdc && i != originL && mirrors[i][ver].outD > 0)
				uploadnumA[destdc] += dataunit;
		}

		if (masterin > 0)
			downloadnumG[destdc] += dataunit;
		if (masterout > 0)
			uploadnumA[destdc] += dataunit;

		oedges = v[ver].outgoingEdges;
		for (it = oedges.begin(); it != oedges.end(); it++)
		{
			long long destId = mapped[(*it)->destID];
			if (v[destId].type == 0 && v[destId].label == originL) //high-degree master
			{
				masterout--;
				if (masterout == 0)
				{
					downloadnumA[originL] -= dataunit;
					uploadnumA[destdc] -= dataunit;
				}
				if (mirrors[destdc][destId].id == -1)
				{
					uploadnumG[destdc] += dataunit;
					downloadnumG[originL] += dataunit;
				}
				else
				{
					if (mirrors[destdc][destId].inD == 0)
					{
						uploadnumG[destdc] += dataunit;
						downloadnumG[originL] += dataunit;
					}
				}
			}
			else if (v[destId].type == 0 && v[destId].label != originL) //high-degree mirror
			{
				masterout--;
				if (masterout == 0)
				{
					downloadnumA[originL] -= dataunit;
					uploadnumA[destdc] -= dataunit;
				}
				if (mirrors[originL][destId].inD == 1)
				{
					uploadnumG[originL] -= dataunit;
					downloadnumG[v[destId].label] -= dataunit;
				}
				if (v[destId].label != destdc && mirrors[destdc][destId].id == -1)
				{
					uploadnumG[destdc] += dataunit;
					downloadnumG[v[destId].label] += dataunit;
				}
				else if (v[destId].label != destdc && mirrors[destdc][destId].id != -1)
				{
					if (mirrors[destdc][destId].inD == 0)
					{
						uploadnumG[destdc] += dataunit;
						downloadnumG[v[destId].label] += dataunit;
					}
				}
			}
		}
	}
}
void *moveVertexBsp(void *arguments)
{
	Pthread_args *argss = (Pthread_args *)arguments;
	int thread_id = argss->id;
	delete argss;
	double *Upload = network->upload;
	double *Download = network->download;
	double *Upprice = network->upprice;
	long long *mapped = graph->mapped;
	double uploadnumsum[algorithm->DCnums];
	Vertex *v = graph->nodes;
	list<Edge *>::iterator it;
	Mirror **mirrors = graph->mirrors;
	list<Edge *> oedges;
	list<Edge *> iedges;
	//int originL=v[ver].label;
	int masterin = 0;
	int mirrorin = 0;
	int masterout = 0;
	int mirrorout = 0;
	while (true)
	{
		pthread_testcancel();
		if (!bspvec[thread_id].empty())
		{
			Pthread_args args = bspvec[thread_id][0];
			bspvec[thread_id].pop_back();
			int verindex = args.verindex;
			int ver = args.ver;
			double *downloadnumG = args.downloadnumG;
			double *uploadnumG = args.uploadnumG;
			double *downloadnumA = args.downloadnumA;
			double *uploadnumA = args.uploadnumA;
			int destdc = args.destdc;
			int iter = args.iteration;
			int *s = args.s;
			int originL = v[ver].label;
			double *maxArray = new double[algorithm->DCnums];
			double *maxArray2 = new double[algorithm->DCnums];
			if (v[ver].type == 1) //low-degree
			{
				for (int i = 0; i < algorithm->DCnums; i++)
					if (mirrors[i][ver].outD > 0)
						uploadnumA[originL] -= dataunit;

				masterin = v[ver].idegreeinlabel;
				masterout = v[ver].odegreeinlabel;

				if (masterin > 0)
					uploadnumG[originL] += dataunit;
				if (masterout > 0)
					downloadnumA[originL] += dataunit;

				mirrorin = mirrors[destdc][ver].inD;
				mirrorout = mirrors[destdc][ver].outD;

				if (mirrorout > 0)
					downloadnumA[destdc] -= dataunit;
				if (masterin > 0)
					downloadnumG[destdc] += dataunit;

				for (int i = 0; i < algorithm->DCnums; i++)
				{
					if (i != destdc && i != originL && mirrors[i][ver].outD > 0)
						uploadnumA[destdc] += dataunit;
				}

				if (masterout > 0)
					uploadnumA[destdc] += dataunit;

				iedges = v[ver].ingoingEdges;
				for (it = iedges.begin(); it != iedges.end(); it++)
				{
					long long sourceId = mapped[(*it)->sourceID];
					if (v[sourceId].type == 1 && v[sourceId].label == originL) //low-degree master
					{
						masterin--;
						if (masterin == 0)
						{
							uploadnumG[originL] -= dataunit;
							downloadnumG[destdc] -= dataunit;
						}
						if (mirrors[destdc][sourceId].id == -1)
						{
							uploadnumA[originL] += dataunit;
							downloadnumA[destdc] += dataunit;
						}
						else
						{
							//mirrors[destdc][sourceId].addOutD();
						}
					}
					else if (v[sourceId].type == 1 && v[sourceId].label != originL) //low-degree Mirror
					{
						masterin--;
						if (masterin == 0)
						{
							uploadnumG[originL] -= dataunit;
							downloadnumG[destdc] -= dataunit;
						}
						if (mirrors[originL][sourceId].outD == 1)
						{
							downloadnumA[originL] -= dataunit;
							uploadnumA[v[sourceId].label] -= dataunit;
						}
						if (v[sourceId].label != destdc)
						{
							if (mirrors[destdc][sourceId].id == -1)
							{
								downloadnumA[destdc] += dataunit;
								uploadnumA[v[sourceId].label] += dataunit;
							}
						}
					}
					else if (v[sourceId].type == 0 && v[sourceId].label == originL) //high-degree master
					{
						masterin--;
						if (masterin == 0)
						{
							uploadnumG[originL] -= dataunit;
							downloadnumG[destdc] -= dataunit;
						}
						if (mirrors[destdc][sourceId].id != -1)
						{
							if (mirrors[destdc][sourceId].outD == 0)
							{
								downloadnumA[destdc] += dataunit;
								uploadnumA[originL] += dataunit;
							}
						}
						else
						{
							downloadnumA[destdc] += dataunit;
							uploadnumA[originL] += dataunit;
						}
					}
					else if (v[sourceId].type == 0 && v[sourceId].label != originL) //high-degree mirror
					{
						masterin--;
						if (masterin == 0)
						{
							uploadnumG[originL] -= dataunit;
							downloadnumG[destdc] -= dataunit;
						}
						if (mirrors[originL][sourceId].outD == 1)
						{
							downloadnumA[originL] -= dataunit;
							uploadnumA[v[sourceId].label] -= dataunit;
						}
						if (v[sourceId].label != destdc)
						{
							if (mirrors[destdc][sourceId].id == -1)
							{
								uploadnumA[v[sourceId].label] += dataunit;
								downloadnumA[destdc] += dataunit;
							}
							else
							{
								if (mirrors[destdc][sourceId].outD == 0)
								{
									uploadnumA[v[sourceId].label] += dataunit;
									downloadnumA[destdc] += dataunit;
								}
							}
						}
					}
				}
				oedges = v[ver].outgoingEdges;
				for (it = oedges.begin(); it != oedges.end(); it++)
				{
					long long destId = mapped[(*it)->destID];
					if (v[destId].type == 0 && v[destId].label == originL) //high-degree master
					{
						masterout--;
						if (masterout == 0)
						{
							downloadnumA[originL] -= dataunit;
							uploadnumA[destdc] -= dataunit;
						}
						if (mirrors[destdc][destId].id == -1)
						{
							uploadnumG[destdc] += dataunit;
							downloadnumG[originL] += dataunit;
						}
						else
						{
							if (mirrors[destdc][destId].inD == 0)
							{
								uploadnumG[destdc] += dataunit;
								downloadnumG[originL] += dataunit;
							}
						}
					}
					else if (v[destId].type == 0 && v[destId].label != originL) //high-degree mirror
					{
						masterout--;
						if (masterout == 0)
						{
							downloadnumA[originL] -= dataunit;
							uploadnumA[destdc] -= dataunit;
						}
						if (mirrors[originL][destId].inD == 1)
						{
							uploadnumG[originL] -= dataunit;
							downloadnumG[v[destId].label] -= dataunit;
						}
						if (v[destId].label != destdc && mirrors[destdc][destId].id == -1)
						{
							uploadnumG[destdc] += dataunit;
							downloadnumG[v[destId].label] += dataunit;
						}
						else if (v[destId].label != destdc && mirrors[destdc][destId].id != -1)
						{
							if (mirrors[destdc][destId].inD == 0)
							{
								uploadnumG[destdc] += dataunit;
								downloadnumG[v[destId].label] += dataunit;
							}
						}
					}
				}
			}
			else if (v[ver].type == 0) //high-degree
			{
				for (int i = 0; i < algorithm->DCnums; i++)
				{
					if (mirrors[i][ver].id != -1 && mirrors[i][ver].inD > 0)
						downloadnumG[originL] -= dataunit;
					if (mirrors[i][ver].id != -1 && mirrors[i][ver].outD > 0)
						uploadnumA[originL] -= dataunit;
				}
				masterin = v[ver].idegreeinlabel;
				masterout = v[ver].odegreeinlabel;

				if (masterin > 0)
					uploadnumG[originL] += dataunit;
				if (masterout > 0)
					downloadnumA[originL] += dataunit;

				if (mirrors[destdc][ver].inD > 0)
					uploadnumG[destdc] -= dataunit;
				if (mirrors[destdc][ver].outD > 0)
					downloadnumA[destdc] -= dataunit;
				for (int i = 0; i < algorithm->DCnums; i++)
				{
					if (i != destdc && i != originL && mirrors[i][ver].inD > 0)
						downloadnumG[destdc] += dataunit;
					if (i != destdc && i != originL && mirrors[i][ver].outD > 0)
						uploadnumA[destdc] += dataunit;
				}

				if (masterin > 0)
					downloadnumG[destdc] += dataunit;
				if (masterout > 0)
					uploadnumA[destdc] += dataunit;

				oedges = v[ver].outgoingEdges;
				for (it = oedges.begin(); it != oedges.end(); it++)
				{
					long long destId = mapped[(*it)->destID];
					if (v[destId].type == 0 && v[destId].label == originL) //high-degree master
					{
						masterout--;
						if (masterout == 0)
						{
							downloadnumA[originL] -= dataunit;
							uploadnumA[destdc] -= dataunit;
						}
						if (mirrors[destdc][destId].id == -1)
						{
							uploadnumG[destdc] += dataunit;
							downloadnumG[originL] += dataunit;
						}
						else
						{
							if (mirrors[destdc][destId].inD == 0)
							{
								uploadnumG[destdc] += dataunit;
								downloadnumG[originL] += dataunit;
							}
						}
					}
					else if (v[destId].type == 0 && v[destId].label != originL) //high-degree mirror
					{
						masterout--;
						if (masterout == 0)
						{
							downloadnumA[originL] -= dataunit;
							uploadnumA[destdc] -= dataunit;
						}
						if (mirrors[originL][destId].inD == 1)
						{
							uploadnumG[originL] -= dataunit;
							downloadnumG[v[destId].label] -= dataunit;
						}
						if (v[destId].label != destdc && mirrors[destdc][destId].id == -1)
						{
							uploadnumG[destdc] += dataunit;
							downloadnumG[v[destId].label] += dataunit;
						}
						else if (v[destId].label != destdc && mirrors[destdc][destId].id != -1)
						{
							if (mirrors[destdc][destId].inD == 0)
							{
								uploadnumG[destdc] += dataunit;
								downloadnumG[v[destId].label] += dataunit;
							}
						}
					}
				}
			}
			/************************Gather Time***********************************/

			for (int j = 0; j < algorithm->DCnums; j++)
			{
				double divide = uploadnumG[j] / Upload[j];
				double divided = downloadnumG[j] / Download[j];
				maxArray[j] = divide > divided ? divide : divided;

				double divide2 = uploadnumA[j] / Upload[j];
				double divided2 = downloadnumA[j] / Download[j];
				maxArray2[j] = divide2 > divided2 ? divide2 : divided2;

				uploadnumsum[j] = uploadnumG[j] + uploadnumA[j];
			}

			double time = max_value(maxArray) + max_value(maxArray2);
			double cost = sumWeights(uploadnumsum, Upprice);

			double wan = 0;
			double totalwan = 0;
			for (int j = 0; j < algorithm->DCnums; j++)
			{
				wan = wan + uploadnumG[j] + downloadnumG[j] + uploadnumA[j] + downloadnumA[j];
				totalwan = totalwan + ug[j] + dg[j] + ua[j] + da[j];
			}

			double costdown = cost > budget ? cost : budget;
			double score = (data_transfer_time - time) / data_transfer_time + (data_transfer_cost - costdown) / data_transfer_cost;
			double score2 = (data_transfer_time - time) / data_transfer_time - (data_transfer_cost - cost) / data_transfer_cost;

			double rr = 0;
			if (v[ver].iniLabel == v[ver].label && v[ver].label != v[ver].action)
				rr = raw_data_size * Upprice[v[ver].iniLabel];
			else if (v[ver].iniLabel != v[ver].label && v[ver].label != v[ver].action && v[ver].iniLabel == v[ver].action)
				rr = -raw_data_size * Upprice[v[ver].iniLabel];

			double cost_A = data_transfer_cost * mvpercents + current_mvcost;
			double cost_B = cost * mvpercents + current_mvcost + rr;

			double belta = (cost_A - budget) / cost_A;
			double alpha = 1 - belta;
			if (belta < 0)
			{
				belta = 0;
				alpha = 1;
			}
			//double firstw,secondw;

			if (alpha == 1)
			{
			}
			if ((iter >= MAX_ITER - (MAX_ITER / 2 - 1) && cost_A > budget) || (iter == MAX_ITER && ((budget - cost_A) / budget) < 1e-5))
			{
				alpha = 0;
				belta = 1;
			}
			double newscore = 0;
			if (alpha != 1)
				newscore = alpha * (data_transfer_time - time) / data_transfer_time + belta * (cost_A - cost_B) / cost_A;
			else
			{
				double b = ((iter - 1) / 4) * 0.2;
				double a = 1 - b;
				if (optitime == 1)
				{
					a = 1;
					b = 0;
				}
				else
				{
					a = 0;
					b = 1;
				}
				newscore = a * (data_transfer_time - time) / data_transfer_time + b * (totalwan - wan) / totalwan;
			}

			if (newscore >= 0)
				s[verindex] = 1;

			delete[] maxArray;
			delete[] maxArray2;
			bsplock++;
		}
	}
}

struct my_mirror_change
{
	int dc;
	int ver;
	int inD = 0;
	int outD = 0;
	int master_dc = -1;
};

void my_move_mirror(vector<my_mirror_change> &mirror_change)
{

	for (int a = 0; a < mirror_change.size(); a++)
	{
		my_mirror_change &x = mirror_change[a];
		int dc = x.dc;
		int ver = x.ver;
		int inD = x.inD;
		int outD = x.outD;
		int master_dc = x.master_dc == -1 ? graph->getVertexs()[ver].label : x.master_dc;
		Mirror **mirrors = graph->getMirrors();
		Vertex *v = graph->getVertexs();
		pthread_mutex_lock(&mirror_mutex[dc][ver]);
		if (dc != master_dc) //for mirror
		{
			if (mirrors[dc][ver].inD <= 0 && inD > 0 && mirrors[dc][ver].inD + inD > 0)
			{
				atomic_uploadG[dc]++;		   //mirror upload
				atomic_downloadG[master_dc]++; //master download
			}

			else if (mirrors[dc][ver].inD > 0 && inD < 0 && mirrors[dc][ver].inD + inD <= 0)
			{
				atomic_uploadG[dc]--;		   //mirror upload
				atomic_downloadG[master_dc]--; //master download
			}

			if (mirrors[dc][ver].outD <= 0 && outD > 0 && mirrors[dc][ver].outD + outD > 0)
			{
				atomic_downloadA[dc]++;		 //mirror download
				atomic_uploadA[master_dc]++; //master upload
			}

			else if (mirrors[dc][ver].outD > 0 && outD < 0 && mirrors[dc][ver].outD + outD <= 0)
			{
				atomic_downloadA[dc]--;		 //mirror download
				atomic_uploadA[master_dc]--; //master upload
			}

			mirrors[dc][ver].inD += inD;
			mirrors[dc][ver].outD += outD;

			if (mirrors[dc][ver].inD <= 0 && mirrors[dc][ver].outD <= 0)
				mirrors[dc][ver].id = -1;
			else
				mirrors[dc][ver].id = ver;
		}
		else //for master
		{

			v[ver].idegreeinlabel += inD;
			v[ver].odegreeinlabel += outD;

			//mirrors[dc][ver].id = -1;
		}
		pthread_mutex_unlock(&mirror_mutex[dc][ver]);
	}
}

void *my_moveVertexBsp(void *arguments)
{

	Pthread_args *argss = (Pthread_args *)arguments;
	int thread_id = argss->id;
	delete argss;
	srand(time(NULL) * thread_id);

	double *Upload = network->upload;
	double *Download = network->download;
	double *Upprice = network->upprice;
	long long *mapped = graph->mapped;
	double uploadnumsum[algorithm->DCnums];
	Vertex *v = graph->nodes;
	list<Edge *>::iterator it;
	Mirror **mirrors = graph->mirrors;
	list<Edge *> oedges;
	list<Edge *> iedges;

	int masterin = 0;
	int mirrorin = 0;
	int masterout = 0;
	int mirrorout = 0;

	while (true)
	{

		pthread_testcancel();
		if (!bspvec[thread_id].empty())
		{
			// cout << "thread: " << thread_id << endl;
			Pthread_args args = bspvec[thread_id][0];
			bspvec[thread_id].pop_back();

			int ver = args.ver;
			double *downloadnumG = args.downloadnumG;
			double *uploadnumG = args.uploadnumG;
			double *downloadnumA = args.downloadnumA;
			double *uploadnumA = args.uploadnumA;

			vector<my_mirror_change> mirror_change;
			vector<my_mirror_change> master_change;

			int destdc = args.destdc;
			int iter = args.iteration;

			int originL = v[ver].label;
			double maxArray[algorithm->DCnums];
			double maxArray2[algorithm->DCnums];

			if (v[ver].type == 1) //low-degree
			{
				for (int i = 0; i < algorithm->DCnums; i++)
				{
					if (mirrors[i][ver].outD > 0)
						uploadnumA[originL] -= dataunit;

					if (mirrors[i][ver].id == ver)
					{

						master_change.push_back({i, ver, -mirrors[i][ver].inD, -mirrors[i][ver].outD, originL});
						if (i != destdc)
							master_change.push_back({i, ver, mirrors[i][ver].inD, mirrors[i][ver].outD, destdc});
					}
				}

				master_change.push_back({destdc, ver, mirrors[destdc][ver].inD - v[ver].idegreeinlabel, mirrors[destdc][ver].outD - v[ver].odegreeinlabel, destdc});

				master_change.push_back({originL, ver, v[ver].idegreeinlabel, v[ver].odegreeinlabel, destdc});

				masterin = v[ver].idegreeinlabel;
				masterout = v[ver].odegreeinlabel;

				if (masterin > 0)
					uploadnumG[originL] += dataunit;

				if (masterout > 0)
					downloadnumA[originL] += dataunit;

				mirrorin = mirrors[destdc][ver].inD;
				mirrorout = mirrors[destdc][ver].outD;

				if (mirrorout > 0)
					downloadnumA[destdc] -= dataunit;

				if (masterin > 0)
					downloadnumG[destdc] += dataunit;

				for (int i = 0; i < algorithm->DCnums; i++)
				{
					if (i != destdc && i != originL && mirrors[i][ver].outD > 0)
						uploadnumA[destdc] += dataunit;
				}

				if (masterout > 0)
					uploadnumA[destdc] += dataunit;

				iedges = v[ver].ingoingEdges;
				for (it = iedges.begin(); it != iedges.end(); it++)
				{
					long long sourceId = mapped[(*it)->sourceID];

					mirror_change.push_back({originL, ver, -1, 0, destdc});
					mirror_change.push_back({destdc, ver, 1, 0, destdc});

					mirror_change.push_back({originL, (int)sourceId, 0, -1, -1});
					mirror_change.push_back({destdc, (int)sourceId, 0, 1, -1});
					if (v[sourceId].type == 1 && v[sourceId].label == originL) //low-degree master
					{

						masterin--;
						if (masterin == 0)
						{
							uploadnumG[originL] -= dataunit;
							downloadnumG[destdc] -= dataunit;
						}
						if (mirrors[destdc][sourceId].id == -1)
						{

							uploadnumA[originL] += dataunit;
							downloadnumA[destdc] += dataunit;
						}
						else
						{
							//mirrors[destdc][sourceId].addOutD();
						}
					}
					else if (v[sourceId].type == 1 && v[sourceId].label != originL) //low-degree Mirror
					{

						masterin--;

						if (masterin == 0)
						{
							uploadnumG[originL] -= dataunit;
							downloadnumG[destdc] -= dataunit;
						}
						if (mirrors[originL][sourceId].outD == 1)
						{
							downloadnumA[originL] -= dataunit;
							uploadnumA[v[sourceId].label] -= dataunit;
						}
						if (v[sourceId].label != destdc)
						{
							if (mirrors[destdc][sourceId].id == -1)
							{
								downloadnumA[destdc] += dataunit;
								uploadnumA[v[sourceId].label] += dataunit;
							}
						}
					}
					else if (v[sourceId].type == 0 && v[sourceId].label == originL) //high-degree master
					{
						masterin--;
						if (masterin == 0)
						{
							uploadnumG[originL] -= dataunit;
							downloadnumG[destdc] -= dataunit;
						}
						if (mirrors[destdc][sourceId].id != -1)
						{
							if (mirrors[destdc][sourceId].outD == 0)
							{
								downloadnumA[destdc] += dataunit;
								uploadnumA[originL] += dataunit;
							}
						}
						else
						{
							downloadnumA[destdc] += dataunit;
							uploadnumA[originL] += dataunit;
						}
					}
					else if (v[sourceId].type == 0 && v[sourceId].label != originL) //high-degree mirror
					{

						masterin--;
						if (masterin == 0)
						{
							uploadnumG[originL] -= dataunit;
							downloadnumG[destdc] -= dataunit;
						}
						if (mirrors[originL][sourceId].outD == 1)
						{
							downloadnumA[originL] -= dataunit;
							uploadnumA[v[sourceId].label] -= dataunit;
						}
						if (v[sourceId].label != destdc)
						{
							if (mirrors[destdc][sourceId].id == -1)
							{
								uploadnumA[v[sourceId].label] += dataunit;
								downloadnumA[destdc] += dataunit;
							}
							else
							{
								if (mirrors[destdc][sourceId].outD == 0)
								{
									uploadnumA[v[sourceId].label] += dataunit;
									downloadnumA[destdc] += dataunit;
								}
							}
						}
					}
				}
				oedges = v[ver].outgoingEdges;
				for (it = oedges.begin(); it != oedges.end(); it++)
				{
					long long destId = mapped[(*it)->destID];
					if (v[destId].type == 0)
					{
						mirror_change.push_back({originL, ver, 0, -1, destdc});
						mirror_change.push_back({destdc, ver, 0, 1, destdc});
						mirror_change.push_back({originL, (int)destId, -1, 0, -1});
						mirror_change.push_back({destdc, (int)destId, 1, 0, -1});
					}
					if (v[destId].type == 0 && v[destId].label == originL) //high-degree master
					{
						masterout--;
						if (masterout == 0)
						{
							downloadnumA[originL] -= dataunit;
							uploadnumA[destdc] -= dataunit;
						}
						if (mirrors[destdc][destId].id == -1)
						{
							uploadnumG[destdc] += dataunit;
							downloadnumG[originL] += dataunit;
						}
						else
						{
							if (mirrors[destdc][destId].inD == 0)
							{
								uploadnumG[destdc] += dataunit;
								downloadnumG[originL] += dataunit;
							}
						}
					}
					else if (v[destId].type == 0 && v[destId].label != originL) //high-degree mirror
					{
						masterout--;
						if (masterout == 0)
						{
							downloadnumA[originL] -= dataunit;
							uploadnumA[destdc] -= dataunit;
						}
						if (mirrors[originL][destId].inD == 1)
						{
							uploadnumG[originL] -= dataunit;
							downloadnumG[v[destId].label] -= dataunit;
						}
						if (v[destId].label != destdc && mirrors[destdc][destId].id == -1)
						{
							uploadnumG[destdc] += dataunit;
							downloadnumG[v[destId].label] += dataunit;
						}
						else if (v[destId].label != destdc && mirrors[destdc][destId].id != -1)
						{
							if (mirrors[destdc][destId].inD == 0)
							{
								uploadnumG[destdc] += dataunit;
								downloadnumG[v[destId].label] += dataunit;
							}
						}
					}
				}
			}

			/************************Gather Time***********************************/

			for (int j = 0; j < algorithm->DCnums; j++)
			{
				double divide = uploadnumG[j] / Upload[j];
				double divided = downloadnumG[j] / Download[j];
				maxArray[j] = divide > divided ? divide : divided;

				double divide2 = uploadnumA[j] / Upload[j];
				double divided2 = downloadnumA[j] / Download[j];
				maxArray2[j] = divide2 > divided2 ? divide2 : divided2;

				uploadnumsum[j] = uploadnumG[j] + uploadnumA[j];
			}
			double time = max_value(maxArray) + max_value(maxArray2);
			double cost = sumWeights(uploadnumsum, Upprice);

			double wan = 0;
			double totalwan = 0;
			for (int j = 0; j < algorithm->DCnums; j++)
			{
				wan = wan + uploadnumG[j] + downloadnumG[j] + uploadnumA[j] + downloadnumA[j];
				totalwan = totalwan + ug[j] + dg[j] + ua[j] + da[j];
			}
			/************************Apply Time***********************************/
			double costdown = cost > budget ? cost : budget;
			double score = (data_transfer_time - time) / data_transfer_time + (data_transfer_cost - costdown) / data_transfer_cost;
			double score2 = (data_transfer_time - time) / data_transfer_time - (data_transfer_cost - cost) / data_transfer_cost;

			double rr = 0;
			if (v[ver].iniLabel == v[ver].label && v[ver].label != v[ver].action)
				rr = raw_data_size * Upprice[v[ver].iniLabel];
			else if (v[ver].iniLabel != v[ver].label && v[ver].label != v[ver].action && v[ver].iniLabel == v[ver].action)
				rr = -raw_data_size * Upprice[v[ver].iniLabel];

			double cost_A = data_transfer_cost * mvpercents + current_mvcost;
			double cost_B = cost * mvpercents + current_mvcost + rr;

			double belta = (cost_A - budget) / cost_A;
			double alpha = 1 - belta;
			if (belta < 0)
			{
				belta = 0;
				alpha = 1;
			}
			if (alpha == 1)
			{
			}
			if ((iter >= MAX_ITER - (MAX_ITER / 2 - 1) && cost_A > budget) || (iter == MAX_ITER && ((budget - cost_A) / budget) < 1e-5))
			{
				alpha = 0;
				belta = 1;
			}
			double newscore = 0;
			if (alpha != 1)
				newscore = alpha * (data_transfer_time - time) / data_transfer_time + belta * (cost_A - cost_B) / cost_A;
			else
			{
				double b = ((iter - 1) / 4) * 0.2;
				double a = 1 - b;
				if (optitime == 1)
				{
					a = 1;
					b = 0;
				}
				else
				{
					a = 0;
					b = 1;
				}
				newscore = a * (data_transfer_time - time) / data_transfer_time + b * (totalwan - wan) / totalwan;
			}

			if (newscore >= 0)
			{
				pthread_mutex_lock(&mutex);
				if (iter == 1 && v[ver].iniLabel != v[ver].action)
				{
					current_mvcost += raw_data_size * Upprice[v[ver].iniLabel];
				}
				else if (iter > 1)
				{
					if (v[ver].iniLabel == v[ver].label && v[ver].label != v[ver].action)
						current_mvcost += raw_data_size * Upprice[v[ver].iniLabel];
					else if (v[ver].iniLabel != v[ver].label && v[ver].label != v[ver].action)
					{
						if (v[ver].iniLabel == v[ver].action)
							current_mvcost -= raw_data_size * Upprice[v[ver].iniLabel];
					}
				}

				v[ver].label = destdc;

				pthread_mutex_unlock(&mutex);
				my_move_mirror(master_change);
			}

			ready_thread++;

			sem_wait(&ready);
			if (newscore >= 0)
			{
				my_move_mirror(mirror_change);
			}

			bsplock++;
		}
	}
}

void calculateTimeAndPrice(double &data_transfer_time, double &data_transfer_cost) //������ʱ����ܻ���
{
	Vertex *v = graph->getVertexs();
	list<int>::iterator it;
	Mirror **mirrors = graph->getMirrors();
	long long *mapped = graph->getMapped();
	double *Upload = network->getUpload();
	double *Download = network->getDownload();
	double *Upprice = network->getUpprice();
	double *Uptime = new double[algorithm->getDCNums()];   //ÿ������upload����
	double *Downtime = new double[algorithm->getDCNums()]; //ÿ������download����

	for (int i = 0; i < algorithm->getDCNums(); i++)
	{
		Uptime[i] = 0;
		Downtime[i] = 0;
	}
	/*--------------------------------Gather Stage----------------------------------*/
	for (int i = 0; i < algorithm->getDCNums(); i++)
	{
		for (int j = 0; j < graph->getNum_Nodes(); j++)
			if (mirrors[i][j].getID() != -1 && v[j].getType() == 0 && mirrors[i][j].getInD() != 0)
			{
				Uptime[i] += dataunit;
				Downtime[v[j].getLabel()] += dataunit;
			}
	}

	for (int w = 0; w < algorithm->DCnums; w++)
	{
		ug[w] = Uptime[w];
		dg[w] = Downtime[w];
		//cout<<Uptime[w]<<" "<<Downtime[w]<<" " <<network->uploadnumG[w]<<" "<<network->downloadnumG[w]<<endl;
	}

	double *UptimeS = new double[algorithm->getDCNums()];
	double *DowntimeS = new double[algorithm->getDCNums()];
	double *maxArray = new double[algorithm->getDCNums()];

	for (int i = 0; i < algorithm->getDCNums(); i++)
	{

		maxArray[i] = Uptime[i] / Upload[i] > Downtime[i] / Download[i] ? Uptime[i] / Upload[i] : Downtime[i] / Download[i];
		UptimeS[i] = 0;
		DowntimeS[i] = 0;
	}

	data_transfer_time = max_value(maxArray);

	/*--------------------------------Apply Stage----------------------------------*/
	for (int i = 0; i < algorithm->getDCNums(); i++)
	{
		for (int j = 0; j < graph->getNum_Nodes(); j++)
		{
			if (mirrors[i][j].getID() != -1 && mirrors[i][j].getOutD() != 0)
			{
				Downtime[i] += dataunit;
				Uptime[v[j].getLabel()] += dataunit;
				DowntimeS[i] += dataunit;
				UptimeS[v[j].getLabel()] += dataunit;
			}
		}
	}
	for (int w = 0; w < algorithm->DCnums; w++)
	{
		ua[w] = UptimeS[w];
		da[w] = DowntimeS[w];
	}
	/*--------------------------------------------------------------------------------*/
	data_transfer_cost = sumWeights(Uptime, Upprice);

	Partition *partitions = new Partition[algorithm->getDCNums()];

	for (int i = 0; i < algorithm->getDCNums(); i++)
	{
		partitions[i].setUploadNum(Uptime[i]);
		partitions[i].setDownloadNum(Downtime[i]);
		maxArray[i] = UptimeS[i] / Upload[i] > DowntimeS[i] / Download[i] ? UptimeS[i] / Upload[i] : DowntimeS[i] / Download[i];
	}

	network->setPartition(partitions);
	data_transfer_time += max_value(maxArray);

	delete[] Uptime;
	delete[] Downtime;
	delete[] maxArray;
	delete[] UptimeS;
	delete[] DowntimeS;
}

void powerlyrareCalTimeAndCost(double &data_transfer_time, double &data_transfer_cost)
{
	Vertex *v = graph->getVertexs();
	list<int>::iterator it;
	Mirror **mirrors = graph->getMirrors();
	long long *mapped = graph->getMapped();
	double *Upload = network->getUpload();
	double *Download = network->getDownload();
	double *Upprice = network->getUpprice();
	double *maxArray = new double[algorithm->getDCNums()];
	double *Uptime = new double[algorithm->getDCNums()];	//ÿ������upload����
	double *Downtime = new double[algorithm->getDCNums()];	//ÿ������download����
	double *UptimeS = new double[algorithm->getDCNums()];	//ÿ������upload����
	double *DowntimeS = new double[algorithm->getDCNums()]; //ÿ������download����

	for (int i = 0; i < algorithm->getDCNums(); i++)
	{
		Uptime[i] = 0;
		Downtime[i] = 0;
		UptimeS[i] = 0;
		DowntimeS[i] = 0;
	}
	/*--------------------------------Gather Stage----------------------------------*/
	for (int i = 0; i < algorithm->getDCNums(); i++)
	{
		for (int j = 0; j < graph->getNum_Nodes(); j++)
			if (mirrors[i][j].getID() != -1 && v[j].getType() == 0)
			{
				Uptime[i] += dataunit;
				Downtime[v[j].getLabel()] += dataunit;
			}
	}

	for (int i = 0; i < algorithm->getDCNums(); i++)
	{
		maxArray[i] = Uptime[i] / Upload[i] > Downtime[i] / Download[i] ? Uptime[i] / Upload[i] : Downtime[i] / Download[i];
	}

	data_transfer_time = max_value(maxArray);

	/*--------------------------------Apply Stage----------------------------------*/
	for (int i = 0; i < algorithm->getDCNums(); i++)
	{
		for (int j = 0; j < graph->getNum_Nodes(); j++)
		{
			if (mirrors[i][j].getID() != -1)
			{
				Downtime[i] += dataunit;
				Uptime[v[j].getLabel()] += dataunit;
				DowntimeS[i] += dataunit;
				UptimeS[v[j].getLabel()] += dataunit;
			}
		}
	}

	/*--------------------------------------------------------------------------------*/
	data_transfer_cost = sumWeights(Uptime, Upprice);

	for (int i = 0; i < algorithm->getDCNums(); i++)
	{
		maxArray[i] = UptimeS[i] / Upload[i] > DowntimeS[i] / Download[i] ? UptimeS[i] / Upload[i] : DowntimeS[i] / Download[i];
	}

	data_transfer_time += max_value(maxArray);

	delete[] Uptime;
	delete[] Downtime;
	delete[] UptimeS;
	delete[] DowntimeS;
	delete[] maxArray;
}
void *MyActionSelect(void *arguments)
{
	Pthread_args *args = (Pthread_args *)arguments;
	unsigned int low = args->getLow();
	unsigned int high = args->getHigh();
	int iter = args->getIter();
	srand(time(NULL) * +low);

	double **UCB = algorithm->UCB_value;

	Vertex *v = graph->getVertexs();
	double epison = 0.8;

	/***************************choice max UCB value**************************************************/
	for (int i = low; i <= high; i++)
	{
		unordered_set<int>::iterator itora = trained.find(i);
		if (itora != trained.end())
		{

			int index = my_max_value_index(UCB[i], algorithm->getDCNums());
			algorithm->select_time[i][index]++;
			v[i].setAction(index);
		}
	}
	delete args;
}
void *Sampling(void *arguments)
{

	Pthread_args *args = (Pthread_args *)arguments;
	unsigned int low = args->low;
	unsigned int high = args->high;
	srand(time(NULL) + high);
	for (int i = low; i <= high; i++)
	{
		if (1. * rand() / RAND_MAX <= randpro)
		{
			pthread_mutex_lock(&mutex);
			trained.insert(i);
			pthread_mutex_unlock(&mutex);
		}
	}
	delete args;
}

void *probability_Update(void *arguments)
{
	Pthread_args *args = (Pthread_args *)arguments;
	unsigned int low = args->getLow();
	unsigned int high = args->getHigh();
	srand(time(NULL) + high);
	int iter = args->getIter();
	double **probability = algorithm->getProbability();
	double alpha = 0.6;
	double belta = 0.2;

	Vertex *v = graph->getVertexs();
	double *signal = NULL;

	double **sum_score = algorithm->sum_score;
	double **UCB_value = algorithm->UCB_value;
	long long **select_time = algorithm->select_time;
	long long *choice_T = algorithm->choice_T;

	for (int i = low; i <= high; i++)
	{
		unordered_set<int>::iterator itora = trained.find(i);
		signal = v[i].getSignal_();
		if (itora != trained.end())
		{
			choice_T[i]++;

			int index = max_value_index(signal, algorithm->getDCNums());
			for (int j = 0; j < algorithm->getDCNums(); j++)
			{

				if (index == v[i].action || true)
				{
					if (index == j)
						probability[i][j] += alpha * (1 - probability[i][j]);
					else
						probability[i][j] = probability[i][j] * (1 - alpha);
				}
				else
				{
					if (index == j)
						probability[i][j] = probability[i][j] * (1 - belta);
					else
						probability[i][j] = probability[i][j] * (1 - belta) + belta / (algorithm->DCnums - 1);
				}
				sum_score[i][j] += probability[i][j];
				UCB_value[i][j] = sum_score[i][j] / choice_T[i] + sqrt(c * log(choice_T[i]) / select_time[i][j]);
			}
		}
		for (int k = 0; k < algorithm->getDCNums(); k++)
		{
			signal[k] = 0;
		}
	}
	delete args;
}

void *retrain_Sampling(void *arguments)
{

	Pthread_args *args = (Pthread_args *)arguments;
	unsigned int low = args->low;
	unsigned int high = args->high;
	srand(time(NULL) + high);
	for (int i = low; i <= high; i++)
	{
		// chose from retrain_node
		if (retrain_node.count(i) && 1. * rand() / RAND_MAX && 1. * rand() / RAND_MAX <= randpro)
		{
			pthread_mutex_lock(&mutex);
			trained.insert(i);
			pthread_mutex_unlock(&mutex);
		}
	}
	delete args;
}

double budget_rate = budget;
// before using dynamic part, you should forbid adding some edge in "void initialEdgeI()" and put them in not_load_edge.
vector<Edge *> not_load_edge;
// the rate you want to reload to graph in not_load_edge.
double ignore_edge_rate = 0;
void RLcut_dynamic()
{

	int flag = 0;
	double *signal_ = NULL;
	double timeOld = 0;
	Vertex *v = graph->getVertexs();
	double *uploadnumG = new double[algorithm->getDCNums()];
	double *downloadnumG = new double[algorithm->getDCNums()];
	double *uploadnumA = new double[algorithm->getDCNums()];
	double *downloadnumA = new double[algorithm->getDCNums()];
	double *maxArray = new double[algorithm->getDCNums()];
	double *maxArray2 = new double[algorithm->getDCNums()];
	double *uploadnumsum = new double[algorithm->getDCNums()];
	double *Upload = network->getUpload();
	double *Download = network->getDownload();
	double *Upprice = network->getUpprice();
	double *SR = new double[MAX_ITER + 1];
	double *TOEI = new double[MAX_ITER + 1];

	budget_rate = budget;
	cout << "budget_rate = " << budget_rate << endl;

	DC_mutex = new pthread_mutex_t[algorithm->DCnums];

	initPartitioning();
	calculateTimeAndPrice(data_transfer_time, data_transfer_cost);
	Mirror **mirrors = graph->getMirrors();
	Partition *partition = network->getPartition();
	pthread_t tid[MAX_THREADS_NUM];
	pthread_t tidd[MAX_THREADS_NUM];
	pthread_t tid_bsp[batchsize];
	int *eachThreadsNum = new int[MAX_THREADS_NUM];
	cout << "batchsize: " << batchsize << endl;
	cout << "before executing the algorithm:" << endl;
	cout << "#data_transfer_time: " << data_transfer_time << endl;
	cout << "#data_transfer_cost: " << data_transfer_cost << endl;
	cout << "The seed is: " << Seed << endl;

	outfile = ofstream(outputfile);

	ofstream outtrain(FN);
	if (data_transfer_cost <= budget)
	{
		F1 = 0;
		F2 = 1;
		flag = 0;
	}
	else
	{
		F1 = 1;
		F2 = 0;
		flag = 1;
	}

	for (int i = 0; i < graph->getNum_Nodes(); i++)
		v[i].initSignal_();

	multiprocessing_pool(eachThreadsNum);

	for (int i = 0; i < graph->getNum_Nodes(); i++)
		v[i].action = v[i].label;

	budget = global_mvcost * budget_rate + mvpercents * ginger_cost;
	cout << "The budget is " << budget << endl;

	map<int, int> cnt;
	for (int i = 0; i < graph->getNum_Nodes(); i++)
		cnt[v[i].getIngoingEdges().size()]++;
	int t = 20;
	int sum = 0;
	int train_num = 0;
	for (auto &x : cnt)
	{
		sum += x.second * x.first;
		train_num += x.second;

		if (1. * sum / graph->getNum_Edges() >= train_edge_rate)
		{
			k_degree = x.first;
			cout << "k = " << k_degree << ", training " << 1. * sum / graph->getNum_Edges() << " of edges" << endl;
			outfile << "train_edge_rate = " << train_edge_rate << ", k = " << k_degree << ", training " << 1. * sum / graph->getNum_Edges() << " of edges" << endl;

			;
			break;
		}
	}

	cout << "-----------------------------------------------------------------" << endl;

	while (iter <= MAX_ITER)
	{
		/********************************Init*************************************/
		for (int i = 0; i < algorithm->DCnums; i++)
		{
			uploadnumG[i] = ug[i];
			downloadnumG[i] = dg[i];
			uploadnumA[i] = ua[i];
			downloadnumA[i] = da[i];
			maxArray[i] = 0;
		}

		time_t start = time(NULL);

		if (iter > 0)
		{
			/************************************MyActionSelect***************************************/
			int low1 = 0;
			int high1 = eachThreadsNum[0] - 1;
			int status1 = 0;
			start = time(NULL);
			high_resolution_clock::time_point beginTime = high_resolution_clock::now();

			for (int j = 0; j < MAX_THREADS_NUM; j++)
			{
				Pthread_args *args = new Pthread_args();
				args->setLow(low1);
				args->setHigh(high1);
				args->setIter(iter);
				status1 = pthread_create(&tid[j], NULL, MyActionSelect, (void *)args);
				low1 = high1 + 1;
				if (j < MAX_THREADS_NUM - 1)
					high1 = low1 + eachThreadsNum[j + 1] - 1;
			}

			for (int j = 0; j < MAX_THREADS_NUM; j++)
				pthread_join(tid[j], NULL);

			// t1=time(NULL)-start;
			high_resolution_clock::time_point endTime = high_resolution_clock::now();
			milliseconds timeInterval = std::chrono::duration_cast<milliseconds>(endTime - beginTime);
			t1 = timeInterval.count();
			timeOld = data_transfer_time;
			priceOld = data_transfer_cost;

			/*-------------------------------------------Vertex Migrate------------------------------------------*/
			if (bsp == 0)
			{

				int percent = 0;
				int decidedtomove = 0;
				start = time(NULL);
				unordered_set<int>::iterator itora = trained.begin();
				while (itora != trained.end())
				{
					int i = *itora;
					int ori = v[i].label;
					//start=time(NULL);
					if (v[i].label != v[i].action)
					{
						decidedtomove++;
						vertexMigrate(i, downloadnumG, uploadnumG, downloadnumA, uploadnumA, v[i].action);

						/************************Gather***********************************/

						for (int j = 0; j < algorithm->DCnums; j++)
						{
							double divide = uploadnumG[j] / Upload[j];
							double divided = downloadnumG[j] / Download[j];
							maxArray[j] = divide > divided ? divide : divided;
						}

						double time = max_value(maxArray);

						/************************Apply***********************************/
						for (int j = 0; j < algorithm->DCnums; j++)
						{
							double divide = uploadnumA[j] / Upload[j];
							double divided = downloadnumA[j] / Download[j];
							maxArray[j] = divide > divided ? divide : divided;
						}
						time += max_value(maxArray);

						for (int j = 0; j < algorithm->DCnums; j++)
						{
							uploadnumsum[j] = uploadnumG[j] + uploadnumA[j];
						}
						double cost = sumWeights(uploadnumsum, Upprice);

						if (time <= data_transfer_time)
						{
							data_transfer_time = time;
							data_transfer_cost = cost;
						}
						else
						{
							percent++;
							vertexMigrate(i, downloadnumG, uploadnumG, downloadnumA, uploadnumA, ori);
						}
					}
					itora++;
				}

				for (int i = 0; i < algorithm->DCnums; i++)
				{
					ug[i] = uploadnumG[i];
					dg[i] = downloadnumG[i];
					ua[i] = uploadnumA[i];
					da[i] = downloadnumA[i];
				}

				t2 = time(NULL) - start;
			}
			else if (bsp == 1)
			{

				for (int j = 0; j < batchsize; j++)
				{
					Pthread_args *args = new Pthread_args();
					args->id = j;
					status1 = pthread_create(&tid_bsp[j], NULL, my_moveVertexBsp, (void *)args);
				}
				unordered_set<int>::iterator itora = trained.begin();
				int *verID = new int[batchsize];

				double **ulg = new double *[batchsize];
				double **dlg = new double *[batchsize];
				double **ula = new double *[batchsize];
				double **dla = new double *[batchsize];
				for (int i = 0; i < batchsize; i++)
				{
					ulg[i] = new double[algorithm->DCnums];
					dlg[i] = new double[algorithm->DCnums];
					ula[i] = new double[algorithm->DCnums];
					dla[i] = new double[algorithm->DCnums];
				}
				vector<int> k_degree_trained_2, k_degree_trained;
				int t = 10;
				while (itora != trained.end())
				{
					if (v[*itora].label != v[*itora].action)
					{
						k_degree_trained.push_back(*itora);
					}
					//if(t-- >= 0)	cout << itora->first << " " ;
					itora++;
				}
				cout << endl;

				// int size = k_degree_trained_2.size();
				// for (int i = 0; i < size / batchsize; i++)
				// 	for (int j = i; j < k_degree_trained_2.size(); j += size / batchsize)
				// 		k_degree_trained.push_back(k_degree_trained_2[j]);

				start = time(NULL);
				atomic_uploadG = vector<atomic_int>(algorithm->getDCNums());
				atomic_uploadA = vector<atomic_int>(algorithm->getDCNums());
				atomic_downloadG = vector<atomic_int>(algorithm->getDCNums());
				atomic_downloadA = vector<atomic_int>(algorithm->getDCNums());
				high_resolution_clock::time_point beginTime = high_resolution_clock::now();
				double o = 0;
				time_t s = time(NULL);
				int vertype = 0;
				int trained_cnt = 0;

				while (trained_cnt < k_degree_trained.size())
				{
					int numver = 0;
					sem_init(&ready, 0, 0);

					for (int i = 0; i < algorithm->DCnums; i++)
					{
						atomic_uploadG[i] = 0;
						atomic_uploadA[i] = 0;
						atomic_downloadG[i] = 0;
						atomic_downloadA[i] = 0;
					}

					while (trained_cnt < k_degree_trained.size() && numver < batchsize)
					{

						verID[numver] = k_degree_trained[trained_cnt];
						// sig[numver] = 0;
						numver++;

						trained_cnt++;
					}

					for (int i = 0; i < numver; i++)
					{
						for (int j = 0; j < algorithm->DCnums; j++)
						{
							ulg[i][j] = ug[j];
							dlg[i][j] = dg[j];
							ula[i][j] = ua[j];
							dla[i][j] = da[j];
						}
					}

					s = time(NULL);
					high_resolution_clock::time_point parallel_begin = high_resolution_clock::now();

					for (int j = 0; j < numver; j++)
					{
						// cout<<"j: "<<j<<endl;
						Pthread_args args = Pthread_args();
						args.id = j;
						args.iteration = iter;
						args.verindex = j;
						args.ver = verID[j];
						args.downloadnumG = dlg[j];
						args.uploadnumG = ulg[j];
						args.downloadnumA = dla[j];
						args.uploadnumA = ula[j];
						args.destdc = v[verID[j]].action;

						bspvec[j].push_back(args);
					}

					while (ready_thread != numver)
						;
					ready_thread = 0;

					for (int i = 0; i < numver; i++)
						sem_post(&ready);

					while (bsplock != numver)
					{
					}
					bsplock = 0;
					for (int i = 0; i < algorithm->getDCNums(); i++)
					{
						ua[i] += atomic_uploadA[i] * dataunit;
						ug[i] += atomic_uploadG[i] * dataunit;
						da[i] += atomic_downloadA[i] * dataunit;
						dg[i] += atomic_downloadG[i] * dataunit;
					}

					o += time(NULL) - s;

					/************************Time***********************************/

					for (int j = 0; j < algorithm->DCnums; j++)
					{
						double divide = ug[j] / Upload[j];
						double divided = dg[j] / Download[j];
						maxArray[j] = divide > divided ? divide : divided;

						double divide2 = ua[j] / Upload[j];
						double divided2 = da[j] / Download[j];
						maxArray2[j] = divide2 > divided2 ? divide2 : divided2;

						uploadnumsum[j] = ug[j] + ua[j];
					}

					double time = max_value(maxArray) + max_value(maxArray2);
					double cost = sumWeights(uploadnumsum, Upprice);
					data_transfer_time = time;
					data_transfer_cost = cost;
				}

				for (int j = 0; j < batchsize; j++)
					pthread_cancel(tid_bsp[j]);

				for (int j = 0; j < batchsize; j++)
					pthread_join(tid_bsp[j], NULL);

				high_resolution_clock::time_point endTime = high_resolution_clock::now();
				milliseconds timeInterval = std::chrono::duration_cast<milliseconds>(endTime - beginTime);
				t2 = timeInterval.count();

				delete[] verID;
				// delete[] sig;
				for (int i = 0; i < batchsize; i++)
				{
					delete[] ulg[i];
					delete[] dlg[i];
					delete[] ula[i];
					delete[] dla[i];
				}
				delete[] ulg;
				delete[] dlg;
				delete[] ula;
				delete[] dla;
			}
			TOEI[iter - 1] = (t1 + t2 + t3 + t4 + t5) / 1000;
			SR[iter - 1] = randpro;
			double usedtime = 0;
			for (int p = 0; p < iter; p++)
				usedtime += TOEI[p];
			double lefteach = (L - usedtime) / (MAX_ITER - iter);
			int recent = 3;
			// int recent = 1;
			if (lefteach <= 0)
				randpro = 0;
			else
			{
				if (iter >= recent)
				{
					double total_sr = 0;
					for (int p = iter - 1; p >= iter - recent; p--)
						total_sr += lefteach / TOEI[p] * SR[p];
					randpro = total_sr / recent;
				}
				else
				{
					double total_sr = 0;
					for (int p = 0; p <= iter - 1; p++)
						total_sr += lefteach / TOEI[p] * SR[p];
					randpro = total_sr / recent;
				}
			}
			//randpro=0.01;
			// if (randpro > 1)

			// in dynamic part, we use randpro = 1 in training the subgraph.
			randpro = 1;

			trained.clear();
			{
				int low1 = 0;
				int high1 = eachThreadsNum[0] - 1;
				int status1 = 0;
				start = time(NULL);
				high_resolution_clock::time_point beginTime = high_resolution_clock::now();
				for (int j = 0; j < MAX_THREADS_NUM; j++)
				{
					Pthread_args *args = new Pthread_args();
					args->setLow(low1);
					args->setHigh(high1);
					args->setIter(iter);
					status1 = pthread_create(&tid[j], NULL, Sampling, (void *)args);
					low1 = high1 + 1;
					if (j < MAX_THREADS_NUM - 1)
						high1 = low1 + eachThreadsNum[j + 1] - 1;
				}

				for (int j = 0; j < MAX_THREADS_NUM; j++)
					pthread_join(tid[j], NULL);

				high_resolution_clock::time_point endTime = high_resolution_clock::now();
				milliseconds timeInterval = std::chrono::duration_cast<milliseconds>(endTime - beginTime);
				// t3 = timeInterval.count();
				t3 = time(NULL) - start;
				// t3 = 0;
			}

			cout << "trained size: " << trained.size() << endl;

			cout << "the " << iter << " iteration :" << endl;
			//cout << " #the sampling rate: " << randpro << endl;
			cout << fixed << setprecision(15) << " #the sampling rate: " << randpro << endl;
			//cout << "stepbudget: " << stepbudget << endl;
			cout << "#data_transfer_time: " << data_transfer_time << ";#data_transfer_cost: " << data_transfer_cost << endl;
			cout << "#mvcost: " << current_mvcost << " ;#monetary cost: " << current_mvcost + mvpercents * data_transfer_cost << endl;

			outfile << data_transfer_time << '\t' << data_transfer_cost << '\t' << trained.size() << '\t';

			if (iter == 0)
				outfile << endl;

			if (iter % 4 == 0)
			{
				betaC = (iter / 4) * 0.25;
				alphaT = 1 - betaC;
			}
		}

		/**********************************Score And Objective Function**********************************/

		int low = 0;
		int high = eachThreadsNum[0] - 1;
		int status = 0;
		high_resolution_clock::time_point beginTime = high_resolution_clock::now();
		start = time(NULL);
		for (int j = 0; j < MAX_THREADS_NUM; j++)
		{
			Pthread_args *args = new Pthread_args();
			args->setId(j);
			args->setLow(low);
			args->setHigh(high);
			args->setIter(iter);
			status = pthread_create(&tidd[j], NULL, Parallel_Score_Signal_function, (void *)args);
			low = high + 1;
			if (j < MAX_THREADS_NUM - 1)
				high = low + eachThreadsNum[j + 1] - 1;
		}

		for (int j = 0; j < MAX_THREADS_NUM; j++)
			pthread_join(tidd[j], NULL);
		high_resolution_clock::time_point endTime = high_resolution_clock::now();
		milliseconds timeInterval = std::chrono::duration_cast<milliseconds>(endTime - beginTime);
		t5 = timeInterval.count();

		/**********************************Probability Update**********************************/
		low = 0;
		high = eachThreadsNum[0] - 1;
		status = 0;
		start = time(NULL);
		beginTime = high_resolution_clock::now();
		for (int j = 0; j < MAX_THREADS_NUM; j++)
		{
			Pthread_args *args = new Pthread_args();
			args->setLow(low);
			args->setHigh(high);
			args->setIter(iter);
			status = pthread_create(&tid[j], NULL, probability_Update, (void *)args);
			low = high + 1;
			if (j < MAX_THREADS_NUM - 1)
				high = low + eachThreadsNum[j + 1] - 1;
		}
		for (int j = 0; j < MAX_THREADS_NUM; j++)
			pthread_join(tid[j], NULL);
		endTime = high_resolution_clock::now();
		timeInterval = std::chrono::duration_cast<milliseconds>(endTime - beginTime);
		t4 = timeInterval.count();

		if (iter >= 1)
		{
			cout << "Action Select: " << t1 << endl;
			cout << "Vertex Move: " << t2 << endl;
			cout << "Sampling: " << t3 << endl;
			cout << "Probability Update: " << t4 << endl;
			cout << "Score Function: " << t5 << endl;
			double T = t1 + t2 + t3 + t4 + t5;
			overhead += T;

			outfile << TOEI[iter - 1] << '\t' << randpro << endl;
			cout << "******************************************************************" << endl;
			cout << "The overhead is: " << TOEI[iter - 1] << endl;
			cout << "******************************************************************" << endl;
		}

		iter++;
	}
	overhead = 0;

	outfile << data_transfer_cost << '\t' << current_mvcost << '\t' << current_mvcost + mvpercents * data_transfer_cost << "\t" << overhead << endl;

	// outfile << data_transfer_cost<< '\t' << current_mvcost << '\t'  << current_mvcost + mvpercents * data_transfer_cost;

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

	trained.clear();
	vector<my_mirror_change> mirror_change;

	// reloading edge.
	for (auto &x : not_load_edge)
	{
		int s = graph->mapped[x->getsourceID()];
		int d = graph->mapped[x->getdestID()];

		if (1. * rand() / RAND_MAX >= ignore_edge_rate)
			continue;

		retrain_node.insert(s);
		retrain_node.insert(d);

		if (v[d].getIngoingEdges().size() == theta - 1)
		{
			for (auto &in_edge : v[d].getIngoingEdges())
			{

				int mirror_label = v[graph->mapped[in_edge->sourceID]].label;

				mirror_change.push_back({v[d].label, d, -1, 0, v[d].label});
				mirror_change.push_back({v[d].label, (int)graph->mapped[in_edge->sourceID], 0, -1, mirror_label});

				mirror_change.push_back({mirror_label, d, 1, 0, v[d].label});
				mirror_change.push_back({mirror_label, (int)graph->mapped[in_edge->sourceID], 0, 1, mirror_label});
			}
			v[d].setType(0);
		}

		v[s].addOutgoingEdge(x);
		v[d].addIngoingEdge(x);

		if (v[d].getType())
		{
			mirror_change.push_back({v[d].label, d, 1, 0, v[d].label});
			mirror_change.push_back({v[d].label, s, 0, 1, v[s].label});
		}
		else
		{
			mirror_change.push_back({v[s].label, d, 1, 0, v[d].label});
			mirror_change.push_back({v[s].label, s, 0, 1, v[s].label});
		}
	}
	atomic_uploadG = vector<atomic_int>(algorithm->getDCNums());
	atomic_uploadA = vector<atomic_int>(algorithm->getDCNums());
	atomic_downloadG = vector<atomic_int>(algorithm->getDCNums());
	atomic_downloadA = vector<atomic_int>(algorithm->getDCNums());
	cout << "Begin loading edges..." << endl;
	my_move_mirror(mirror_change);
	cout << "Finished." << endl;
	for (int i = 0; i < algorithm->getDCNums(); i++)
	{
		cout << atomic_uploadA[i]
			 << " " << atomic_uploadG[i]
			 << " " << atomic_downloadA[i]
			 << " " << atomic_downloadG[i] << endl;
	}
	for (int i = 0; i < algorithm->getDCNums(); i++)
	{
		ua[i] += atomic_uploadA[i] * dataunit;
		ug[i] += atomic_uploadG[i] * dataunit;
		da[i] += atomic_downloadA[i] * dataunit;
		dg[i] += atomic_downloadG[i] * dataunit;
	}
	for (int j = 0; j < algorithm->DCnums; j++)
	{
		double divide = ug[j] / Upload[j];
		double divided = dg[j] / Download[j];
		maxArray[j] = divide > divided ? divide : divided;

		double divide2 = ua[j] / Upload[j];
		double divided2 = da[j] / Download[j];
		maxArray2[j] = divide2 > divided2 ? divide2 : divided2;

		uploadnumsum[j] = ug[j] + ua[j];
	}

	double time_ = max_value(maxArray) + max_value(maxArray2);

	double cost = sumWeights(uploadnumsum, Upprice);
	data_transfer_time = time_;
	data_transfer_cost = cost;
	cout << "**********************************************************************" << endl;
	cout << "#data_transfer_time: " << data_transfer_time << ";#data_transfer_cost: " << data_transfer_cost << endl;
	cout << "#mvcost: " << current_mvcost << " ;#totalcost: " << current_mvcost + mvpercents * data_transfer_cost << endl;

	cout << "**********************************************************************" << endl;
	for (int i = 0; i < algorithm->getDCNums(); i++)
		printf("DC %d --- node : %d\tuG : %.16f\tuA : %.16f\tdG : %.16f\tdA : %.16f\n", i, dc[i], ug[i], ua[i], dg[i], da[i]);

	// retrain
	iter = 0;
	// init
	for (auto &x : retrain_node)
	{
		v[x].iniLabel = v[x].label;
		for (int i = 0; i < algorithm->getDCNums(); i++)
			algorithm->UCB_value[x][i] = 1e50, algorithm->probability[x][i] = (double)1 / algorithm->getDCNums(), algorithm->sum_score[x][i] = 0, algorithm->select_time[x][i] = 1, algorithm->choice_T[x] = 0;
	}
	flag = 0;
	timeOld = 0;
	randpro = 0.01;
	global_mvcost = 0;
	overhead = 0;
	int max_price_dc = max_value_index(Upprice, algorithm->DCnums);

	// recompute budget
	for (auto &x : retrain_node)
	{
		if (v[x].label != max_price_dc)
			global_mvcost += raw_data_size * Upprice[v[x].label];
	}
	cout << retrain_node.size() << " " << global_mvcost << " " << budget_rate << endl;
	budget = global_mvcost * budget_rate + mvpercents * ginger_cost;
	current_mvcost = 0;

	calculateTimeAndPrice(data_transfer_time, data_transfer_cost);
	cout << "initializaion finished." << endl;
	cout << "The budget is " << budget << endl;
	cout << "batchsize: " << batchsize << endl;
	cout << "before executing the algorithm:" << endl;
	cout << "#data_transfer_time: " << data_transfer_time << endl;
	cout << "#data_transfer_cost: " << data_transfer_cost << endl;
	cout << "The seed is: " << Seed << endl;
	while (iter <= MAX_ITER)
	{
		for (int i = 0; i < algorithm->DCnums; i++)
		{
			uploadnumG[i] = ug[i];
			downloadnumG[i] = dg[i];
			uploadnumA[i] = ua[i];
			downloadnumA[i] = da[i];
			maxArray[i] = 0;
		}

		time_t start = time(NULL);

		if (iter > 0)
		{
			int low1 = 0;
			int high1 = eachThreadsNum[0] - 1;
			int status1 = 0;
			start = time(NULL);
			high_resolution_clock::time_point beginTime = high_resolution_clock::now();

			for (int j = 0; j < MAX_THREADS_NUM; j++)
			{
				Pthread_args *args = new Pthread_args();
				args->setLow(low1);
				args->setHigh(high1);
				args->setIter(iter);
				status1 = pthread_create(&tid[j], NULL, MyActionSelect, (void *)args);
				low1 = high1 + 1;
				if (j < MAX_THREADS_NUM - 1)
					high1 = low1 + eachThreadsNum[j + 1] - 1;
			}

			for (int j = 0; j < MAX_THREADS_NUM; j++)
				pthread_join(tid[j], NULL);

			high_resolution_clock::time_point endTime = high_resolution_clock::now();
			milliseconds timeInterval = std::chrono::duration_cast<milliseconds>(endTime - beginTime);
			t1 = timeInterval.count();
			timeOld = data_transfer_time;
			priceOld = data_transfer_cost;
			if (bsp == 0)
			{
				int percent = 0;
				int decidedtomove = 0;
				start = time(NULL);
				unordered_set<int>::iterator itora = trained.begin();
				while (itora != trained.end())
				{
					int i = *itora;
					int ori = v[i].label;
					if (v[i].label != v[i].action)
					{
						decidedtomove++;
						vertexMigrate(i, downloadnumG, uploadnumG, downloadnumA, uploadnumA, v[i].action);

						/************************Gather Time***********************************/

						for (int j = 0; j < algorithm->DCnums; j++)
						{
							double divide = uploadnumG[j] / Upload[j];
							double divided = downloadnumG[j] / Download[j];
							maxArray[j] = divide > divided ? divide : divided;
						}

						double time = max_value(maxArray);

						/************************Apply Time***********************************/
						for (int j = 0; j < algorithm->DCnums; j++)
						{
							double divide = uploadnumA[j] / Upload[j];
							double divided = downloadnumA[j] / Download[j];
							maxArray[j] = divide > divided ? divide : divided;
						}
						time += max_value(maxArray);

						for (int j = 0; j < algorithm->DCnums; j++)
						{
							uploadnumsum[j] = uploadnumG[j] + uploadnumA[j];
						}
						double cost = sumWeights(uploadnumsum, Upprice);

						if (time <= data_transfer_time)
						{
							data_transfer_time = time;
							data_transfer_cost = cost;
						}
						else
						{
							percent++;
							vertexMigrate(i, downloadnumG, uploadnumG, downloadnumA, uploadnumA, ori);
						}
					}
					itora++;
				}

				for (int i = 0; i < algorithm->DCnums; i++)
				{
					ug[i] = uploadnumG[i];
					dg[i] = downloadnumG[i];
					ua[i] = uploadnumA[i];
					da[i] = downloadnumA[i];
				}

				t2 = time(NULL) - start;
			}
			else if (bsp == 1)
			{
				for (int j = 0; j < batchsize; j++)
				{
					Pthread_args *args = new Pthread_args();
					args->id = j;
					status1 = pthread_create(&tid_bsp[j], NULL, my_moveVertexBsp, (void *)args);
				}
				unordered_set<int>::iterator itora = trained.begin();
				int *verID = new int[batchsize];
				double **ulg = new double *[batchsize];
				double **dlg = new double *[batchsize];
				double **ula = new double *[batchsize];
				double **dla = new double *[batchsize];
				for (int i = 0; i < batchsize; i++)
				{
					ulg[i] = new double[algorithm->DCnums];
					dlg[i] = new double[algorithm->DCnums];
					ula[i] = new double[algorithm->DCnums];
					dla[i] = new double[algorithm->DCnums];
				}
				vector<int> my_trained_2, my_trained;
				int t = 10;
				while (itora != trained.end())
				{
					if (v[*itora].label != v[*itora].action)
					{
						my_trained_2.push_back(*itora);
					}
					itora++;
				}
				cout << endl;

				int size = my_trained_2.size();
				for (int i = 0; i < size / batchsize; i++)
					for (int j = i; j < my_trained_2.size(); j += size / batchsize)
						my_trained.push_back(my_trained_2[j]);

				start = time(NULL);
				high_resolution_clock::time_point beginTime = high_resolution_clock::now();
				double o = 0;
				time_t s = time(NULL);
				int vertype = 0;
				int train_cnt = 0;

				while (train_cnt < my_trained.size())
				{
					int numver = 0;
					sem_init(&ready, 0, 0);
					atomic_uploadG = vector<atomic_int>(algorithm->getDCNums());
					atomic_uploadA = vector<atomic_int>(algorithm->getDCNums());
					atomic_downloadG = vector<atomic_int>(algorithm->getDCNums());
					atomic_downloadA = vector<atomic_int>(algorithm->getDCNums());
					while (train_cnt < my_trained.size() && numver < batchsize)
					{

						verID[numver] = my_trained[train_cnt];
						numver++;

						train_cnt++;
					}

					for (int i = 0; i < numver; i++)
					{
						for (int j = 0; j < algorithm->DCnums; j++)
						{
							ulg[i][j] = ug[j];
							dlg[i][j] = dg[j];
							ula[i][j] = ua[j];
							dla[i][j] = da[j];
						}
					}

					s = time(NULL);
					high_resolution_clock::time_point parallel_begin = high_resolution_clock::now();

					for (int j = 0; j < numver; j++)
					{
						// cout<<"j: "<<j<<endl;
						Pthread_args args = Pthread_args();
						args.id = j;
						args.iteration = iter;
						args.verindex = j;
						args.ver = verID[j];
						args.downloadnumG = dlg[j];
						args.uploadnumG = ulg[j];
						args.downloadnumA = dla[j];
						args.uploadnumA = ula[j];
						args.destdc = v[verID[j]].action;
						bspvec[j].push_back(args);
					}

					while (ready_thread != numver)
						;
					ready_thread = 0;

					for (int i = 0; i < numver; i++)
						sem_post(&ready);

					while (bsplock != numver)
					{
					}
					bsplock = 0;
					for (int i = 0; i < algorithm->getDCNums(); i++)
					{
						ua[i] += atomic_uploadA[i] * dataunit;
						ug[i] += atomic_uploadG[i] * dataunit;
						da[i] += atomic_downloadA[i] * dataunit;
						dg[i] += atomic_downloadG[i] * dataunit;
					}

					o += time(NULL) - s;

					for (int j = 0; j < algorithm->DCnums; j++)
					{
						double divide = ug[j] / Upload[j];
						double divided = dg[j] / Download[j];
						maxArray[j] = divide > divided ? divide : divided;

						double divide2 = ua[j] / Upload[j];
						double divided2 = da[j] / Download[j];
						maxArray2[j] = divide2 > divided2 ? divide2 : divided2;

						uploadnumsum[j] = ug[j] + ua[j];
					}

					double time = max_value(maxArray) + max_value(maxArray2);

					/************************Apply Time***********************************/

					double cost = sumWeights(uploadnumsum, Upprice);
					data_transfer_time = time;
					data_transfer_cost = cost;
				}

				for (int j = 0; j < batchsize; j++)
					pthread_cancel(tid_bsp[j]);

				for (int j = 0; j < batchsize; j++)
					pthread_join(tid_bsp[j], NULL);

				high_resolution_clock::time_point endTime = high_resolution_clock::now();
				milliseconds timeInterval = std::chrono::duration_cast<milliseconds>(endTime - beginTime);
				t2 = timeInterval.count();

				delete[] verID;
				for (int i = 0; i < batchsize; i++)
				{
					delete[] ulg[i];
					delete[] dlg[i];
					delete[] ula[i];
					delete[] dla[i];
				}
				delete[] ulg;
				delete[] dlg;
				delete[] ula;
				delete[] dla;
			}

			TOEI[iter - 1] = (t1 + t2 + t3 + t4 + t5) / 1000;
			SR[iter - 1] = randpro;
			double usedtime = 0;
			for (int p = 0; p < iter; p++)
				usedtime += TOEI[p];
			double lefteach = (L - usedtime) / (MAX_ITER - iter);
			int recent = 3;
			if (lefteach <= 0)
				randpro = 0;
			else
			{
				if (iter >= recent)
				{
					double total_sr = 0;
					for (int p = iter - 1; p >= iter - recent; p--)
						total_sr += lefteach / TOEI[p] * SR[p];
					randpro = total_sr / recent;
				}
				else
				{
					double total_sr = 0;
					for (int p = 0; p <= iter - 1; p++)
						total_sr += lefteach / TOEI[p] * SR[p];
					randpro = total_sr / recent;
				}
			}
			if (randpro > 1)
				randpro = 1;
		}

		{
			trained.clear();

			int low1 = 0;
			int high1 = eachThreadsNum[0] - 1;
			int status1 = 0;
			start = time(NULL);
			high_resolution_clock::time_point beginTime = high_resolution_clock::now();
			for (int j = 0; j < MAX_THREADS_NUM; j++)
			{
				Pthread_args *args = new Pthread_args();
				args->setLow(low1);
				args->setHigh(high1);
				args->setIter(iter);
				status1 = pthread_create(&tid[j], NULL, retrain_Sampling, (void *)args);
				low1 = high1 + 1;
				if (j < MAX_THREADS_NUM - 1)
					high1 = low1 + eachThreadsNum[j + 1] - 1;
			}

			for (int j = 0; j < MAX_THREADS_NUM; j++)
				pthread_join(tid[j], NULL);

			high_resolution_clock::time_point endTime = high_resolution_clock::now();
			milliseconds timeInterval = std::chrono::duration_cast<milliseconds>(endTime - beginTime);
			t3 = timeInterval.count();
			//t3=time(NULL)-start;
			t3 = 0;
		}

		cout << "trained size: " << trained.size() << endl;

		cout << "the " << iter << " iteration :" << endl;
		cout << fixed << setprecision(15) << " #the sampling rate: " << randpro << endl;
		cout << "#data_transfer_time: " << data_transfer_time << ";#data_transfer_cost: " << data_transfer_cost << endl;
		cout << "#mvcost: " << current_mvcost << " ;#totalcost: " << current_mvcost + mvpercents * data_transfer_cost << endl;

		outfile << data_transfer_time << '\t' << data_transfer_cost << '\t' << 1. * trained.size() / graph->getNum_Nodes() << '\t';
		if (iter == 0)
			outfile << endl;

		if (iter % 4 == 0)
		{
			betaC = (iter / 4) * 0.25;
			alphaT = 1 - betaC;
		}
		int low = 0;
		int high = eachThreadsNum[0] - 1;
		int status = 0;
		high_resolution_clock::time_point beginTime = high_resolution_clock::now();
		start = time(NULL);
		for (int j = 0; j < MAX_THREADS_NUM; j++)
		{
			//cout<<"low: "<<low<<", high: "<<high<<endl;
			Pthread_args *args = new Pthread_args();
			args->setId(j);
			args->setLow(low);
			args->setHigh(high);
			args->setIter(iter);
			status = pthread_create(&tidd[j], NULL, Parallel_Score_Signal_function, (void *)args);
			low = high + 1;
			if (j < MAX_THREADS_NUM - 1)
				high = low + eachThreadsNum[j + 1] - 1;
		}
		//cout<<"Main Thread finished!"<<endl;
		for (int j = 0; j < MAX_THREADS_NUM; j++)
			pthread_join(tidd[j], NULL);
		high_resolution_clock::time_point endTime = high_resolution_clock::now();
		milliseconds timeInterval = std::chrono::duration_cast<milliseconds>(endTime - beginTime);
		t5 = timeInterval.count();
		low = 0;
		high = eachThreadsNum[0] - 1;
		status = 0;
		start = time(NULL);
		beginTime = high_resolution_clock::now();
		for (int j = 0; j < MAX_THREADS_NUM; j++)
		{
			Pthread_args *args = new Pthread_args();
			args->setLow(low);
			args->setHigh(high);
			args->setIter(iter);
			status = pthread_create(&tid[j], NULL, probability_Update, (void *)args);
			low = high + 1;
			if (j < MAX_THREADS_NUM - 1)
				high = low + eachThreadsNum[j + 1] - 1;
		}
		for (int j = 0; j < MAX_THREADS_NUM; j++)
			pthread_join(tid[j], NULL);
		endTime = high_resolution_clock::now();
		timeInterval = std::chrono::duration_cast<milliseconds>(endTime - beginTime);
		t4 = timeInterval.count();

		if (iter >= 1)
		{
			cout << "Action Select: " << t1 << endl;
			cout << "Vertex Move: " << t2 << endl;
			cout << "Sampling: " << t3 << endl;
			cout << "Probability Update: " << t4 << endl;
			cout << "Score Function: " << t5 << endl;
			double T = t1 + t2 + t3 + t4 + t5;
			overhead += T;
			outfile << TOEI[iter - 1] << '\t' << randpro << endl;
			cout << "******************************************************************" << endl;
			cout << "The overhead is: " << TOEI[iter - 1] << endl;
			cout << "******************************************************************" << endl;
		}
		iter++;

		outtrain = ofstream(FN);
		for (int i = 0; i < graph->getNum_Nodes(); i++)
			outtrain << i << " " << v[i].label << endl;
		outtrain.close();
	}
	overhead = 0;
	for (int p = 0; p < MAX_ITER; p++)
		overhead += TOEI[p];

	outfile << data_transfer_cost << '\t' << current_mvcost << '\t' << current_mvcost + mvpercents * data_transfer_cost << "\t" << overhead << endl;

	cout << "**********************************************************************" << endl;
	cout << "#data_transfer_time: " << data_transfer_time << ";#data_transfer_cost: " << data_transfer_cost << endl;
	cout << "#mvcost: " << current_mvcost << " ;#totalcost: " << current_mvcost + mvpercents * data_transfer_cost << endl;

	cout << "**********************************************************************" << endl;
	for (int i = 0; i < algorithm->getDCNums(); i++)
		printf("DC %d --- node : %d\tuG : %.16f\tuA : %.16f\tdG : %.16f\tdA : %.16f\n", i, dc[i], ug[i], ua[i], dg[i], da[i]);

	overhead = 0;
	for (int p = 0; p < MAX_ITER; p++)
		overhead += TOEI[p];
	//outfile.close();
	outtrain.close();
	delete[] eachThreadsNum;
	delete[] uploadnumA;
	delete[] uploadnumG;
	delete[] downloadnumA;
	delete[] downloadnumG;
	delete[] maxArray;
	delete[] maxArray2;
	delete[] uploadnumsum;
}
