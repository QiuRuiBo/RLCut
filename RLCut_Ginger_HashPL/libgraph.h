#ifndef LIBGRAPH_H
#define LIBGRAPH_H
#include<list>
using namespace std;
class Message
{
private:
	int source;
	int dest;
	double value;
public:
	Message(int source, int dest, double msgValue);
};
class Partition
{
private:
	list<Message> megs;
	double UploadNum;
	double DownloadNum;
	double budget;
public:
	void recMsg(Message msg);
	void setUploadNum(double uploadnum);
	double getUploadNum();
	void setDownloadNum(double uploadnum);
	double getDownloadNum();
	void setBudget(double budget);
	double getBudget();

};
class Edge
{
public:
	int sourceID;
	int destID;
	double value;
public:
	Edge(int source, int dest, double value);
	int getsourceID();
	void setsourceID(int source);
	int getdestID();
	void setdestID(int dest);
	double getValue();
	void setValue(double value);
};
class Vertex
{
public:
	int vertexID;
	int iniLabel;
	int type;  //  0表示high-degree,1表示low-degree
	list<Message> msgs;
	list<Edge*> outgoingEdges;
	list<Edge*> ingoingEdges;
	Partition partition;
	double* signal_;
	bool active;
	int signal;
	int action;   
	double value;
	int label;
	int idegreeinlabel;
	int odegreeinlabel;
public:
    Vertex();
	~Vertex();
	int getVertexID();
	void setType(int type);
	int getType();
	void setVertexID(int vertexID);
	double getValue();
	void setValue(double value);
	int getLabel();
	void setLabel(int label);
	int getSignal();
	void setSignal(int signal);
	int getAction();
	void setAction(int action);
	void initSignal_();
    void resetSignal_();
	double* getSignal_();
	list<Edge*> getOutgoingEdges();
	list<Edge*> getIngoingEdges();
	void addOutgoingEdge(Edge* e);
	void addIngoingEdge(Edge* e);
	void sendMessage(int destination, double msgValue);
	void recMessage(Message mes);
	list<Message> getMessage();
	void clearMessage();
	void showVertex();
};
class Algorithm
{
public:
	char* name;
	double** probability;
	int DCnums;
	// thb
	double **UCB_value;			//上置信区间
	long long *choice_T;		//node总的决策次数
	double **sum_score;			//每个node每个DC的奖励总和
	long long **select_time;	//每个node每个DC的选择次数
public:
	void InitAlgorithm(char*,int);
	~Algorithm();
	int getDCNums();
	void InitLabel();
	void InitProbability();
	double** getProbability();
	
};
class Mirror
{
public: 
    int outD;
    int inD;
    int  id;
public:
    Mirror();
	void setID(int id);
	void setInD(int ind);
	void setOutD(int outd);
	int getID();
	void addOutD();
	void subOutD();
	void subInD();
	void addInD();
	void addInDMulti(int num);
	int getOutD();
	int getInD();
	void reset();
};
class Graph
{
public:
	long long num_edges;    //�����
	long long num_nodes;
	long long *mapped;
	//list<int> * mirrors;
	Vertex *nodes;
	Mirror** mirrors;

public:
	Graph();
	~Graph();
	void setMapped(long long*);
	long long* getMapped();
	void setNodes(Vertex*);
	void addNode();
	void addEdge();
	Vertex* getVertexs();
	long long getNum_Nodes();
	long long getNum_Edges();
	void showGraph();
	void initMirrors(int,int);
	Mirror** getMirrors();
	
};
class Application
{

};
class Network
{
public:
	double* upload;
	double* download;
	double* upprice;
	double* uploadnumG;
	double* downloadnumG;
	double* uploadnumA;
	double* downloadnumA;
	Partition* partitions;
public:
	Network();
	~Network();
	void initNetwork(double* upload,double* download,double* upprice);
	void setPartition(Partition*);
	Partition* getPartition();
	double* getUpload();
	double* getDownload();
	double* getUpprice();
};
class Pthread_args
{
public:
	int iteration;
	int id;
	int dc;
	unsigned int low;
	unsigned int high;
	int verindex;
	int ver;
	double* downloadnumG;
	double* uploadnumG;
	double* downloadnumA;
	double* uploadnumA;
	int destdc;
	int* s;
	// thb
	double **sum_score;
	long long **choice_time;
public:
	void setIter(int iter);
	int  getIter();
	int getId();
	void setId(int id);
	void setLow(int low);
	int  getLow();
	void setHigh(int high);
	int getHigh();
	void setDc(int dc);
	int getDc();
};
#endif
