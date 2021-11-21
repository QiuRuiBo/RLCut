#ifndef LIB_H
#define LIB_H
#include<iostream>
#include <unordered_set>
#include <unordered_map>
#include<map>
#include<vector>
#include"libgraph.h"
#include <queue>

extern double alphaT;
extern double betaC;
extern Graph* graph;
extern double* ug;
extern double* dg;
extern double* ua;
extern double* da;
extern long mvpercents;
extern char* FN;
extern double ginger_cost;
extern double global_mvcost;
extern double current_mvcost;
extern double L;
extern int optitime;
extern int F1;
extern int F2;
extern double t1;
extern double t3;
extern double t4;
extern double t5;  //score时间
extern double t2;   //vertex move时间
extern double t6;
extern double moverand;
extern ofstream outfile;
extern float raw_data_size;
extern vector<Pthread_args> *bspvec;
extern unordered_map<int,vector<int>>PSS;
extern double dataunit;
extern int bsp;
// extern int bsplock;
extern int batchsize;
extern Algorithm* algorithm;
extern Network* network;
extern int MAX_ITER;
extern int MAX_THREADS_NUM;
extern double data_transfer_time;
extern double data_transfer_cost;
extern double timeOld ;
extern double priceOld ;
extern int theta;
extern char* outputfile;
extern double budget;
extern double stepbudget;
extern double addbudget;
extern double randpro;
extern double overhead;
extern int Seed;
extern unordered_set<int>trained;
extern int* optimize;
using namespace std;
void Simulate();
void* test(void* arguments);
void Mirrortest();
void reCalTimeAndCost(double&,double&);
void powerlyrareCalTimeAndCost(double&,double&);
void powerlyrareCalTimeAndCost(vector<double> &Uptime, vector<double> &Downtime, vector<double> &UptimeS, vector<double> &DowntimeS);
void* Parallel_Score_Migration_function(void* arguments);
void* keepMirrorsStable(void* arguments);
void* keepMirrorsBackUpStable(void* arguments);
void* Parallel_Score_Signal_function(void* arguments);
char* input_format_specifier(char *,char*);
int read_input_file(char*,bool);
int read_input_network(char*,double*upload,double*download,double* upprice);
void initialNode(FILE* , char*);
void initialNode(FILE* , char* , double);
void initialEdge(FILE*, char*,bool);
void initialEdge(FILE*, char*,double,bool);
void initAlgorithm(char*,int);
void moveVertex(int ver,double* downloadnumG,double* uploadnumG,double* downloadnumA,double* uploadnumA,int destdc);
void* moveVertexBsp(void* arguments);
void* my_moveVertexBsp(void* arguments);
void *my_assign_dc(void*);
void vertexMigrate(int ver,double* downloadnumG,double* uploadnumG,double* downloadnumA,double* uploadnumA,int destdc);
void RLcut();
void ActionSelect();
void* MyActionSelect(void*);
void* Sampling(void*);
void Score_Migration_function(double*,double*,int,double*,double*);
void calculateTimeAndPrice(double&,double&);
void initPartitioning();
void* probability_Update(void*);
void update_Download_Upload(int);
void allocBudget();
int trimmer_tool(double*,int);
void trim_probability(double**,int**,int*,double);
void weighted_probability_update(int);
void multiprocessing_pool(int*);
double Benifit(int,int);
void objective_function(double*,int);
extern int High_degree_node_size;
extern unordered_set<int> k_degree_trained_candidates;
extern int k_degree;
extern pthread_mutex_t **mirror_mutex;
extern double c;
extern double train_edge_rate;
void dfs(vector<double> &Uptime, vector<double> &Downtime, vector<double> &UptimeS, vector<double> &DowntimeS, vector<bool> &tag, int index, int max_depth, double time_, double time_s, double price_);
class save;
void print_pq(int );
extern priority_queue<save> pq;
extern double my_sp_rate;
#endif
