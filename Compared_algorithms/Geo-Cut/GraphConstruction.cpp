#include "Distributed_graph.h"
#include <sstream>
#include <fstream>
#include <iostream>
#include <string>
#include <string.h>

// [vertex_id1] [vertex_id2]
// NOTE: vertex id should start from 1.
extern int numofdcs;
extern ofstream fileResult;
bool distributed_graph::line_parser(const std::string& filename,const std::string& textline, int &ecount) {
	std::stringstream strm(textline);
	size_t source = 0;
	size_t target = 0;
	float weight = 0.0;
	strm >> source;
	strm.ignore(1);
	strm >> target;
	if (source != target){
		//需要加入random_edges
		(this->g)->random_edges.push_back(make_pair(source, target));
		(this->g)->myvertex[target]->in_neighbour.push_back(source);
		(this->g)->myvertex[target]->inSize = (this->g)->myvertex[target]->in_neighbour.size();
		(this->g)->myvertex[source]->out_neighbour.push_back(target);
		(this->g)->myvertex[source]->outSize = (this->g)->myvertex[source]->out_neighbour.size();
		//std::pair<Edge, bool> cur_edge = add_edge(source, target, *(this->g));
		//random_access_edges.emplace(std::make_pair(ecount,cur_edge.first));   // 这个是用来干什么的呢？
	}
	else
	 	ecount--;
	return true;
}
void distributed_graph::load_from_file(std::string filename, int vertices, int edges){
	this->g = new Graph();
	//load vertices	
	std::ifstream graph_file(filename);
	std::string line;
	this->num_vertices = vertices;
	this->num_edges = edges;
	std::vector<float> rndlocs;
	std::ifstream rndfile("rndlog");
	std::string rndline;	
	for(int i=0; i<1000000; i++){
		std::getline(rndfile,rndline);
		std::stringstream strm(rndline);
		float loc;
		strm >> loc;
		rndlocs.push_back(loc);
	}
	//add vertices
	/*for(int i=0; i<num_vertices; i++){
		MyVertex* v = new MyVertex();
		v->vertex_id = i;  			//点的id  --shenbk
		//////////////////
		// v->data->input_size = 10;
		//////////////////
		int loc = std::floor(rndlocs[i%1000000]*(float)numofdcs);
		v->location_id = (Amazon_EC2_regions)loc;
		// v->data->routed_nodes.push_back(v->vertex_id);
		add_vertex(*v, *this->g);    //没有找到add_vertex函数， 属于boost里面的。需要重新写  --by  shenbk
	}*/
	this->g->GraphInit(num_vertices);
	int label = 0;  // hash partition
	int num = num_vertices / numofdcs;  // hash partition
	cout << "Use the hash initial graph partitioning." << endl;
	for (int i = 0; i < num_vertices; ++i)
	{
		g->myvertex[i]->vertex_id = i;
		//int loc = std::floor(rndlocs[i%1000000]*(float)numofdcs);  
		if (i%num == 0&&label< numofdcs -1&&i!=0)   // hash partition
			label++;
		g->myvertex[i]->location_id = (Amazon_EC2_regions)label;

	}
	//add edges
	std::getline(graph_file, line);
	int ecount = 0;
	while (!graph_file.eof()){
		line_parser(filename, line, ecount);  //函数中调用boost的add_edges函数  --by  shenbk
		std::getline(graph_file, line);
		ecount ++;
		//std::cout<<ecount<<" edges inserted\n";	
		if (ecount % 1000000 == 0)
		{
			std::cout << ecount << " edges inserted\n";
			fileResult << ecount << " edges inserted\n";
		}
	}
	graph_file.close();
	//printf("should have %d edges, added %d edges\n",edges,ecount);
	fileResult << "should have " << edges << " edges, added " << ecount << " edges\n";
	this->num_edges = ecount;
	//std::cout << "load graph from file done.\n";
	fileResult << "load graph from file done.\n" ;
}

void distributed_graph::load_synthetic_file(const char* filename) {
	//load live journal graph from files
	if (strcmp(filename, "powerlaw-10000-2.1") == 0) {
		this->num_vertices = 10000;
	}else if (strcmp(filename, "powerlaw-100-2.1") == 0) {
		this->num_vertices = 100;
	}
	else if (strcmp(filename, "powerlaw") == 0) {
		this->num_vertices = 100;
	}
	else if (strcmp(filename, "powerlaw-50000-2.1") == 0) {
		this->num_vertices = 50000;
	}
	else if (strcmp(filename, "powerlaw-100000-2.1") == 0) {
		this->num_vertices = 100000;
	}
	else if (strcmp(filename, "powerlaw-1000000-2.1") == 0) {
		this->num_vertices = 1000000;
	}
	else {
		//printf("please select from 10000, 50000 and 100000 vertices.\n");
		fileResult << "please select from 10000, 50000 and 100000 vertices.\n";
		exit(1);
	}
	this->load_from_file(filename, this->num_vertices, 0);
}



void distributed_graph::load_twitter_graph(){
	//std::string filename = "twitter_rv.net";
	//int v = 61578415; //41652230; //81306; 
	//int e = 1468364885;//1468365182; //1768149;
	//std::string filename = "Twitter-dataset/data/twitter_rv.net";
	std::string filename = "../../ruibo/geo_Pregel/twitter/twitter_rv.net";
	int v = 61578415;
	int e = 1468365182;
	this->load_from_file(filename,v,e);
}
void distributed_graph::load_orkut_graph() {
	//std::string filename = "twitter_rv.net";
	//int v = 61578415; //41652230; //81306; 
	//int e = 1468364885;//1468365182; //1768149;
	//std::string filename = "Twitter-dataset/data/twitter_rv.net";
	std::string filename = "../../ruibo/geo_Pregel/orkut/com-orkut.ungraph.txt";
	int v = 3072441;
	int e = 117185083;
	this->load_from_file(filename, v, e);
}
void distributed_graph::load_uk_graph() {
	//std::string filename = "twitter_rv.net";
	//int v = 61578415; //41652230; //81306; 
	//int e = 1468364885;//1468365182; //1768149;
	//std::string filename = "Twitter-dataset/data/twitter_rv.net";
	std::string filename = "../../ruibo/geo_Pregel/uk/uk-2005.mtx";
	int v = 39454746;
	int e = 936364282;
	this->load_from_file(filename, v, e);
}
void distributed_graph::load_it_graph() {
	//std::string filename = "twitter_rv.net";
	//int v = 61578415; //41652230; //81306; 
	//int e = 1468364885;//1468365182; //1768149;
	//std::string filename = "Twitter-dataset/data/twitter_rv.net";
	std::string filename = "../../ruibo/geo_Pregel/it/web-it-2004-all.mtx";
	int v = 41290682;
	int e = 1150725436;
	this->load_from_file(filename, v, e);
}
void distributed_graph::load_live_journal_graph(){
	std::string filename = "../../ruibo/geo_Pregel/livejournal/soc-LiveJournal1.txt";
	int v = 4847571;
	long int e = 68993773;
	this->load_from_file(filename,v,e);
}
void distributed_graph::load_googleweb_graph(){
        std::string filename = "../../ruibo/geo_Pregel/googleweb/web-Google.txt";
		//std::string filename = "./web-Google.txt";
        int v = 875713; //875713;
        int e = 5105039;
        this->load_from_file(filename,v,e);
}

void distributed_graph::load_facebook_graph(){
        std::string filename = "../../graph/facebook_combined.txt";
        int v = 4039; 
        int e = 88234;
        this->load_from_file(filename,v,e);
}

void distributed_graph::load_wiki_graph(){
        std::string filename = "../../graph/Wiki-Vote.txt";
        int v = 8298; //7115;
        int e = 103689;
        this->load_from_file(filename,v,e);
}

void distributed_graph::load_roadnet_graph(){
        std::string filename = "../../graph/weight-roadNet-CA.txt";
        int v = 1971281; //1965206;
        int e = 5533214;
        this->load_from_file(filename,v,e);
}
void distributed_graph::load_p2p_graph(){
        std::string filename = "../../graph/p2p-Gnutella09.txt";
        int v = 8114; //8846;
        int e = 26013; //31839;
        this->load_from_file(filename,v,e);
}
