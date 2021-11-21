#ifndef DISTRIBUTED_GRAPH_H
#define DISTRIBUTED_GRAPH_H

#include <vector>
#include <random>
#include <mutex>
#include <unordered_map>
//#include <boost/graph/adjacency_list.hpp>
//#include <boost/graph/graph_traits.hpp>
#include "Geography.h"
#include<map>
#include<vector>
#include <algorithm>
using  namespace std;

extern ofstream fileResult;


//using namespace boost;

#endif

enum Graph_type
{
	test,
	synthetic,
	livejournal,
	twitter,
	roadca
};

enum vertex_status{
	activated,
	deactivated
};

class VertexData{
public:
	/** size of raw input data in MB
	* for example, the tweets of a twitter user etc.
	*/
	float input_size;
	// float msg_size;
	// /*for pagerank application*/
	// float rank;
	// float last_change;
	// //double accum;
	// /*for sssp application*/	
	// float dist;
	// bool changed;
	// std::vector<int> routed_nodes;
	// /*for subgraph isomorphism*/
	// int v_pattern;
	
	// class Message{
		// public:
		// /** Possible matching vertices pairs 
		// * the 1st int: vertex id in the pattern 
		// * the 2nd int: vertex id in the data graph
		// */
		// std::vector<std::pair<int,int> > pairs;
		// std::vector<int> forwarding_trace;
	// };
	// std::vector<Message> matches;
	// std::vector<std::vector<int> > matched_subgraphs;
	
	virtual float size_of() { return 0; }
	VertexData(){
		input_size = 10; 
		// rank = 0.0; last_change = 0.0; 
		// dist = 0.0; changed = false; 
		// v_pattern = 0;
	}
	VertexData& operator=(const VertexData& other)  {
//		printf("base assignment called\n");
		this->input_size = other.input_size;
        return *this;
    }
};

class PageRankVertexData:public VertexData{
public:
	/*for pagerank application*/
	float rank;
	float last_change;
	PageRankVertexData(){
		input_size = 10; 
		rank = 0.0; last_change = 0.0; 
	}
	float size_of(){
		return 0.000008;
	}
	PageRankVertexData& operator=(const PageRankVertexData& other)  {
//		printf("pagerank assignment called\n");
		VertexData::operator= (other);
		this->rank = other.rank;
		this->last_change = other.last_change;
        return *this;
    }
};

class SSSPVertexData:public VertexData{
public:
	/*for sssp application*/	
	float dist;
	bool changed;
	std::vector<int> routed_nodes;
	SSSPVertexData(){
		input_size = 10; 
		dist = 0.0; changed = false; 
	}
	float size_of(){
		return 0.000008;
	}
	SSSPVertexData& operator=(const SSSPVertexData& other)  {
//		printf("sssp assignment called\n");
		VertexData::operator= (other);
		this->dist = other.dist;
		this->changed = other.changed;
		this->routed_nodes = other.routed_nodes;
        return *this;
    }
};

class SubgraphVertexData:public VertexData{
public:
		/*for subgraph isomorphism*/
	int v_pattern;
	
	struct Message{
		/** Possible matching vertices pairs 
		* the 1st int: vertex id in the pattern 
		* the 2nd int: vertex id in the data graph
		*/
		std::vector<std::pair<int,int> > pairs;
		std::vector<int> forwarding_trace;
	};
	std::vector<Message> matches;
	std::vector<std::vector<int> > matched_subgraphs;
	
	SubgraphVertexData(){
		input_size = 10; 
		v_pattern = 0;
	}
	float size_of(){
		cout << matches.size() << endl;
		return sizeof(Message)*matches.size();
		//return 1.0;
	}
	SubgraphVertexData& operator=(const SubgraphVertexData& other)  {
//		printf("subgraph assignment called\n");
		VertexData::operator= (other);
		this->v_pattern = other.v_pattern;
		this->matches = other.matches;
		this->matched_subgraphs = other.matched_subgraphs;
        return *this;
    }
};


class MyVertex{

public:
	/*identification*/
	int vertex_id;
	// char* username;
	/*init location*/
	Amazon_EC2_regions location_id;
	VertexData* data;
	VertexData* accum;

	//add by shenbk
	vector<int> in_neighbour;
	vector<int> out_neighbour;
	int inSize;
	int outSize;
	/*performance info*/
	// float size_in_data;
	// float size_out_data;
	// float time_computation;
	// float start_time;
	// float rest_time;
	// float finish_time;
	vertex_status status;
	vertex_status next_status;
	// float cost;

	/*replication*/
	std::vector<int> replica_location;
	// int expected_replicas;
	bool is_master;
	int master_location;
	int local_vid;

	/*vertex performance estimation*/
	//float perf_estimation();

	MyVertex(){
		master_location = -1;
		status = deactivated;
		next_status = deactivated;
	}
	
	/* std::vector<int> get_rep_location(){
		mtx.lock();
		std::vector<int> out = replica_location;
		mtx.unlock();
		return out;
	}
	
	void update_rep_location(std::vector<int> rep){
		mtx.lock();
		replica_location = rep;
		mtx.unlock();
	} */

};

class EdgeData{
public:
	int edge_id;
	int start_node_id;
	int end_node_id;
	float weight;
	Amazon_EC2_regions assigned_location;
};

//typedef adjacency_list<boost::vecS, boost::vecS, bidirectionalS, MyVertex, property<edge_weight_t, float> > Graph; //
class Graph{
public:
	//MyVertex *myvertex;
	vector< MyVertex * > myvertex;
	vector<pair<int, int>> random_edges;
public:

	void GraphInit(int i){
		//myvertex = new MyVertex[i];
		for (int num =0; num <i; num++)
		{
			MyVertex* v = new MyVertex();
			myvertex.push_back(v);
		}
	}
	int source_edge(int i ){
		return random_edges[i].first;
	}
	int target_edge(int i){
		return random_edges[i].second;
	}

	Graph(){
		random_edges.clear();
	}
};
/*typedef graph_traits<Graph>::vertex_descriptor Vertex;
typedef graph_traits<Graph>::vertex_iterator vertex_iter;
typedef graph_traits<Graph>::edge_iterator edge_iter;
typedef graph_traits<Graph>::out_edge_iterator out_edge_iterator;
typedef graph_traits<Graph>::in_edge_iterator in_edge_iterator;
typedef graph_traits<Graph>::edge_descriptor Edge;
typedef graph_traits<Graph>::adjacency_iterator adja_iterator;*/

class distributed_graph{
public:
	
	Graph_type graph_type;
	Graph* g;
	//property_map<Graph, edge_weight_t>::type WeightMap = get(edge_weight, *g);
	// implemented for random access to edges, which is not supported in boost::graph
	std::unordered_map<int,pair<int,int>> random_access_edges;
	
	int num_vertices;
	int num_edges;
	
	/*indicating at initilization stage
	* for SubgraphIsomorphism app only
	*/
	bool init_indicator = true; 
	int  get_in_nbrs_size(int v){
		return g->myvertex[v]->in_neighbour.size(); 
	}

	void  vectorSort() {
		for (int i = 0; i < num_vertices; i++) {
			sort(g->myvertex[i]->in_neighbour.begin(), g->myvertex[i]->in_neighbour.end());
			sort(g->myvertex[i]->out_neighbour.begin(), g->myvertex[i]->out_neighbour.end());
		}
	}
	std::vector<int> get_in_nbrs(int v){
		/*in_edge_iterator in_i, in_end;
		Edge e;
		std::vector<int> in_nbrs;
		boost::tie(in_i, in_end) = in_edges(v, *g);
		for (; in_i != in_end; ++in_i) {//if only one parent
			e = *in_i;
			Vertex src = source(e, *g);
			in_nbrs.push_back(src);
		}*/
		std::vector<int> in_brs;
		in_brs = g->myvertex[v]->in_neighbour;
		return in_brs;
	}

	int get_out_nbrs_size(int v){
		return g->myvertex[v]->out_neighbour.size();
	}

	std::vector<int> get_out_nbrs(int v){
		/*out_edge_iterator out_i, out_end;
		Edge e;
		std::vector<int> out_nbrs;
		boost::tie(out_i, out_end) = out_edges(v, *g);
		for (; out_i != out_end; ++out_i){
			e = *out_i;
			Vertex tgt = target(e, *g);
			out_nbrs.push_back(tgt);
		}*/
		std::vector<int> out_nbrs;
		out_nbrs = g->myvertex[v]->out_neighbour;
		return out_nbrs;
	}

	void pdf2cdf(std::vector<double>& pdf) {
		double Z = 0;
		for (size_t i = 0; i < pdf.size(); ++i) Z += pdf[i];
		for (size_t i = 0; i < pdf.size(); ++i)
			pdf[i] = pdf[i] / Z + ((i>0) ? pdf[i - 1] : 0);
	}

	double rnd_generator(double lower, double upper){
		std::random_device rd;
		std::mt19937 gen(rd());
		std::uniform_real_distribution<> dis(lower, upper);
		return dis(gen);
	}
	/**
	* Generate a draw from a multinomial using a CDF.  This is
	* slightly more efficient since normalization is not required
	* and a binary search can be used.
	*/
	size_t multinomial_cdf(const std::vector<double>& cdf) {
		double rnd = rnd_generator(0,1);
		return 1;
		//return std::upper_bound(cdf.begin(), cdf.end(),rnd) - cdf.begin();
	}
	/**
	*   the line parser returns true if the line is parsed successfully and
	*	calls graph.add_vertex(...) or graph.add_edge(...)
	*/
	bool line_parser(const std::string& filename,const std::string& linename, int &ecount);
	
	
	/**
	* \brief Constructs a synthetic power law graph.
	*
	* This function constructs a synthetic out-degree power law of "nverts"
	* vertices with a particular alpha parameter.
	* In other words, the probability that a vertex has out-degree \f$d\f$,
	* is given by:
	*
	/**
	* \brief LiveJournal social network graph
	* citation: Mikl¨®s Kurucz, Andr¨¢s A. Bencz¨²r, Attila Pereszl¨¦nyi. Large-Scale Principal Component Analysis on LiveJournal Friends Network. In proc Workshop on Social Network Mining and Analysis Held in conjunction with The 13th ACM SIGKDD International Conference on Knowledge Discovery and Data Mining (KDD 2008) August 24-27, 2008, Las Vegas, NV
	* source: https://dms.sztaki.hu/en/letoltes/livejournal-data
	* #nodes: 3,577,166
	* #edges: 44,913,072 directed edges
	* #countries: 244
	*/
	void load_live_journal_graph();

	/**
	* \brief Twitter social graph
	* citation: J. McAuley and J. Leskovec. Learning to Discover Social Circles in Ego Networks. NIPS, 2012. 
	* source: https://snap.stanford.edu/data/egonets-Twitter.html
	* #nodes: 81,306
	* #edges: 1,768,149
	*/
	void load_twitter_graph();
	void load_orkut_graph();
	void load_uk_graph();
	void load_it_graph();
	void load_synthetic_file(const char* filename);

	void load_p2p_graph();

	void load_wiki_graph();

	void load_roadnet_graph();

	void load_facebook_graph();

	void test_graph();

	void load_googleweb_graph();
	
	/**
	* file format: src tgt weight
	*/
	void load_from_file(std::string, int, int);

	~distributed_graph(){delete g; g=NULL; random_access_edges.clear();}
};

double rnd_generator(double lower, double upper);
