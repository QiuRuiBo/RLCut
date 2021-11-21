#ifndef APPS_H
#define APPS_H

//#include <boost/graph/adjacency_list.hpp>
//#include <boost/graph/graph_traits.hpp>
#include <limits>
#include "Cloud_instance.h"
//using namespace boost;

#endif

extern ofstream fileResult;
extern int Max_Monte_Carlo;
const float barrier_time = 0.03; //randomly set to 3, need to verify
//const float pagerank_delta = 0.01; //can be changed, set arbitrarily
//const int pruned_DCs = 5;

enum AppType{
	pagerank,
	sssp,
	subgraph,
	typesofapps
};

class BaseApp{
public:

	AppType mytype;
	distributed_graph* global_graph;
	
	std::vector<int> sources;
	// for sssp 
	bool directed = true;
	// gather from both direction
	bool BiDirected = true;
	
	Graph* pattern_graph;
	/**
	* the rate of reducing message size
	* assume all vertices have the same value
	*/
	float red_rate;
	float budget;
	int ITERATIONS;
	/** msg size of each vertex in MB
	* assume all vertices have the same msg size
	*/
	float msg_size;
	//virtual float gather_msg_size(int vindex, distributed_graph*)=0;
	//virtual float apply_msg_size(int vindex, distributed_graph*)=0;
	
	virtual std::vector<int> get_gather_nbrs(int vindex,distributed_graph*)=0;	
	virtual std::vector<int> get_scatter_nbrs(int vindex,distributed_graph*)=0;	
	
	/**
	* application implementation using GAS model 
	*/
	virtual void init(distributed_graph*, int i = 3577167) = 0;
	virtual void gather(int,int,distributed_graph*,VertexData*)=0;
	virtual void  sum(VertexData*,VertexData*,VertexData*)=0;
	virtual void apply(int,VertexData*,distributed_graph*)=0;
	virtual float scatter(int,int,distributed_graph*)=0;
	virtual void output(distributed_graph*)=0;
};


class PageRank:public BaseApp{
public:
	// Global random reset probability
	double RESET_PROB = 0.15;

	double TOLERANCE = 1.0E-2;

	bool USE_DELTA_CACHE = false;
	
	std::vector<int> get_gather_nbrs(int vindex,distributed_graph* dag){
		/**
		* gather neighbor: in_edges
		*/
		//cout << 1 << "nbrs " << endl;
		std::vector<int> gather_nbrs = dag->get_in_nbrs(vindex);
		return gather_nbrs;
	}
	std::vector<int> get_scatter_nbrs(int vindex,distributed_graph* dag){
		/**
		* scatter neighbor: out_edges
		*/		
		std::vector<int> scatter_nbrs = dag->get_out_nbrs(vindex);
		return scatter_nbrs;
	}

	void init(distributed_graph* dag,int i = 3577167){
		/*initial, activate all*/
		for (int viter = 0; viter < i; viter++){
			(*dag->g).myvertex[viter]->status = activated;
			(*dag->g).myvertex[viter]->next_status = deactivated;
			static_cast<PageRankVertexData*>((*dag->g).myvertex[viter]->data)->rank = 0;
		}
	}
	/* Gather the weighted rank of the adjacent page   */
	void gather(int vid, int nbr_id, distributed_graph* dag, VertexData* result){
		//int num_out_nbrs = dag->get_out_nbrs(nbr_id).size();
		int num_out_nbrs = dag->get_out_nbrs_size(nbr_id);
		if(num_out_nbrs == 0){
			//std::cout << "gather error. sth wrong with local graph edges." <<std::endl;
			fileResult << "gather error. sth wrong with local graph edges.\n";
			exit(1);
		}
		//PageRankVertexData* result=new PageRankVertexData();
		static_cast<PageRankVertexData*>(result)->rank = static_cast<PageRankVertexData*>((*dag->g).myvertex[nbr_id]->data)->rank / (float)num_out_nbrs;
		//result->rank = static_cast<PageRankVertexData*>(dag->g->myvertex[nbr_id]->data)->rank / (float)num_out_nbrs;
	//	printf("gather from %d to %d: %f\n",nbr_id,vid,result->rank);
		//lvalue = result;
		//delete result;
	}
	/*Sum the results from multiple gathering neighbors*/
	void sum(VertexData* a, VertexData* b, VertexData* result){
		//PageRankVertexData* result = new PageRankVertexData();
		//result->rank = static_cast<PageRankVertexData*>(a)->rank + static_cast<PageRankVertexData*>(b)->rank;
		//a = result;
		//delete result;
		static_cast<PageRankVertexData*>(result)->rank = static_cast<PageRankVertexData*>(a)->rank + static_cast<PageRankVertexData*>(b)->rank;
	}
	/* Use the total rank of adjacent pages to update this page */
	void apply(int vid, VertexData* total,distributed_graph* dag){
		float newvalue = (1.0 - RESET_PROB) * static_cast<PageRankVertexData*>(total)->rank + RESET_PROB;
		//float last_change = (newvalue - static_cast<PageRankVertexData*>((*dag->g).myvertex[vid].data)->rank)/(float)dag->get_out_nbrs(vid).size();
		float last_change = (newvalue - static_cast<PageRankVertexData*>((*dag->g).myvertex[vid]->data)->rank) / (float)dag->get_out_nbrs_size(vid);
		static_cast<PageRankVertexData*>((*dag->g).myvertex[vid]->data)->last_change = last_change;
		static_cast<PageRankVertexData*>((*dag->g).myvertex[vid]->data)->rank = newvalue;
		//std::cout << "vertex "<< vid << " new rank: "<<newvalue <<std::endl;
		if(ITERATIONS) (*dag->g).myvertex[vid]->next_status = activated;
	}
	/* The scatter function just signal adjacent pages */
	float scatter(int vid, int nbr_id,distributed_graph* dag) {
		// if(USE_DELTA_CACHE) {
		  // context.post_delta(edge.target(), last_change);
		// }
		if(USE_DELTA_CACHE){
			if(static_cast<PageRankVertexData*>((*dag->g).myvertex[vid]->data)->last_change > TOLERANCE || static_cast<PageRankVertexData*>((*dag->g).myvertex[vid]->data)->last_change < -TOLERANCE) {
				(*dag->g).myvertex[nbr_id]->next_status = activated;
			} 				
		}else{
			(*dag->g).myvertex[nbr_id]->next_status = activated;
		}	
		return static_cast<PageRankVertexData*>((*dag->g).myvertex[vid]->data)->last_change;
	  }
	  
	void output(distributed_graph* dag){
		for(int v=0; v<dag->num_vertices; v++){
			//std::cout << (*dag->g).myvertex[v]->vertex_id << ": rank " <<static_cast<PageRankVertexData*>((*dag->g).myvertex[v]->data)->rank << std::endl;
			fileResult << (*dag->g).myvertex[v]->vertex_id << ": rank " <<static_cast<PageRankVertexData*>((*dag->g).myvertex[v]->data)->rank << "\n";
		}			
	}
	/* float gather_msg_size(int vindex, distributed_graph* dag){
		float msg_size = 0.000008;		
		return msg_size;
	}
	float apply_msg_size(int vindex, distributed_graph* dag){
		float msg_size = 0.000008;		
		return msg_size;
	} */
	PageRank(){
		red_rate = 0.5; BiDirected = false;
		msg_size = 0.000008;
	}
};

class SSSP :public BaseApp{
public:
	
	std::vector<int> get_gather_nbrs(int vindex,distributed_graph* dag){
		/**
		* gather neighbor: all neighbors if undirected
		*				   in edges if directed
		*/
		//cout << 2 << "nbrs " << endl;
		std::vector<int> gather_nbrs = dag->get_in_nbrs(vindex);
		if (!directed){
			//cout << "!directed" << endl;
			std::vector<int> out_nbrs = dag->get_out_nbrs(vindex);
			for (int i = 0; i < out_nbrs.size(); i++)
				gather_nbrs.push_back(out_nbrs[i]);
		}
		//cout << gather_nbrs.size() << "gather_nbrs" << endl;
		return gather_nbrs;
	}
	std::vector<int> get_scatter_nbrs(int vindex,distributed_graph* dag){
		/**
		* scatter neighbor: all_edges if undirected; out edges if directed
		*/
		std::vector<int> scatter_nbrs = dag->get_out_nbrs(vindex);
		if (!directed){
			std::vector<int> in_nbrs = dag->get_in_nbrs(vindex);
			for (int i = 0; i < in_nbrs.size(); i++)
				scatter_nbrs.push_back(in_nbrs[i]);
		}		
		return scatter_nbrs;
	}

	void init(distributed_graph* dag, int i = 3577167){
		/*initial, activate all sources*/
		for (int viter = 0; viter < i; viter++){
			static_cast<SSSPVertexData*>((*dag->g).myvertex[viter]->data)->dist = std::numeric_limits<float>::max();
			for (int siter = 0; siter < sources.size(); siter++){
				/* global to local vertex id */
				if((*dag->g).myvertex[viter]->vertex_id == sources[siter]){
					(*dag->g).myvertex[viter]->status = activated;
					break;
				}
			}
		}
	}

	inline int get_other_vertex(int edge_id, int v_id, distributed_graph* dag){
		//int src = source(dag->random_access_edges[edge_id], *dag->g);
		int src = dag->g->random_edges[edge_id].first;
		//int tgt = target(dag->random_access_edges[edge_id], *dag->g);
		int tgt = dag->g->random_edges[edge_id].second;
		return src == v_id ? tgt : src;
	}
	
	/*Collect the distance to the neighbor*/
	void gather(int vid, int nbr_id, distributed_graph* dag, VertexData* result){
		//std::cout << "gather not implemented for sssp" << std::endl;
		/*in_edge_iterator in_i, in_end;
		Edge e, edge_id;
		boost::tie(in_i, in_end) = in_edges(vid, *dag->g);
		for (; in_i != in_end; ++in_i) {
			e = *in_i;
			Vertex src = source(e, *dag->g);
			if(src == nbr_id){
				edge_id = e;
				break;
			}
		}*/
		//SSSPVertexData* result = new SSSPVertexData();
		//result->dist = (static_cast<SSSPVertexData*>((*dag->g).myvertex[nbr_id]->data)->dist + dag->WeightMap[edge_id]);
		//result->dist = static_cast<SSSPVertexData*>((*dag->g).myvertex[nbr_id]->data)->dist;
		//lvalue = result;		
		//delete result;
		static_cast<SSSPVertexData*>(result)->dist = (static_cast<SSSPVertexData*>((*dag->g).myvertex[nbr_id]->data)->dist);// + dag->WeightMap[edge_id]);
	}
	/*Sum the results from multiple gathering neighbors*/
	void sum(VertexData* a, VertexData* b, VertexData* result){
		//SSSPVertexData* result = new SSSPVertexData();
		//result->dist = std::min(static_cast<SSSPVertexData*>(a)->dist,static_cast<SSSPVertexData*>(b)->dist);
		//a = result;
		//delete result;
		static_cast<SSSPVertexData*>(result)->dist = std::min(static_cast<SSSPVertexData*>(a)->dist,static_cast<SSSPVertexData*>(b)->dist);
	}
	/*If the distance is smaller then update*/
	void apply(int vid, VertexData* acc,distributed_graph* dag){
		static_cast<SSSPVertexData*>((*dag->g).myvertex[vid]->data)->changed = false;
		if(static_cast<SSSPVertexData*>((*dag->g).myvertex[vid]->data)->dist > static_cast<SSSPVertexData*>(acc)->dist) {
		  static_cast<SSSPVertexData*>((*dag->g).myvertex[vid]->data)->changed = true;
		  static_cast<SSSPVertexData*>((*dag->g).myvertex[vid]->data)->dist = static_cast<SSSPVertexData*>(acc)->dist;
		}
	}
	/*The scatter function just signal adjacent pages */
	float scatter(int vid, int nbr_id,distributed_graph* dag){
		/*out_edge_iterator out_i, out_end;
		Edge e,edge_id;
		boost::tie(out_i, out_end) = out_edges(vid, *dag->g);
		for (; out_i != out_end; ++out_i){
			e = *out_i;
			Vertex tgt = target(e, *dag->g);
			if(tgt == nbr_id){
				edge_id = e;
				break;
			}
		}*/
		//float newd = static_cast<SSSPVertexData*>((*dag->g).myvertex[vid]->data)->dist + dag->WeightMap[edge_id];
		float newd = static_cast<SSSPVertexData*>((*dag->g).myvertex[vid]->data)->dist;
		if(static_cast<SSSPVertexData*>((*dag->g).myvertex[vid]->data)->changed)
			(*dag->g).myvertex[nbr_id]->next_status = activated;
		return newd;
	}
	void output(distributed_graph* dag){
		std::cout << "output not implemented for sssp." <<std::endl;
	}
	SSSP(){ 
		red_rate=0.5; msg_size = 0.000004; 
		if(directed) BiDirected = false;
		else BiDirected = true;
	}
};


class SubgraphIsom: public BaseApp{
	
public:
	std::vector<int> get_gather_nbrs(int vindex,distributed_graph* dag){
		/**
		* gather neighbor: all neighbors 
		*/
		//cout << 3 << "nbrs " << endl;
		std::vector<int> gather_nbrs = dag->get_in_nbrs(vindex);
		std::vector<int> out_nbrs = dag->get_out_nbrs(vindex);
		for (int i = 0; i < out_nbrs.size(); i++)
			gather_nbrs.push_back(out_nbrs[i]);		
		return gather_nbrs;
	}
	std::vector<int> get_scatter_nbrs(int vindex,distributed_graph* dag){
		/**
		* scatter neighbor: all neighbors
		*/
		std::vector<int> scatter_nbrs = dag->get_out_nbrs(vindex);
		std::vector<int> in_nbrs = dag->get_in_nbrs(vindex);
		for (int i = 0; i < in_nbrs.size(); i++)
			scatter_nbrs.push_back(in_nbrs[i]);		
		return scatter_nbrs;
	}

	void init(distributed_graph* dag,int i = 3577167){
		/*initial, activate all*/
		//#pragma omp parallel for
		for (int viter = 0; viter < i; viter++){
			(*dag->g).myvertex[viter]->status = activated;
			(*dag->g).myvertex[viter]->next_status = deactivated;
			int gvid = (*dag->g).myvertex[viter]->vertex_id;			
			for(int i=0; i<pattern_graph->myvertex.size(); i++){
				//no label, so add all
				SubgraphVertexData::Message init_msg;
				init_msg.pairs.push_back(std::pair<int,int> (i,gvid));	
				init_msg.forwarding_trace.push_back(gvid);
				static_cast<SubgraphVertexData*>((*dag->g).myvertex[viter]->data)->matches.push_back(init_msg);				
			}			
		}
	}

	/*return the messages of neighbors*/
	void gather(int vid, int nbr_id, distributed_graph* dag, VertexData* result){			
		//SubgraphVertexData* result = new SubgraphVertexData();
		for(int i=0; i< static_cast<SubgraphVertexData*>((*dag->g).myvertex[nbr_id]->data)->matches.size(); i++)
			//result->matches.push_back(static_cast<SubgraphVertexData*>((*dag->g).myvertex[nbr_id]->data)->matches[i]);
		    static_cast<SubgraphVertexData*>(result)->matches.push_back(static_cast<SubgraphVertexData*>((*dag->g).myvertex[nbr_id]->data)->matches[i]);
		//lvalue = result;
		//delete result;
	}
	
	/*Sum the results from multiple gathering neighbors*/
	void sum(VertexData* a, VertexData* b, VertexData* result){
		//SubgraphVertexData* result = new SubgraphVertexData();
		//int asize = static_cast<SubgraphVertexData*>(a)->matches.size();
		int bsize = static_cast<SubgraphVertexData*>(b)->matches.size();
		//for(int i=0; i<asize; i++)
			//result->matches.push_back(static_cast<SubgraphVertexData*>(a)->matches[i]);
		for(int i=0; i<bsize; i++)
			//result->matches.push_back(static_cast<SubgraphVertexData*>(b)->matches[i]);
			static_cast<SubgraphVertexData*>(result)->matches.push_back(static_cast<SubgraphVertexData*>(b)->matches[i]);
		//a = result ;
		//delete result;
	}
	
	/** On receiving messages, vertex vg uses the following operations on each message according to different situations.
	* Forward: if vg is not in the forwarding trace, 
			   add it to the forwarding trace, and send the message to all neighbors.
	* Update (Spawn): If vg has not matched any vertex in the pattern and it can match to
		some neighbor of the vertex in the pattern, which the message source has matched, then
		for each possible matching vertex vp, vertex function generates a new message based on
		the old one by adding the matching pair and replacing all IDs in the forwarding trace
		with the ID of the current vertex vg. In this case, a message may spawn multiple new
		messages. If all the pattern vertices have been matched already after this operation in a
		new message, then an isomorphic subgraph has been found. Otherwise the messages are sent to all neighbors.
	* Drop: In other situations, the message is dropped, because it is hopeless or redundant to
		find further matching vertex.
	*/
	void apply(int vid, VertexData* acc, distributed_graph* dag){
		static_cast<SubgraphVertexData*>((*dag->g).myvertex[vid]->data)->matches.clear();
		//use global vid 
		int gvid = (*dag->g).myvertex[vid]->vertex_id;
		if(false){
			dag->init_indicator = false;
		}else{
			int num_msg = static_cast<SubgraphVertexData*>(acc)->matches.size();
			for(int i=0; i<num_msg; i++){
				int j = 0;
				for(; j< static_cast<SubgraphVertexData*>(acc)->matches[i].pairs.size(); j++){
					if(static_cast<SubgraphVertexData*>(acc)->matches[i].pairs[j].second == gvid){
						if(std::find(static_cast<SubgraphVertexData*>(acc)->matches[i].forwarding_trace.begin(),
							static_cast<SubgraphVertexData*>(acc)->matches[i].forwarding_trace.end(),gvid) == static_cast<SubgraphVertexData*>(acc)->matches[i].forwarding_trace.end()){
							//forward operation
							static_cast<SubgraphVertexData*>(acc)->matches[i].forwarding_trace.push_back(gvid);
							static_cast<SubgraphVertexData*>((*dag->g).myvertex[vid]->data)->matches.push_back(static_cast<SubgraphVertexData*>(acc)->matches[i]);
						}
						break;						
					}//end if				
				}//end for each match pair
				if(j == static_cast<SubgraphVertexData*>(acc)->matches[i].pairs.size()){
					//update operation
					int last_trace = static_cast<SubgraphVertexData*>(acc)->matches[i].forwarding_trace.back();
					int pattern_vertex = -1;
					for(j=0; j<static_cast<SubgraphVertexData*>(acc)->matches[i].pairs.size(); j++){
						if(static_cast<SubgraphVertexData*>(acc)->matches[i].pairs[j].second == last_trace){
							pattern_vertex = static_cast<SubgraphVertexData*>(acc)->matches[i].pairs[j].first;
							break;							
						}
					}
					//adja_iterator nbrIt, nbrEnd;
					//boost::tie(nbrIt, nbrEnd) = adjacent_vertices( pattern_vertex, *pattern_graph );
					vector<int> nbrs1 = pattern_graph->myvertex[pattern_vertex]->out_neighbour;
//					printf("pattern_vertex:%d\n", pattern_vertex);
					//for (; nbrIt != nbrEnd; ++nbrIt){
					for(int i =0; i < nbrs1.size(); i++){
						//check the matching criteria
						int k = 0;
						for(;k<static_cast<SubgraphVertexData*>(acc)->matches[i].pairs.size(); k++){
							//if(static_cast<SubgraphVertexData*>(acc)->matches[i].pairs[k].first == *nbrIt)
							if(static_cast<SubgraphVertexData*>(acc)->matches[i].pairs[k].first == nbrs1[i])
								break;
						}
						if(k == static_cast<SubgraphVertexData*>(acc)->matches[i].pairs.size()){
							//this nbr is not matched yet		
							bool nomatch = false;
							//adja_iterator nnIt, nnEnd;									
							for(k = 0; k < static_cast<SubgraphVertexData*>(acc)->matches[i].pairs.size(); k++){
								//if there is an edge between *nbrIt and the k-th matched pattern
								//boost::tie(nnIt, nnEnd) = adjacent_vertices( *nbrIt, *pattern_graph );
								vector<int> nbrs2 = pattern_graph->myvertex[nbrs1[i]]->out_neighbour;
								//printf("nbrs1[i]:%d\n", nbrs1[i]);
								//for(; nnIt != nnEnd; ++nnIt){
								for(int i2 = 0; i2 < nbrs2.size(); i2++){
									//if(*nnIt == static_cast<SubgraphVertexData*>(acc)->matches[i].pairs[k].first){
									if(nbrs2[i2] == static_cast<SubgraphVertexData*>(acc)->matches[i].pairs[k].first){
										//check if the same edge exist in the graph
										//adja_iterator localIt, localEnd;
										//boost::tie(localIt, localEnd) = adjacent_vertices( static_cast<SubgraphVertexData*>(acc)->matches[i].pairs[k].second, *global_graph->g );
										vector<int> nbrs3 = pattern_graph->myvertex[static_cast<SubgraphVertexData*>(acc)->matches[i].pairs[k].second]->out_neighbour;
										//printf("third:%3:%d\n",static_cast<SubgraphVertexData*>(acc)->matches[i].pairs[k].second);
										//for(; localIt != localEnd; ++ localIt)
										int i3;
										for(i3 = 0; i3 < nbrs3.size(); i3++)
											if(nbrs3[i3] == gvid)
												break;
										//if(localIt == localEnd){
										if(i3 == nbrs3.size()){
											nomatch = true;
											break;
										}
									}
								}
								if(nomatch)
									break;		
							}//for all pairs
							if(!nomatch){
								SubgraphVertexData::Message mprime;
								mprime.pairs = static_cast<SubgraphVertexData*>(acc)->matches[i].pairs;
								//mprime.pairs.push_back(std::pair<int,int>(*nbrIt,gvid));
								mprime.pairs.push_back(std::pair<int,int>(nbrs1[i],gvid));
								mprime.forwarding_trace.push_back(gvid);
								//if(mprime.pairs.size() == pattern_graph->m_vertices.size()){
								if (mprime.pairs.size() == pattern_graph->myvertex.size()){
									//found a subgraph
									/* std::cout << "found a subgraph, with vertices: \n" ;
									for(int kk=0; kk<mprime.pairs.size(); kk++){
										std::cout << mprime.pairs[kk].first << ",\t" << mprime.pairs[kk].second << std::endl;
									} */
								}else{
									static_cast<SubgraphVertexData*>(acc)->matches.push_back(mprime);
								}
							}
						}
					}//end for pv
				}
			}//end for each msg
		}		
	}
	
	/*The scatter function signal all neighbors */
	float scatter(int vid, int nbr_id, distributed_graph* dag){
		if(static_cast<SubgraphVertexData*>((*dag->g).myvertex[vid]->data)->matches.size() > 0){
			(*dag->g).myvertex[vid]->next_status = activated;
			(*dag->g).myvertex[nbr_id]->next_status = activated;	
		}				
		return 0.0;
	}
	void output(distributed_graph* dag){
		
	}
	SubgraphIsom(){ 
		// red_rate=1.0; msg_size = 1.0; 
		red_rate=1.0; msg_size = 0.001; 
		cout << "msg_size = 0.001" << endl;
		BiDirected = true;
	} //msg size not constant
};
