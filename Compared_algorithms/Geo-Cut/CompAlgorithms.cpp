#include "Simulator.h"
#include <omp.h>
#include <ctime>
#include <fstream>
#include <ostream>
#include <iomanip>
/**
* the expected number of replicas for a vertex
* can be fixed by a user (we use 4 as an example)
* or calculated using methods in PowerGraph paper
*/
extern int N_THREADS;
float LARGE_FLOAT = 1e18;
extern ofstream fileResult;

double pairing_function(int x, int y){
	double result = 0;
	double a = x+y;
	double b = x+y+1;
	result = a*b*0.5+y;
	return result;
}

void Optimizer::RandomReplication(){
	/** random edge distribution, a version of streaming vertex cut
	* If A(u) and/or A(v) not empty, randomly select one dc to place
	*/	
	
	omp_set_num_threads(N_THREADS);
	
	for(int vi=0; vi<dag->num_vertices; vi++){
		(*dag->g).myvertex[vi]->replica_location.clear();
		(*dag->g).myvertex[vi]->replica_location.push_back((*dag->g).myvertex[vi]->location_id);		
	}
	//int numedges = num_edges(*dag->g);
	int numedges = dag->num_edges;
	edge_distribution = std::vector<std::vector<int> >(DCs.size());
	//printf("num of edges: %d\n",numedges);
	fileResult << "num of edges: " << numedges << "\n" ;
	// std::pair<edge_iter,edge_iter> eit = edges(*dag->g);
	
	//int numoflocks = pairing_function(dag->num_vertices,dag->num_vertices);
	int numoflocks = dag->num_vertices;
	std::vector<omp_lock_t> writelock(numoflocks);
	for(int i=0; i<numoflocks; i++){
		omp_init_lock(&writelock[i]);
		// printf("initializing %d out of %d locks\n",i,numoflocks);
	}
	int numoflocks1 = DCs.size();
	std::vector<omp_lock_t> writelock1(numoflocks1);
	for(int i=0; i<numoflocks1; i++){
		omp_init_lock(&writelock1[i]);
	}
	time_t starttime , endtime;
	time(&starttime);
#pragma omp parallel for 
	for(int ecount = 0; ecount < numedges; ecount++){	
		//find out the time for one edge assignment
		//Edge e = dag->random_access_edges[ecount];
		//int src = source(e, *dag->g);
		int src = (*dag->g).source_edge(ecount);
		//int tgt = target(e, *dag->g);
		int tgt = (*dag->g).target_edge(ecount);
		int threadid = omp_get_thread_num();
		int lockid1, lockid2;
		lockid1 = src<tgt?src:tgt;
		lockid2 = src>tgt?src:tgt;
		// int lockid = std::fmod(pairing_function(src,tgt),(double)numoflocks);
		// printf("thread %d acquiring lock %d and %d: %d-->%d\n",threadid,lockid1,lockid2,src,tgt);
        omp_set_lock(&writelock[lockid1]);	
		omp_set_lock(&writelock[lockid2]);	
		
		std::vector<int> src_replicas = (*dag->g).myvertex[src]->replica_location;
		std::vector<int> tgt_replicas = (*dag->g).myvertex[tgt]->replica_location;
		int loc_id;
		if(src_replicas.size()>tgt_replicas.size()){
			loc_id = src_replicas[(int)dag->rnd_generator(0, src_replicas.size())];
			if(std::find(tgt_replicas.begin(),tgt_replicas.end(),loc_id) == tgt_replicas.end()){
				(*dag->g).myvertex[tgt]->replica_location.push_back(loc_id);
			}	
		}else{
			loc_id = tgt_replicas[(int)dag->rnd_generator(0, tgt_replicas.size())];
			if(std::find(src_replicas.begin(),src_replicas.end(),loc_id) == src_replicas.end()){
				(*dag->g).myvertex[src]->replica_location.push_back(loc_id);
			}
		}
		omp_set_lock(&writelock1[loc_id]);
		edge_distribution[loc_id].push_back(ecount);
		omp_unset_lock(&writelock1[loc_id]);
		if(ecount % 1000000 == 0){
			//std::printf("thread id: %d,\t addressed %d edges\n",threadid, ecount);
			fileResult << "threads id: " << threadid << " , addressed " << ecount << " edges\n";
		}
		// printf("thread %d release lock %d\n",threadid,lockid);
		omp_unset_lock(&writelock[lockid2]);
		omp_unset_lock(&writelock[lockid1]);
	}
/*	time(&endtime);
	float timeelapsed = difftime(endtime,starttime);
	cout << timeelapsed << endl;*/

	for(int i=0; i<numoflocks; i++)
		omp_destroy_lock(&writelock[i]);
	for(int i=0; i<numoflocks1; i++)
		omp_destroy_lock(&writelock1[i]);
	/*randomly select one replica as master*/
	#pragma omp parallel for
	for(int vi=0; vi<dag->num_vertices; vi++){
		int master = dag->rnd_generator(0,(*dag->g).myvertex[vi]->replica_location.size());
		(*dag->g).myvertex[vi]->master_location = (*dag->g).myvertex[vi]->replica_location[master];
	}
	time(&endtime);
	float timeelapsed = difftime(endtime, starttime);
	cout << timeelapsed << endl;
	
}

void Optimizer::GreedyReplication(){
	/** load balacing vertex cut, from PowerGraph paper
	* Case 1: If A(u) and A(v) intersect, then the edge should be
		assigned to the least loaded machine in the intersection.
	* Case 2: If A(u) and A(v) are not empty and do not intersect,
		then the edge should be assigned to one of the machines
		from the vertex with the most unassigned edges.
	* Case 3: If only one of the two vertices has been assigned, then
		choose the least loaded machine from the assigned vertex.
	* Case 4: If neither vertex has been assigned, then assign the
		edge to the least loaded machine.
	*/
	std::vector<int> num_unassigned_edge = std::vector<int>(dag->num_vertices);

	for(int vi=0; vi<dag->num_vertices; vi++){
		(*dag->g).myvertex[vi]->replica_location.clear();
		(*dag->g).myvertex[vi]->replica_location.push_back((*dag->g).myvertex[vi]->location_id);
		int nbrs;
		if(myapp->BiDirected)
			//nbrs = dag->get_in_nbrs(vi).size() + dag->get_out_nbrs(vi).size();
			nbrs = dag->get_in_nbrs_size(vi) + dag->get_out_nbrs_size(vi);
		else 
			//nbrs = dag->get_in_nbrs(vi).size();
			nbrs = dag->get_in_nbrs_size(vi);
		num_unassigned_edge[vi] = nbrs;
	}
	//int numedges = num_edges(*dag->g);		
	int numedges = dag->num_edges;
	edge_distribution = std::vector<std::vector<int> >(DCs.size());
	std::vector<int> loads = std::vector<int>(DCs.size(),0);
	for(int vi = 0; vi< dag->num_vertices; vi++){
		loads[(*dag->g).myvertex[vi]->location_id] ++;
	}
	omp_set_num_threads(N_THREADS);
	int numoflocks = dag->num_vertices;
	std::vector<omp_lock_t> writelock(numoflocks);
	for(int i=0; i<numoflocks; i++){
		omp_init_lock(&writelock[i]);
		// printf("initializing %d out of %d locks\n",i,numoflocks);
	}
	int numoflocks1 = DCs.size();
	std::vector<omp_lock_t> writelock1(numoflocks1);
	for(int i=0; i<numoflocks1; i++){
		omp_init_lock(&writelock1[i]);
	}
	
//#pragma omp parallel for
	for(int ecount = 0; ecount < numedges; ecount++){
		//int src = source(dag->random_access_edges[ecount], *dag->g);
		int src = (*dag->g).source_edge(ecount);
		//printf("ecout:%d\n",ecount);
		//printf("src:%d\n",src);
		//int tgt = target(dag->random_access_edges[ecount], *dag->g);	
		int tgt = (*dag->g).target_edge(ecount);
		//printf("tgt:%d\n",tgt);
			
		int threadid = omp_get_thread_num();
		int lockid1, lockid2;
		lockid1 = src<tgt?src:tgt;
		lockid2 = src>tgt?src:tgt;
		// int lockid = std::fmod(pairing_function(src,tgt),(double)numoflocks);
		// printf("thread %d acquiring lock %d and %d: %d-->%d\n",threadid,lockid1,lockid2,src,tgt);
        omp_set_lock(&writelock[lockid1]);	
		omp_set_lock(&writelock[lockid2]);
		
		num_unassigned_edge[src] --;
		num_unassigned_edge[tgt] --;
		std::vector<int> src_replicas = (*dag->g).myvertex[src]->replica_location;
		//printf("src_replicas:%d\n",(int)src_replicas.size());
		std::vector<int> tgt_replicas = (*dag->g).myvertex[tgt]->replica_location;
		//printf("tgt_replicas:%d\n", (int)tgt_replicas.size());
		std::vector<int> intersection(src_replicas.size()+tgt_replicas.size());
		std::vector<int>::iterator it;
		std::sort(src_replicas.begin(),src_replicas.end());
		std::sort(tgt_replicas.begin(),tgt_replicas.end());
		it = std::set_intersection(src_replicas.begin(), src_replicas.end(), tgt_replicas.begin(), tgt_replicas.end(), intersection.begin());
		intersection.resize(it-intersection.begin()); 
		
		if(intersection.size() > 0){		
			//cout << "1" << endl;	
			int least_loaded = -1;
			float load = LARGE_FLOAT;
			for(int i=0; i<intersection.size(); i++){
				if(loads[intersection[i]] < load){
					least_loaded = intersection[i];
					load = loads[intersection[i]];
				}
			}
			//edge_distribution[least_loaded].push_back(std::pair<int,int>(src,tgt));	
			omp_set_lock(&writelock1[least_loaded]);
			edge_distribution[least_loaded].push_back(ecount);
			omp_unset_lock(&writelock1[least_loaded]);
			//std::cout << "case 1: " << least_loaded << "added one edge." << std::endl;
		}else if(src_replicas.size() > 0 && tgt_replicas.size() > 0){
		//	cout << "2 " << endl;
			std::vector<int> space = num_unassigned_edge[src] > num_unassigned_edge[tgt] ? src_replicas : tgt_replicas;
			float load = LARGE_FLOAT;
			int least_loaded = -1;
			for(int i=0; i<space.size(); i++){				
				if(loads[space[i]] < load){
					load = loads[space[i]];
					least_loaded = space[i];
				}
			}
			//edge_distribution[least_loaded].push_back(std::pair<int,int>(src,tgt));
			omp_set_lock(&writelock1[least_loaded]);
			edge_distribution[least_loaded].push_back(ecount);		
			if(num_unassigned_edge[src] > num_unassigned_edge[tgt]){
				(*dag->g).myvertex[tgt]->replica_location.push_back(least_loaded);
				//printf("tgt:%d,size:%d", tgt, (int)((*dag->g).myvertex[tgt].replica_location.size()));
				//DCs[least_loaded]->contained_vertices.push_back(tgt);
				loads[least_loaded] ++;
				//std::cout << "case 2: " << least_loaded << "added one vertex." << std::endl;
			}else{
				(*dag->g).myvertex[src]->replica_location.push_back(least_loaded);
				//printf("tgt:%d,size:%d", tgt, (int)((*dag->g).myvertex[tgt].replica_location.size()));
				//DCs[least_loaded]->contained_vertices.push_back(src);
				loads[least_loaded] ++;
				//std::cout << "case 2: " << least_loaded << "added one vertex." << std::endl;
			}
			omp_unset_lock(&writelock1[least_loaded]);	
		}else if(src_replicas.size() > 0 || tgt_replicas.size() > 0){
			std::cout << "case 3" <<std::endl;
			std::vector<int> space = src_replicas.size()>0 ? src_replicas : tgt_replicas;
			float load = LARGE_FLOAT;
			int least_loaded = -1;
			for(int i=0; i<space.size(); i++){				
				if(loads[space[i]] < load){
					load = loads[space[i]];
					least_loaded = space[i];
				}
			}
			//edge_distribution[least_loaded].push_back(std::pair<int,int>(src,tgt));
			omp_set_lock(&writelock1[least_loaded]);
			edge_distribution[least_loaded].push_back(ecount);
			if(src_replicas.size()>0){
				//tgt is replicated
				(*dag->g).myvertex[tgt]->replica_location.push_back(least_loaded);
				//printf("tgt:%d,size:%d", tgt, (int)((*dag->g).myvertex[tgt].replica_location.size()));
				//DCs[least_loaded]->contained_vertices.push_back(tgt);
				loads[least_loaded] ++;
			}else{
				(*dag->g).myvertex[src]->replica_location.push_back(least_loaded);
				//printf("tgt:%d,size:%d", tgt, (int)((*dag->g).myvertex[src].replica_location.size()));
				//DCs[least_loaded]->contained_vertices.push_back(src);
				loads[least_loaded] ++;
			}
			omp_unset_lock(&writelock1[least_loaded]);	
		}else {
			std::cout << "this case should never happen \n";
			fileResult << "this case should never happen \n";
			float load = LARGE_FLOAT;
			int least_loaded = -1;
			for(int i=0; i<DCs.size(); i++){				
				if(loads[i] < load){
					load = loads[i];
					least_loaded = i;
				}
			}
			//edge_distribution[least_loaded].push_back(std::pair<int,int>(src,tgt));
			omp_set_lock(&writelock1[least_loaded]);
			edge_distribution[least_loaded].push_back(ecount);
			(*dag->g).myvertex[tgt]->replica_location.push_back(least_loaded);
			//printf("tgt:%d,size:%d", tgt, (int)((*dag->g).myvertex[tgt].replica_location.size()));
			loads[least_loaded] ++;
			(*dag->g).myvertex[src]->replica_location.push_back(least_loaded);
			//printf("tgt:%d,size:%d",tgt, (int)((*dag->g).myvertex[tgt].replica_location.size()));
			//printf("done");
			loads[least_loaded] ++;
			omp_unset_lock(&writelock1[least_loaded]);	
		}
		
		if(ecount % 1000000 == 0){
			//std::cout << "addressed " << ecount << "edges\n";
			fileResult <<"addressed " <<ecount <<" edges\n";
		}
		omp_unset_lock(&writelock[lockid2]);
		omp_unset_lock(&writelock[lockid1]);
	}
	
	for(int i=0; i<numoflocks; i++)
		omp_destroy_lock(&writelock[i]);
	
	/*use the initial location as master*/
	#pragma omp parallel for
	for(int vi=0; vi<dag->num_vertices; vi++){
		(*dag->g).myvertex[vi]->master_location = (*dag->g).myvertex[vi]->location_id;
	}
}

/**
* HybridCut: Powerlyra paper, Eurosys'15
* step 1: heuristic hybrid-cut
		  differentiated partitioning for low-degree and high-degree vertices
* step 2: locality-conscious graph layout
*/
void Optimizer::HybridCut(){
	
	/******************* Step 1: Ginger-based hybrid-cut **********************/
	
	/************** Step 2: locality-conscious graph layout *******************/
	
}

std::pair<float,float> Optimizer::commCost(){
	omp_set_num_threads(N_THREADS);
	std::vector<float> totalcost = std::vector<float>(DCs.size(),0.0);
	std::vector<float> cur_wan_usage = std::vector<float>(DCs.size(),0.0);
	#pragma omp parallel for
	for(int dc=0; dc<DCs.size(); dc++){
		DCs[dc]->g_dnload_data = 0.0;
		DCs[dc]->g_upload_data = 0.0;
		DCs[dc]->a_dnload_data = 0.0;
		DCs[dc]->a_upload_data =0.0;
		std::vector<int> contained_vertices;
		for(int ei=0; ei<edge_distribution[dc].size(); ei++){
			int ecount = edge_distribution[dc][ei];
			//int src = source(dag->random_access_edges[ecount], *dag->g);
			int src = (*dag->g).source_edge(ecount);
			//int tgt = target(dag->random_access_edges[ecount], *dag->g);	
			int tgt = (*dag->g).target_edge(ecount);
			contained_vertices.push_back(src);
			contained_vertices.push_back(tgt);			
		}
		std::sort(contained_vertices.begin(),contained_vertices.end());
		std::vector<int>::iterator it = std::unique(contained_vertices.begin(),contained_vertices.end());
		contained_vertices.resize(std::distance(contained_vertices.begin(),it));
		for(int vi=0; vi<contained_vertices.size(); vi++){
			//gather: master download, non-master upload
			//apply: master upload, non-master download
			int gv = contained_vertices[vi];
			if((*dag->g).myvertex[gv]->master_location == dc){
				if(!myapp->BiDirected){
					//gather neighbor: in_edges
					for(int di=0; di<DCs.size(); di++){
						if(dc != di){
							DCs[dc]->g_dnload_data += in_nbr_distribution[gv][di] * myapp->msg_size;// * myapp->red_rate;
						}
					}					
				}else {
					//gather neighbor: all neighbors
					for(int di=0; di<DCs.size(); di++){
						if(dc != di){
							DCs[dc]->g_dnload_data += (in_nbr_distribution[gv][di] + out_nbr_distribution[gv][di]) 
								* myapp->msg_size;// * myapp->red_rate;							
						}
					}
				}
				DCs[dc]->a_upload_data += myapp->msg_size * ((*dag->g).myvertex[gv]->replica_location.size() - 1);
			}else{
				if(!myapp->BiDirected){
					//gather neighbor: in_edges					
					DCs[dc]->g_upload_data += in_nbr_distribution[gv][dc] * myapp->msg_size;// * myapp->red_rate;					
				}else{
					//gather neighbor: all neighbors
					DCs[dc]->g_upload_data += (in_nbr_distribution[gv][dc] + out_nbr_distribution[gv][dc]) * myapp->msg_size;// * myapp->red_rate;							
				}
				DCs[dc]->a_dnload_data += myapp->msg_size;
			}
			DCs[dc]->data_size += (*dag->g).myvertex[gv]->data->input_size;
		}//end for each vi		
		cur_wan_usage[dc] = DCs[dc]->g_upload_data + DCs[dc]->a_upload_data;
		totalcost[dc] = (DCs[dc]->g_upload_data + DCs[dc]->a_upload_data) * DCs[dc]->upload_price / 1000.0; //price per GB
		// totalcost += (DCs[dc]->g_dnload_data + DCs[dc]->a_dnload_data) * DCs[dc]->download_price / 1000.0; //price per GB		
	}//end for each dc
	float cost=0.0; float wan=0.0;
	for(int dc=0; dc<DCs.size(); dc++){
		cost += totalcost[dc];
		wan += cur_wan_usage[dc];
	}
	return std::pair<float,float>(cost,wan);
}

//replica location does not switch, because calculating the commCost only need the number of replicas
void ContextSwitch(int dc1, int dc2, Optimizer* opt){
	omp_set_num_threads(N_THREADS);

	int vertexsize = opt->dag->num_vertices;
	std::vector<int> tmp_master = std::vector<int>(vertexsize);
	for(int i=0; i<vertexsize; i++){
		tmp_master[i] = (*opt->dag->g).myvertex[i]->master_location;
	}
	//update vertex replica location and in/out nbrs
	std::vector<int> contained_vertices[2];
	#pragma omp parallel for
	for(int i=0; i<2; i++){
		int dc = dc1;
		if(i==1) dc=dc2;
		for(int ei=0; ei<opt->edge_distribution[dc].size(); ei++){
			int ecount = opt->edge_distribution[dc][ei];
			//int src = source(opt->dag->random_access_edges[ecount], *opt->dag->g);
			int src = (*opt->dag->g).source_edge(ecount);
			//int tgt = target(opt->dag->random_access_edges[ecount], *opt->dag->g);	
			int tgt = (*opt->dag->g).target_edge(ecount);
			contained_vertices[i].push_back(src);
			contained_vertices[i].push_back(tgt);			
		}
		std::sort(contained_vertices[i].begin(),contained_vertices[i].end());
		std::vector<int>::iterator it = std::unique(contained_vertices[i].begin(),contained_vertices[i].end());
		contained_vertices[i].resize(std::distance(contained_vertices[i].begin(),it));
	}
		
	// switch master location
#pragma omp parallel for
	for(int vi=0; vi<contained_vertices[0].size(); vi++){
		int gv = contained_vertices[0][vi];
		opt->in_nbr_distribution[gv][dc1] = 0;
		opt->out_nbr_distribution[gv][dc1] = 0;
		
		for(int ri =0; ri < (*opt->dag->g).myvertex[gv]->replica_location.size(); ri++){
			if((*opt->dag->g).myvertex[gv]->replica_location[ri] == dc1){
				(*opt->dag->g).myvertex[gv]->replica_location[ri] = dc2;
				break;
			}
		}
		if((*opt->dag->g).myvertex[gv]->master_location == dc1)
			tmp_master[gv] = dc2;
	}
#pragma omp parallel for
	for(int vi=0; vi<contained_vertices[1].size(); vi++){				
		int gv = contained_vertices[1][vi];
		opt->in_nbr_distribution[gv][dc2] = 0;
		opt->out_nbr_distribution[gv][dc2] = 0;
		
		for(int ri =0; ri < (*opt->dag->g).myvertex[gv]->replica_location.size(); ri++){
			if((*opt->dag->g).myvertex[gv]->replica_location[ri] == dc2){
				(*opt->dag->g).myvertex[gv]->replica_location[ri] = dc1;
				break;
			}
		}
		if((*opt->dag->g).myvertex[gv]->master_location == dc2)
			tmp_master[gv] = dc1;
	}
#pragma omp parallel for
	for(int vi=0; vi<contained_vertices[0].size(); vi++){
		int gv = contained_vertices[0][vi];
		(*opt->dag->g).myvertex[gv]->master_location = tmp_master[gv];
		std::sort((*opt->dag->g).myvertex[gv]->replica_location.begin(),(*opt->dag->g).myvertex[gv]->replica_location.end());
		std::vector<int>::iterator it = std::unique((*opt->dag->g).myvertex[gv]->replica_location.begin(),(*opt->dag->g).myvertex[gv]->replica_location.end());
		(*opt->dag->g).myvertex[gv]->replica_location.resize(std::distance((*opt->dag->g).myvertex[gv]->replica_location.begin(),it));
	}
#pragma omp parallel for
	for(int vi=0; vi<contained_vertices[1].size(); vi++){
		int gv = contained_vertices[1][vi];
		(*opt->dag->g).myvertex[gv]->master_location = tmp_master[gv];
		std::sort((*opt->dag->g).myvertex[gv]->replica_location.begin(),(*opt->dag->g).myvertex[gv]->replica_location.end());
		std::vector<int>::iterator it = std::unique((*opt->dag->g).myvertex[gv]->replica_location.begin(),(*opt->dag->g).myvertex[gv]->replica_location.end());
		(*opt->dag->g).myvertex[gv]->replica_location.resize(std::distance((*opt->dag->g).myvertex[gv]->replica_location.begin(),it));
	}		
	// switch edge distribution
	std::vector<int> tmp = opt->edge_distribution[dc1];
	opt->edge_distribution[dc1] = opt->edge_distribution[dc2];
	opt->edge_distribution[dc2] = tmp;
// #pragma omp parallel for
	for(int ei=0; ei<opt->edge_distribution[dc1].size(); ei++){
		int ecount = opt->edge_distribution[dc1][ei];
		//int src = source(opt->dag->random_access_edges[ecount], *opt->dag->g);
		int src = (*opt->dag->g).source_edge(ecount);
		//int tgt = target(opt->dag->random_access_edges[ecount], *opt->dag->g);	
		int tgt = (*opt->dag->g).target_edge(ecount);
		opt->in_nbr_distribution[tgt][dc1]++;
		opt->out_nbr_distribution[src][dc1]++;
	}
// #pragma omp parallel for
	for(int ei=0; ei<opt->edge_distribution[dc2].size(); ei++){
		int ecount = opt->edge_distribution[dc2][ei];
		//int src = source(opt->dag->random_access_edges[ecount], *opt->dag->g);
		int src = (*opt->dag->g).source_edge(ecount);
		//int tgt = target(opt->dag->random_access_edges[ecount], *opt->dag->g);	
		int tgt = (*opt->dag->g).target_edge(ecount);
		opt->in_nbr_distribution[tgt][dc2]++;
		opt->out_nbr_distribution[src][dc2]++;
	}	
	//debug
	for(int vi=0; vi<vertexsize; vi++){
		int loc_master = (*opt->dag->g).myvertex[vi]->master_location;
		if(std::find((*opt->dag->g).myvertex[vi]->replica_location.begin(),(*opt->dag->g).myvertex[vi]->replica_location.end(),loc_master)
		== (*opt->dag->g).myvertex[vi]->replica_location.end())
			//printf("ContexSwitch mess up the master location for vertex %d\n",vi);
			fileResult << "ContexSwitch mess up the master location  for vertext " << vi << "\n";
	}
}
/**
* GraphH: ICDCS'16
* 1. H-Load: initial partitioning
* 2. H-Move: distributed edge migrations
*/
void Optimizer::HPartition(){
	/******************* Step 1: H-Load **********************/
	// ignore the clustering process
	/** initial partitioning, load means the traffic */
	std::vector<int> num_unassigned_edge = std::vector<int>(dag->num_vertices);

	for(int vi=0; vi<dag->num_vertices; vi++){
		(*dag->g).myvertex[vi]->replica_location.clear();
		(*dag->g).myvertex[vi]->replica_location.push_back((*dag->g).myvertex[vi]->location_id);
		int nbrs;
		if(myapp->BiDirected)
			nbrs = dag->get_in_nbrs(vi).size() + dag->get_out_nbrs(vi).size();
		else nbrs = dag->get_in_nbrs(vi).size();
		num_unassigned_edge[vi] = nbrs;
	}
	//int numedges = num_edges(*dag->g);
	int numedges = dag->num_edges;
	edge_distribution = std::vector<std::vector<int> >(DCs.size());
	std::vector<int> loads = std::vector<int>(DCs.size(),0);
	for(int vi = 0; vi< dag->num_vertices; vi++){
		loads[(*dag->g).myvertex[vi]->location_id] ++; //+= num_unassigned_edge[vi];
	}
	omp_set_num_threads(N_THREADS);
	int numoflocks = dag->num_vertices;
	std::vector<omp_lock_t> writelock(numoflocks);
	for(int i=0; i<numoflocks; i++){
		omp_init_lock(&writelock[i]);
		// printf("initializing %d out of %d locks\n",i,numoflocks);
	}
	int numoflocks1 = DCs.size();
	std::vector<omp_lock_t> writelock1(numoflocks1);
	for(int i=0; i<numoflocks1; i++){
		omp_init_lock(&writelock1[i]);
	}
#pragma omp parallel for
	for(int ecount = 0; ecount < numedges; ecount++){
		//int src = source(dag->random_access_edges[ecount], *dag->g);
		int src = (*dag->g).source_edge(ecount);
		//int tgt = target(dag->random_access_edges[ecount], *dag->g);	
		int tgt = (*dag->g).target_edge(ecount);
		int threadid = omp_get_thread_num();
		int lockid1, lockid2;
		lockid1 = src<tgt?std::fmod(src,(double)numoflocks):std::fmod(tgt,(double)numoflocks);
		lockid2 = src>tgt?std::fmod(src,(double)numoflocks):std::fmod(tgt,(double)numoflocks);
		// int lockid = std::fmod(pairing_function(src,tgt),(double)numoflocks);
		// printf("thread %d acquiring lock %d and %d: %d-->%d\n",threadid,lockid1,lockid2,src,tgt);
        omp_set_lock(&writelock[lockid1]);	
		omp_set_lock(&writelock[lockid2]);		
		num_unassigned_edge[src] --;
		num_unassigned_edge[tgt] --;
		std::vector<int> src_replicas = (*dag->g).myvertex[src]->replica_location;
		std::vector<int> tgt_replicas = (*dag->g).myvertex[tgt]->replica_location;
		std::vector<int> intersection(src_replicas.size()+tgt_replicas.size());
		std::vector<int>::iterator it;
		std::sort(src_replicas.begin(),src_replicas.end());
		std::sort(tgt_replicas.begin(),tgt_replicas.end());
		it = std::set_intersection(src_replicas.begin(), src_replicas.end(), tgt_replicas.begin(), tgt_replicas.end(), intersection.begin());
		intersection.resize(it-intersection.begin()); 
		
		if(intersection.size() > 0){			
			int least_loaded = -1;
			float load = LARGE_FLOAT;
			for(int i=0; i<intersection.size(); i++){
				if(loads[intersection[i]] < load){
					least_loaded = intersection[i];
					load = loads[intersection[i]];
				}
			}
			if(least_loaded == -1){
				exit(1);
			}
			omp_set_lock(&writelock1[least_loaded]);
			edge_distribution[least_loaded].push_back(ecount);
			omp_unset_lock(&writelock1[least_loaded]);
		}else if(src_replicas.size() > 0 && tgt_replicas.size() > 0){
			std::vector<int> space = num_unassigned_edge[src] > num_unassigned_edge[tgt] ? (*dag->g).myvertex[src]->replica_location : (*dag->g).myvertex[tgt]->replica_location;
			float load = LARGE_FLOAT;
			int least_loaded = -1;
			for(int i=0; i<space.size(); i++){				
				if(loads[space[i]] < load){
					load = loads[space[i]];
					least_loaded = space[i];
				}
			}
			if(least_loaded == -1){
				exit(1);
			}
			omp_set_lock(&writelock1[least_loaded]);
			edge_distribution[least_loaded].push_back(ecount);			
			if(num_unassigned_edge[src] > num_unassigned_edge[tgt]){
				(*dag->g).myvertex[tgt]->replica_location.push_back(least_loaded);
				loads[least_loaded] ++;
			}else{
				(*dag->g).myvertex[src]->replica_location.push_back(least_loaded);
				loads[least_loaded] ++;
			}
			omp_unset_lock(&writelock1[least_loaded]);
		}else if(src_replicas.size() > 0 || tgt_replicas.size() > 0){
			std::vector<int> space = src_replicas.size()>0 ? (*dag->g).myvertex[src]->replica_location : (*dag->g).myvertex[tgt]->replica_location;
			float load = LARGE_FLOAT;
			int least_loaded = -1;
			for(int i=0; i<space.size(); i++){				
				if(loads[space[i]] < load){
					load = loads[space[i]];
					least_loaded = space[i];
				}
			}
			if(least_loaded == -1){
				exit(1);
			}
			omp_set_lock(&writelock1[least_loaded]);
			edge_distribution[least_loaded].push_back(ecount);		
			//update the replica_location 
			if(src_replicas.size()>0){
				//tgt is replicated
				(*dag->g).myvertex[tgt]->replica_location.push_back(least_loaded);
				loads[least_loaded] ++;
			}else{
				(*dag->g).myvertex[src]->replica_location.push_back(least_loaded);
				loads[least_loaded] ++;
			}
			omp_unset_lock(&writelock1[least_loaded]);
		}else {
			//std::cout << "this case should never happen \n";
			fileResult << "this case should never happen \n";
			float load = LARGE_FLOAT;
			int least_loaded = -1;
			for(int i=0; i<DCs.size(); i++){				
				if(loads[i] < load){
					load = loads[i];
					least_loaded = i;
				}
			}
			//edge_distribution[least_loaded].push_back(std::pair<int,int>(src,tgt));
			omp_set_lock(&writelock1[least_loaded]);
			edge_distribution[least_loaded].push_back(ecount);
			(*dag->g).myvertex[tgt]->replica_location.push_back(least_loaded);
			loads[least_loaded] ++;
			(*dag->g).myvertex[src]->replica_location.push_back(least_loaded);
			loads[least_loaded] ++;
			omp_unset_lock(&writelock1[least_loaded]);	
		}
		
		if(ecount % 1000000 == 0){
			//std::cout << "addressed " << ecount << "edges\n";
			fileResult << "addressed " << ecount << "edges\n";
		}
		omp_unset_lock(&writelock[lockid2]);		
		omp_unset_lock(&writelock[lockid1]);		
	}
	for(int i=0; i<numoflocks; i++)
		omp_destroy_lock(&writelock[i]);
	
	/*use the initial location as master*/
	#pragma omp parallel for
	for(int vi=0; vi<dag->num_vertices; vi++){
		(*dag->g).myvertex[vi]->master_location = (*dag->g).myvertex[vi]->location_id;
	}
	
	/**************************** partition mapping: minimizing overall communication cost ***************************/
	int vertexsize = dag->num_vertices;
	in_nbr_distribution = std::vector<std::vector<int> >(vertexsize,std::vector<int>(DCs.size(),0));
	out_nbr_distribution = std::vector<std::vector<int> >(vertexsize,std::vector<int>(DCs.size(),0));

#pragma omp parallel for
	for(int dc=0; dc<DCs.size(); dc++){
		for(int ei=0; ei<edge_distribution[dc].size(); ei++){
			//Edge e = dag->random_access_edges[edge_distribution[dc][ei]];
			//int src = source(e, *dag->g);
			int src = (*dag->g).source_edge(edge_distribution[dc][ei]);
			//int tgt = target(e, *dag->g);;
			int tgt = (*dag->g).target_edge(edge_distribution[dc][ei]);
			in_nbr_distribution[tgt][dc]++;
			out_nbr_distribution[src][dc]++;
		}
	}
	std::pair<float,float> currCost = commCost();
// #pragma omp parallel for 
	for(int iter=0; iter<100; iter++){
		int dc1 = (int)dag->rnd_generator(0,DCs.size());
		int dc2 = (int)dag->rnd_generator(0,DCs.size());
		while(dc2 == dc1)
			dc2 = (int)dag->rnd_generator(0,DCs.size());
		
		// int lockid1 = dc1<dc2?dc1:dc2;
		// int lockid2 = dc1>dc2?dc1:dc2;
		// omp_set_lock(&writelock1[lockid1]);
		// omp_set_lock(&writelock1[lockid2]);
		// if the wan budget allows
		float new_wan_usage = currCost.second + DCs[dc1]->data_size + DCs[dc2]->data_size;
		if(new_wan_usage <= myapp->budget){		
			//printf("checking dc %d and %d\n",dc1,dc2);
			fileResult << "checking dc " << dc1 << " and " << dc2 << " \n";
			// pretend exchanging partitions in dc1 and dc2
			ContextSwitch(dc1,dc2,this);			
			
			std::pair<float,float> newCost = commCost();
			// #pragma omp critical
			{
				if(newCost.first >= currCost.first){
					// context recover
					ContextSwitch(dc2,dc1,this);						
					float tmp_datasize = DCs[dc1]->data_size;
					DCs[dc1]->data_size = DCs[dc2]->data_size;
					DCs[dc2]->data_size = tmp_datasize;
				}else{
					//printf("exchange dc %d with dc %d, cost changed %.4f-->%.4f\n",dc1,dc2,currCost.first,newCost.first);
					fileResult << "exchange dc " << dc1 << " with dc " << dc2 << ", cost changed " << std::setprecision(4) << currCost.first << "-->" << std::setprecision(4) << newCost.first << "\n";
					currCost = newCost;				
				}
			}			
		}
		// omp_unset_lock(&writelock1[lockid1]);
		// omp_unset_lock(&writelock1[lockid2]);
	}
	for(int i=0; i<numoflocks1; i++)
		omp_destroy_lock(&writelock1[i]);	
	/**************************************** Step 2: H-Move *****************************************
	2:	m' ¡û selectPartner()
	3: b ¡û bagOfEdges(m)
	4: lock(b)
	5: b ¡û updateLocked(b)
	6: ¦¤c ¡û c+ ? c?
	7: if ¦¤c < 0 then
	8: migrateBag(b)  
	*************************************************************************************************/

	std::vector<std::vector<int> > contained_vertices = std::vector<std::vector<int> >(DCs.size());
	#pragma omp parallel for
	for(int dc = 0; dc < DCs.size(); dc++){
		std::vector<int> p_contained_vertices;
		for(int ei=0; ei<edge_distribution[dc].size(); ei++){
			int ecount = edge_distribution[dc][ei];
			//int src = source(dag->random_access_edges[ecount], *dag->g);
			int src = (*dag->g).source_edge(ecount);
			//int tgt = target(dag->random_access_edges[ecount], *dag->g);	
			int tgt = (*dag->g).target_edge(ecount);
			p_contained_vertices.push_back(src);
			p_contained_vertices.push_back(tgt);			
		}
		std::sort(p_contained_vertices.begin(),p_contained_vertices.end());
		std::vector<int>::iterator it = std::unique(p_contained_vertices.begin(),p_contained_vertices.end());
		p_contained_vertices.resize(std::distance(p_contained_vertices.begin(),it));
		contained_vertices[dc]=p_contained_vertices;
	}
		
	for(int dc = 0; dc < DCs.size(); dc++){
		std::vector<std::pair<int,float> > partners;
		std::vector<std::pair<int,int> > bag_of_edges;
		
		for(int p=0; p<DCs.size(); p++){
			if(p!=dc){
				float traffic = 0;			
				for(int i=0; i<contained_vertices[dc].size(); i++){
					int gv = contained_vertices[dc][i];
					if((*dag->g).myvertex[gv]->master_location == p){
						// upload gather, download apply
						if(myapp->BiDirected)
							traffic += (in_nbr_distribution[gv][dc]+out_nbr_distribution[gv][dc])*myapp->msg_size;//*myapp->red_rate;
						else traffic += in_nbr_distribution[gv][dc]*myapp->msg_size;//*myapp->red_rate;
						traffic += myapp->msg_size;
					}
				}
				for(int i=0; i<contained_vertices[p].size(); i++){
					int gv = contained_vertices[p][i];
					if((*dag->g).myvertex[gv]->master_location == dc){
						// upload gather, download apply
						if(myapp->BiDirected)
							traffic += (in_nbr_distribution[gv][p]+out_nbr_distribution[gv][p])*myapp->msg_size;//*myapp->red_rate;
						else traffic += in_nbr_distribution[gv][p]*myapp->msg_size;//*myapp->red_rate;
						traffic += myapp->msg_size;
					} 
				}
				partners.push_back(std::pair<int,float>(p,traffic));
			}			
		}
		std::sort(partners.begin(),partners.end(),mycompare);
		//printf("partners for %d\n",dc);
		fileResult << "partners for " << dc << "\n";
		for(int pi=0; pi<partners.size(); pi++)
			//printf("%d, %f\n",partners[pi].first, partners[pi].second);
			fileResult << partners[pi].first  << "," << partners[pi].second << "\n";
		int partner_dc = -1;
		bool migrated = false;
		while(!migrated && !partners.empty()){
			partner_dc = partners.front().first;
			partners.erase(partners.begin());			
			if(partners.front().second > 0){	
				int max_size = 0;
				float capacity = (DCs[dc]->data_size - DCs[partner_dc]->data_size)/2.0;				
				if(capacity <= 0){					
					continue;
				}				
				std::vector<int> intersection(contained_vertices[partner_dc].size()+contained_vertices[dc].size());
				std::vector<int>::iterator it;
				std::sort(contained_vertices[partner_dc].begin(),contained_vertices[partner_dc].end());
				std::sort(contained_vertices[dc].begin(),contained_vertices[dc].end());
				it = std::set_intersection(contained_vertices[dc].begin(), contained_vertices[dc].end(), contained_vertices[partner_dc].begin(),contained_vertices[partner_dc].end(), intersection.begin());
				intersection.resize(it-intersection.begin()); 
				if(intersection.size() > 0){
					//printf("select %d as partner dc of dc %d, capacity: %f\n",partner_dc,dc,capacity);
					fileResult << "select " << partner_dc << " as partner dc of dc " << dc <<", capacity: " << capacity << "\n";
					//sort candidates according to their traffic
					std::vector<std::pair<int,float> > candidates(intersection.size());
					for(int i=0; i<intersection.size(); i++){
						int v = intersection[i];
						candidates[i].first = v;
						float load = in_nbr_distribution[v][dc] + out_nbr_distribution[v][dc];
						candidates[i].second = load*myapp->msg_size;//*myapp->red_rate;
					}
					std::sort(candidates.begin(),candidates.end(),mycompare);	
					int candi_size = candidates.size();
					if(candi_size > 100){
						//select the top 10-percent
						candi_size = std::ceil(candi_size*0.1);
						candi_size = candi_size>100?100:candi_size;
						candidates.resize(candi_size);
					}
					//printf("candidate vertices to move\n");
					fileResult << "candidate vertices to move\n";
					// for(int ci=0; ci<candidates.size(); ci++)
						// printf("%d, %f\n",candidates[ci].first, candidates[ci].second);
					int leftovercap = capacity;
					while(leftovercap > 0 && !candidates.empty()){
						//check if removing v and all its adj edges can reduce cost
						//pretend migrate all connected edges to partner_dc
						//change master, in/out nbrs, edge distribution and contained vertices	
						int v_to_rm = candidates.front().first;
						// printf("remove vertex %d\n",v_to_rm);
						int in_num = in_nbr_distribution[v_to_rm][dc];
						int out_num = out_nbr_distribution[v_to_rm][dc];
						std::vector<int> connected_edges;
						for(int ei=0; ei<edge_distribution[dc].size(); ei++){
							int ecount = edge_distribution[dc][ei];
							//int src = source(dag->random_access_edges[ecount], *dag->g);
							int src = (*dag->g).source_edge(ecount);
							//int tgt = target(dag->random_access_edges[ecount], *dag->g);
							int tgt = (*dag->g).target_edge(ecount);
							if(src == v_to_rm || tgt == v_to_rm){
								connected_edges.push_back(edge_distribution[dc][ei]);	
								edge_distribution[partner_dc].push_back(edge_distribution[dc][ei]);
								edge_distribution[dc].erase(edge_distribution[dc].begin()+ei);							
								ei --;
							}						
						}
						std::vector<int> orig_master;						
						std::vector<int> orig_in_nbr_dc;
						std::vector<int> orig_in_nbr_partner;
						std::vector<int> orig_out_nbr_dc;
						std::vector<int> orig_out_nbr_partner;
						std::vector<std::vector<int> > orig_replica;
						std::vector<int> orig_contained_dc = contained_vertices[dc];
						std::vector<int> orig_contained_partner = contained_vertices[partner_dc];
						for(int ei=0; ei < connected_edges.size(); ei++){
							int ecount = connected_edges[ei];
							//int src = source(dag->random_access_edges[ecount], *dag->g);
							int src = (*dag->g).source_edge(ecount);
							//int tgt = target(dag->random_access_edges[ecount], *dag->g);
							int tgt = (*dag->g).target_edge(ecount);
							int node = src==v_to_rm? tgt : src;
							orig_master.push_back((*dag->g).myvertex[node]->master_location);
							orig_in_nbr_dc.push_back(in_nbr_distribution[node][dc]);
							orig_in_nbr_partner.push_back(in_nbr_distribution[node][partner_dc]);
							orig_out_nbr_dc.push_back(out_nbr_distribution[node][dc]);
							orig_out_nbr_partner.push_back(out_nbr_distribution[node][partner_dc]);
							orig_replica.push_back((*dag->g).myvertex[node]->replica_location);
							if(src == v_to_rm ){								
								in_nbr_distribution[tgt][dc] --;
								in_nbr_distribution[tgt][partner_dc] ++;
																
							}else{
								out_nbr_distribution[src][dc] --;
								out_nbr_distribution[src][partner_dc] ++;																
							} 
							if(std::find((*dag->g).myvertex[node]->replica_location.begin(),(*dag->g).myvertex[node]->replica_location.end(),
								partner_dc) == (*dag->g).myvertex[node]->replica_location.end()){
								(*dag->g).myvertex[node]->replica_location.push_back(partner_dc);
								contained_vertices[partner_dc].push_back(node);
							}
							if(in_nbr_distribution[node][dc]==0 && out_nbr_distribution[node][dc]==0){
								for(int vi=0; vi<contained_vertices[dc].size(); vi++){
									if(contained_vertices[dc][vi] == node){
										contained_vertices[dc].erase(contained_vertices[dc].begin()+vi);
										break;
									}
								}
								if((*dag->g).myvertex[node]->master_location == dc)
									(*dag->g).myvertex[node]->master_location = partner_dc;
							}								
						}
						orig_master.push_back((*dag->g).myvertex[v_to_rm]->master_location);
						orig_in_nbr_dc.push_back(in_nbr_distribution[v_to_rm][dc]);
						orig_in_nbr_partner.push_back(in_nbr_distribution[v_to_rm][partner_dc]);
						orig_out_nbr_dc.push_back(out_nbr_distribution[v_to_rm][dc]);
						orig_out_nbr_partner.push_back(out_nbr_distribution[v_to_rm][partner_dc]);
						orig_replica.push_back((*dag->g).myvertex[v_to_rm]->replica_location);
						if((*dag->g).myvertex[v_to_rm]->master_location == dc)
							(*dag->g).myvertex[v_to_rm]->master_location = partner_dc;
						in_nbr_distribution[v_to_rm][dc] = 0;
						out_nbr_distribution[v_to_rm][dc] = 0;					
						for(int vi=0; vi<contained_vertices[dc].size(); vi++){
							if(contained_vertices[dc][vi] == v_to_rm){
								contained_vertices[dc].erase(contained_vertices[dc].begin()+vi);
								break;
							}
						}
						for(int di=0; di<(*dag->g).myvertex[v_to_rm]->replica_location.size(); di++){
							if((*dag->g).myvertex[v_to_rm]->replica_location[di] == dc){
								(*dag->g).myvertex[v_to_rm]->replica_location.erase((*dag->g).myvertex[v_to_rm]->replica_location.begin()+di);
								break;
							}
						}
						
						std::pair<float,float> newCost = commCost();
						if(newCost.first >= currCost.first){
							//recover context	
							for(int ei=0; ei<connected_edges.size(); ei++){
								edge_distribution[dc].push_back(connected_edges[ei]);	
								for(int i=0; i<edge_distribution[partner_dc].size(); i++){
									if(edge_distribution[partner_dc][i] == connected_edges[ei]){
										edge_distribution[partner_dc].erase(edge_distribution[partner_dc].begin()+i);									
										break;
									}
								}
							}
							for(int ei=0; ei<connected_edges.size(); ei++){
								//int src = source(dag->random_access_edges[connected_edges[ei]], *dag->g);
								int src = (*dag->g).source_edge(connected_edges[ei]);
								//int tgt = target(dag->random_access_edges[connected_edges[ei]], *dag->g);
								int tgt = (*dag->g).target_edge(connected_edges[ei]);
								int node = (src == v_to_rm? tgt:src);
								(*dag->g).myvertex[node]->master_location = orig_master[ei];
								in_nbr_distribution[node][dc] = orig_in_nbr_dc[ei];
								in_nbr_distribution[node][partner_dc] = orig_in_nbr_partner[ei];
								out_nbr_distribution[node][dc] = orig_out_nbr_dc[ei];
								out_nbr_distribution[node][partner_dc] = orig_out_nbr_partner[ei];
								(*dag->g).myvertex[node]->replica_location = orig_replica[ei];
							}
							(*dag->g).myvertex[v_to_rm]->master_location = orig_master[connected_edges.size()];
							in_nbr_distribution[v_to_rm][dc] = orig_in_nbr_dc[connected_edges.size()];
							in_nbr_distribution[v_to_rm][partner_dc] = orig_in_nbr_partner[connected_edges.size()];
							out_nbr_distribution[v_to_rm][dc] = orig_out_nbr_dc[connected_edges.size()];
							out_nbr_distribution[v_to_rm][partner_dc] = orig_out_nbr_partner[connected_edges.size()];
							(*dag->g).myvertex[v_to_rm]->replica_location = orig_replica[connected_edges.size()];
							contained_vertices[dc] = orig_contained_dc;
							contained_vertices[partner_dc] = orig_contained_partner;							
						}else{		
							//printf("cost reduced from %f to %f\n",currCost.first,newCost.first);
							fileResult << "cost reduced from " << currCost.first << " to " << newCost.first << "\n";
							currCost = newCost;						
							leftovercap -= candidates.front().second;		
							migrated = true;
						}
						candidates.erase(candidates.begin());	
					}
				}	
			}
		}
	}
	for(int v=0; v<dag->num_vertices; v++){
		std::sort((*dag->g).myvertex[v]->replica_location.begin(),(*dag->g).myvertex[v]->replica_location.end());
		std::vector<int>::iterator it = std::unique((*dag->g).myvertex[v]->replica_location.begin(),(*dag->g).myvertex[v]->replica_location.end());
		(*dag->g).myvertex[v]->replica_location.resize(std::distance((*dag->g).myvertex[v]->replica_location.begin(),it));
		int master = (*dag->g).myvertex[v]->master_location;
		if(master == -1){
			(*dag->g).myvertex[v]->master_location = (*dag->g).myvertex[v]->location_id;
		}
		in_nbr_distribution[v].clear();
		out_nbr_distribution[v].clear();
	}
	in_nbr_distribution.clear();
	out_nbr_distribution.clear();
}

