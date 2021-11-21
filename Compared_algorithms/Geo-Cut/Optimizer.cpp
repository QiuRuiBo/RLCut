#include "Simulator.h"
#include <memory.h>
#include<time.h>
#include<iterator>
#include<algorithm>
#include<vector>
#include <omp.h>
#include <set>
using namespace std;
/**
* the expected number of replicas for a vertex
* can be fixed by a user (we use 4 as an example)
* or calculated using methods in PowerGraph paper
*/
extern int N_THREADS;
extern ofstream fileResult;

/** a struct **/
/*struct trafficStruct{
	float apply_download;
	float apply_upload;
	float gather_download;
	float gather_upload;
};
 */

// ***************************** Heterogeneity-aware Partitioning ***************************** //
bool mycompare(std::pair<int, float> a, std::pair<int, float> b) {
	return a.second > b.second;
}

void Optimizer::InputDataMovement(){
	int vertexsize = dag->num_vertices;
	int hot_threshold = vertexsize*0.1; //0.1 chosen randomly
	/** Step 1: vertex clustering **/
	std::vector<std::pair<int,float> > v_to_move;
	for(int i=0; i< vertexsize; i++){
		int tmp_degree = dag->get_in_nbrs(i).size() + dag->get_out_nbrs(i).size();		
		if(tmp_degree > hot_threshold){
			v_to_move.push_back(std::pair<int,float>(i,0));
		}
	}
	/** Step 2: calculate the value for each cluster */
	for(int i=0; i<v_to_move.size(); i++){

	}	
	std::sort(v_to_move.begin(),v_to_move.end(),mycompare);
}

void Optimizer::PartitionPlacement(){
	
	/**
	* Step 1: identify the bottlenecked dcs in gather and apply stages
	*/
	int gather_btlneck = -1;
	int apply_btlneck = -1;
	int gather_link = -1;
	int apply_link = -1;
	int vertexsize = dag->num_vertices;
	in_nbr_distribution = std::vector<std::vector<int> >(vertexsize,std::vector<int>(DCs.size(),0));
	out_nbr_distribution = std::vector<std::vector<int> >(vertexsize,std::vector<int>(DCs.size(),0));

	omp_set_num_threads(N_THREADS);
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

	std::vector<int> tmp_master = std::vector<int>(vertexsize,-1);
#pragma omp parallel for
	for(int i=0; i<vertexsize; i++){
		tmp_master[i] = (*dag->g).myvertex[i]->master_location;
	}
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
		for(int vi=0; vi<vertexsize; vi++){
			if((*dag->g).myvertex[vi]->location_id == dc)
				p_contained_vertices.push_back(vi);
		}
		std::sort(p_contained_vertices.begin(),p_contained_vertices.end());
		std::vector<int>::iterator it = std::unique(p_contained_vertices.begin(),p_contained_vertices.end());
		p_contained_vertices.resize(std::distance(p_contained_vertices.begin(),it));
		contained_vertices[dc]=p_contained_vertices;
	}
	bool cont = false;
	do{
		float cur_wan_usage = 0.0;
		float gather_max_time = 0.0;
		float apply_max_time = 0.0;
		
		#pragma omp parallel for
		for(int dc=0; dc<DCs.size(); dc++){
			DCs[dc]->g_dnload_data = 0.0;
			DCs[dc]->g_upload_data = 0.0;
			DCs[dc]->a_dnload_data = 0.0;
			DCs[dc]->a_upload_data = 0.0;	
			DCs[dc]->data_size = 0.0;			
			for(int vi=0; vi<contained_vertices[dc].size(); vi++){
				//gather: master download, non-master upload
				//apply: master upload, non-master download
				int gv = contained_vertices[dc][vi];
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
		}//end for each dc
		for(int dc=0; dc<DCs.size(); dc++){
			cur_wan_usage += DCs[dc]->g_upload_data + DCs[dc]->a_upload_data;
			float g_upload_time = DCs[dc]->g_upload_data / DCs[dc]->upload_band;
			float g_dnload_time = DCs[dc]->g_dnload_data / DCs[dc]->download_band;
			float a_upload_time = DCs[dc]->a_upload_data / DCs[dc]->upload_band;
			float a_dnload_time = DCs[dc]->a_dnload_data / DCs[dc]->download_band;
			if(g_upload_time > gather_max_time){
				gather_max_time = g_upload_time;
				gather_btlneck = dc;
				gather_link = 0;
			}
			if(g_dnload_time > gather_max_time){
				gather_max_time = g_dnload_time;
				gather_btlneck = dc;
				gather_link = 1;
			}		
			if(a_upload_time > apply_max_time){
				apply_max_time = a_upload_time;
				apply_btlneck = dc;
				apply_link = 0;
			}
			if(a_dnload_time > apply_max_time){
				apply_max_time = a_dnload_time;
				apply_btlneck = dc;
				apply_link = 1;
			}
		}
	
		//debug
		//std::cout<<"step 1 of parition placement is done." <<std::endl;
		/**
		* Step 2: move graph partitions out of bottlenecked dcs 
		*/
		//std::cout << "bottlenecked datacenters for gather and apply: " << gather_btlneck << ",  "  << apply_btlneck << std::endl;
		cout << "bottlenecked datacenters for gather and apply: " << gather_btlneck << ",  "  << apply_btlneck << "\n";
			  
		std::vector<int> alldcs = std::vector<int>(DCs.size());
		for(int dc=0; dc<DCs.size(); dc++){
			alldcs[dc] = dc;
		}
		int dc_to_switch = -1;
		int btlneck = -1;
		if(gather_btlneck == apply_btlneck){		
			std::pair<float,int> gain = EstimateGain(gather_btlneck,alldcs);
			//std::cout << "switch to datacenter: " << gain.second << ", " << gain.first << std::endl;
			cout << "switch to datacenter: " << gain.second << ", " << gain.first << "\n";
			if(gain.second != -1){
				float new_wan_usage = cur_wan_usage + DCs[gain.second]->data_size + DCs[gather_btlneck]->data_size;
				if(new_wan_usage <= myapp->budget){
					dc_to_switch = gain.second;
					btlneck = apply_btlneck;
					//switch gather_btlneck and gain.second
					//std::cout << "switching datacenter " << gather_btlneck << " with " << gain.second << std::endl;
					cout << "switching datacenter " << gather_btlneck << " with " << gain.second << "\n";
				}
			}
		}else{
			std::pair<float,int> gain1 = EstimateGain(gather_btlneck,alldcs);
			std::pair<float,int> gain2 = EstimateGain(apply_btlneck,alldcs);
			/*std::cout << "switch to datacenters: " << gain1.second << ", " << gain1.first
				<< ";	" << gain2.second << ", " << gain2.first << std::endl; */
			cout << "switch to datacenters: " << gain1.second << ", " << gain1.first
				<< ";	" << gain2.second << ", " << gain2.first << "\n";
			int larger_dc = gain1.first > gain2.first ? gain1.second : gain2.second;
			if(larger_dc != -1){
				btlneck = gain1.first > gain2.first ? gather_btlneck : apply_btlneck;
				float new_wan_usage = cur_wan_usage + DCs[larger_dc]->data_size + DCs[btlneck]->data_size;
				if(new_wan_usage <= myapp->budget){
					//switch btlneck and dc_to_switch
					dc_to_switch = larger_dc;
					//std::cout << "switching datacenter " << btlneck << " with " << dc_to_switch << std::endl;
					cout << "switching datacenter " << btlneck << " with " << dc_to_switch << "\n";
				}
			}
		}
		if(dc_to_switch != -1 && btlneck != -1){
			//update vertex replica location and in/out nbrs
		#pragma omp parallel for
			for(int vi=0; vi<contained_vertices[btlneck].size(); vi++){
				int gv = contained_vertices[btlneck][vi];
				in_nbr_distribution[gv][btlneck] = 0;
				out_nbr_distribution[gv][btlneck] = 0;
				for(int ri =0; ri < (*dag->g).myvertex[gv]->replica_location.size(); ri++){
					if((*dag->g).myvertex[gv]->replica_location[ri] == btlneck){
						(*dag->g).myvertex[gv]->replica_location[ri] = dc_to_switch;
						break;
					}
				}
				if((*dag->g).myvertex[gv]->master_location == btlneck)
					tmp_master[gv] = dc_to_switch;
			}
		#pragma omp parallel for
			for(int vi=0; vi<contained_vertices[dc_to_switch].size(); vi++){				
				int gv = contained_vertices[dc_to_switch][vi];
				in_nbr_distribution[gv][dc_to_switch] = 0;
				out_nbr_distribution[gv][dc_to_switch] = 0;
				for(int ri =0; ri < (*dag->g).myvertex[gv]->replica_location.size(); ri++){
					if((*dag->g).myvertex[gv]->replica_location[ri] == dc_to_switch){
						(*dag->g).myvertex[gv]->replica_location[ri] = btlneck;
						break;
					}
				}
				if((*dag->g).myvertex[gv]->master_location == dc_to_switch)
					tmp_master[gv] = btlneck;
			}
		#pragma omp parallel for
			for(int vi=0; vi<contained_vertices[btlneck].size(); vi++){
				int gv = contained_vertices[btlneck][vi];
				(*dag->g).myvertex[gv]->master_location = tmp_master[gv];
			}
		#pragma omp parallel for
			for(int vi=0; vi<contained_vertices[dc_to_switch].size(); vi++){
				int gv = contained_vertices[dc_to_switch][vi];
				(*dag->g).myvertex[gv]->master_location = tmp_master[gv];
			}		
			std::vector<int> tmp = edge_distribution[btlneck];
			edge_distribution[btlneck] = edge_distribution[dc_to_switch];
			edge_distribution[dc_to_switch] = tmp;
			for(int ei=0; ei<edge_distribution[btlneck].size(); ei++){
				int ecount = edge_distribution[btlneck][ei];
				//int src = source(dag->random_access_edges[ecount], *dag->g);
				int src = (*dag->g).source_edge(ecount);
				//int tgt = target(dag->random_access_edges[ecount], *dag->g);
				int tgt = (*dag->g).target_edge(ecount);
				in_nbr_distribution[tgt][btlneck]++;
				out_nbr_distribution[src][btlneck]++;
			}
			for(int ei=0; ei<edge_distribution[dc_to_switch].size(); ei++){
				int ecount = edge_distribution[dc_to_switch][ei];
				//int src = source(dag->random_access_edges[ecount], *dag->g);
				int src = (*dag->g).source_edge(ecount);
				//int tgt = target(dag->random_access_edges[ecount], *dag->g);
				int tgt = (*dag->g).target_edge(ecount);
				in_nbr_distribution[tgt][dc_to_switch]++;
				out_nbr_distribution[src][dc_to_switch]++;
			}
			float tmp_data_size = DCs[btlneck]->data_size;
			DCs[btlneck]->data_size = DCs[dc_to_switch]->data_size;
			DCs[dc_to_switch]->data_size = tmp_data_size;			
			tmp = contained_vertices[btlneck];
			contained_vertices[btlneck] = contained_vertices[dc_to_switch];
			contained_vertices[dc_to_switch] = tmp;
			// ContextSwitch(btlneck,dc_to_switch,DCs);
			cont = true;
		}else {
			cont = false;				
		}		
	}while(cont);

	for(int v=0; v<dag->num_vertices; v++){
		int master = (*dag->g).myvertex[v]->master_location;
		if(master == -1){
			(*dag->g).myvertex[v]->master_location = (*dag->g).myvertex[v]->location_id;
		}
		if(std::find((*dag->g).myvertex[v]->replica_location.begin(),(*dag->g).myvertex[v]->replica_location.end(),master) == (*dag->g).myvertex[v]->replica_location.end()){
			//printf("vertex %d master location error in placement\n",v);
			cout << "vertex " << v << " master location error in placement\n";
		}
		in_nbr_distribution[v].clear();
		out_nbr_distribution[v].clear();
	}
	in_nbr_distribution.clear();
	out_nbr_distribution.clear();
}

/** 
* Step 3: migrate edges from bottlenecked datacenters to other locations
* partition replacement cannot further improve performance
* given the bottlenecked dcs and links, move edges out of the bottlenecks
*/
void Optimizer::EdgeMigration(bool cost){
	time_t time_start ,time_end;
	float durationtime;
	omp_set_num_threads(N_THREADS);
//	omp_set_num_threads(1);
	//bool cont = true;
	int  total_migrated = 0;
	std::vector<int> num_migrated = std::vector<int>(N_THREADS,0);
//	std::vector<int> num_migrated = std::vector<int>(1,0);
	int suc = 0;
	int vcount = 0;
	int numiteration = 0;
	int numoflocks = dag->num_vertices;
	std::vector<omp_lock_t> writelock(numoflocks);
	for(int i =0; i<numoflocks; i++)
		omp_init_lock(&writelock[i]);
	int numoflocks1 = DCs.size();
	std::vector<omp_lock_t> writelock1(numoflocks1);
	for(int i=0; i<numoflocks1; i++)
		omp_init_lock(&writelock1[i]);
	while(vcount < 300 && numiteration < 500){
		//	if(true){
		clock_t t_start,t_end;
		double duration;
		t_start = clock();
		time(&time_start);
		numiteration++;
		float gather_max_time = 0.0;
		float apply_max_time = 0.0;
		float gather_sec_max = 0.0;
		float apply_sec_max = 0.0;
		int gather_btlneck = -1;
		int apply_btlneck = -1;
		int gather_link = -1;
		int apply_link = -1;
		
		/**
		* Step 1: Identify bottlenecks
		*/
		std::vector<std::vector<int> > contained_vertices = std::vector<std::vector<int> >(DCs.size());
		in_nbr_distribution = std::vector<std::vector<int> >(dag->num_vertices,std::vector<int>(DCs.size(),0));
		out_nbr_distribution = std::vector<std::vector<int> >(dag->num_vertices,std::vector<int>(DCs.size(),0));
	#pragma omp parallel for
		for(int dc = 0; dc < DCs.size(); dc++){
			int *array=new int[dag->num_vertices+1];
			memset(array,0,(dag->num_vertices+1)*sizeof(int));
			std::vector<int> p_contained_vertices;
		//	std::set<int> p_contained_vertices_set;
			for(int ei=0; ei<edge_distribution[dc].size(); ei++){
				int ecount = edge_distribution[dc][ei];
				//int src = source(dag->random_access_edges[ecount], *dag->g);
				int src = (*dag->g).source_edge(ecount);
				//int tgt = target(dag->random_access_edges[ecount], *dag->g);	
				int tgt = (*dag->g).target_edge(ecount);
	//			p_contained_vertices.push_back(src);  //shenbk
				array[src] = 1;
//				p_contained_vertices_set.insert(src);
	//			p_contained_vertices.push_back(tgt);
				array[tgt] = 1;
//				p_contained_vertices_set.insert(tgt);
				in_nbr_distribution[tgt][dc]++;
				out_nbr_distribution[src][dc]++;				
			}
			for(int vi=0; vi<dag->num_vertices; vi++){
				if((*dag->g).myvertex[vi]->location_id == dc)
					//p_contained_vertices.push_back(vi);
		;//shenbk			array[vi] = 1;
				//	p_contained_vertices_set.insert(vi);
			}
		//	std::sort(p_contained_vertices.begin(),p_contained_vertices.end());
		//	std::vector<int>::iterator it = std::unique(p_contained_vertices.begin(),p_contained_vertices.end());
		//	p_contained_vertices.resize(std::distance(p_contained_vertices.begin(),it));
		//	std::copy(p_contained_vertices_set.begin(),p_contained_vertices_set.end(),std::back_inserter(p_contained_vertices));
/*			std::set<int> p_contained_vertices_set(p_contained_vertices.begin(),p_contained_vertices.end());
			p_contained_vertices.assign(p_contained_vertices_set.begin(),p_contained_vertices_set.end());  */  //shenbk
			for(int iarray=0; iarray<dag->num_vertices+1; iarray++)
				if(array[iarray]==1)
					p_contained_vertices.push_back(iarray);
			contained_vertices[dc]=p_contained_vertices;
		 	delete []array;
		}
		#pragma omp parallel for
		for(int dc=0; dc<DCs.size(); dc++){
			DCs[dc]->g_dnload_data = DCs[dc]->g_upload_data = DCs[dc]->a_dnload_data= DCs[dc]->a_upload_data =0.0;
			for(int vi=0; vi<contained_vertices[dc].size(); vi++){
				//gather: master download, non-master upload
				//apply: master upload, non-master download
				int gv = contained_vertices[dc][vi];
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
					}else {
						//gather neighbor: all neighbors
						DCs[dc]->g_upload_data += (in_nbr_distribution[gv][dc] + out_nbr_distribution[gv][dc]) * myapp->msg_size;// * myapp->red_rate;							
					}
					DCs[dc]->a_dnload_data += myapp->msg_size;
				}
				DCs[dc]->data_size += (*dag->g).myvertex[gv]->data->input_size;
			}//end for each vi			
		}//end for each dc
		for(int dc=0; dc<DCs.size(); dc++){
			float g_upload_time = DCs[dc]->g_upload_data / DCs[dc]->upload_band;
			float g_dnload_time = DCs[dc]->g_dnload_data / DCs[dc]->download_band;
			float a_upload_time = DCs[dc]->a_upload_data / DCs[dc]->upload_band;
			float a_dnload_time = DCs[dc]->a_dnload_data / DCs[dc]->download_band;
			if(g_upload_time > gather_max_time){
				gather_max_time = g_upload_time;
				gather_btlneck = dc;
				gather_link = 0;
			}
			if(g_dnload_time > gather_max_time){
				gather_max_time = g_dnload_time;
				gather_btlneck = dc;
				gather_link = 1;
			}		
			if(g_upload_time < gather_max_time && g_upload_time > gather_sec_max)
				gather_sec_max = g_upload_time;
			if(g_dnload_time < gather_max_time && g_dnload_time > gather_sec_max)
				gather_sec_max = g_dnload_time;
			if(a_upload_time > apply_max_time){
				apply_max_time = a_upload_time;
				apply_btlneck = dc;
				apply_link = 0;
			}
			if(a_dnload_time > apply_max_time){
				apply_max_time = a_dnload_time;
				apply_btlneck = dc;
				apply_link = 1;
			}
			if(a_upload_time < apply_max_time && a_upload_time > apply_sec_max)
				apply_sec_max = a_upload_time;
			if(a_dnload_time < apply_max_time && a_dnload_time > apply_sec_max)
				apply_sec_max = a_dnload_time;
		}	
		//printf("****************************edge migration step****************************\n");
		//cout << "****************************edge migration step****************************\n";
		//printf("gather btlneck: dc %d link %d\n", gather_btlneck, gather_link);
		//cout << "gather btlneck: dc" << gather_btlneck << " link " << gather_link << "\n";
		//printf("apply btlneck: dc %d link %d\n", apply_btlneck, apply_link);
		//cout << "apply btlneck: dc " << apply_btlneck << " link " << apply_link << "\n";
		//printf("gather max and sec max: %f, %f\n",gather_max_time,gather_sec_max);
		//cout << "gather max and sec max:" << gather_max_time << ", " << gather_sec_max << "\n"; 
		//printf("apply max and sec max: %f, %f\n",apply_max_time,apply_sec_max);
		//cout << "apply max and sec max: " << apply_max_time << ", " << apply_sec_max << "\n";
		
		if(gather_btlneck == -1 && apply_btlneck == -1)
			break;
	
		std::vector<std::pair<int,float> > masters;
		std::vector<std::pair<int,float> > mirrors;
		std::vector<std::pair<int,float> > priorities;
		int btldc = -1;
		int btllink = -1;
		//bound on the same dc and same link
/*		t_end = clock();
		duration = (double)(t_end-t_start)/CLOCKS_PER_SEC;
		std::cout << "pre-one stage: " << duration << std::endl;
		fileResult << " pre-one stage: " << duration << "\n";
		t_start = clock();*/
		if(gather_btlneck == apply_btlneck && gather_link == apply_link){			
			btldc = gather_btlneck;
			btllink = gather_link;	
			
			for(int i=0; i<contained_vertices[btldc].size(); i++){
				int vid = contained_vertices[btldc][i];
				if((*dag->g).myvertex[vid]->master_location == btldc){
					masters.push_back(std::pair<int,float> (vid,0.0));
				}else{
					mirrors.push_back(std::pair<int,float> (vid,0.0));
				}
			}			
							
			if(btllink == 0){
				// uplink
				// estimate the gain of the gather stage, mirror replicas
				for(int i=0; i< mirrors.size(); i++){
					if(!myapp->BiDirected){
						//gather neighbor: in_edges					
						mirrors[i].second = in_nbr_distribution[mirrors[i].first][btldc] * myapp->msg_size;// * myapp->red_rate;					
					}else{
						//gather neighbor: all neighbors
						mirrors[i].second = (in_nbr_distribution[mirrors[i].first][btldc] + out_nbr_distribution[mirrors[i].first][btldc]) * myapp->msg_size;// * myapp->red_rate;							
					}
					priorities.push_back(mirrors[i]);
				}				
				// estimate the gain of apply stage, master replicas
				for(int i=0; i<masters.size();i++){
					masters[i].second = myapp->msg_size * ((*dag->g).myvertex[masters[i].first]->replica_location.size() - 1);
					priorities.push_back(masters[i]);
				}										
			}else if(btllink == 1){
				//downlink
				// estimate the gain of the gather stage, master replicas
				#pragma omp parallel for
				for(int i=0; i< masters.size(); i++){
					if(!myapp->BiDirected){
						//gather neighbor: in_edges
						for(int di=0; di<DCs.size(); di++){
							if(btldc != di){
								masters[i].second += in_nbr_distribution[masters[i].first][di] * myapp->msg_size;// * myapp->red_rate;
							}
						}					
					}else{
						//gather neighbor: all neighbors
						for(int di=0; di<DCs.size(); di++){
							if(btldc != di){
								masters[i].second += (in_nbr_distribution[masters[i].first][di] + out_nbr_distribution[masters[i].first][di]) 
									* myapp->msg_size;// * myapp->red_rate;							
							}
						}
					}	
					#pragma omp critical					
					priorities.push_back(masters[i]);
				}				
				// estimate the gain of apply stage, mirror replicas
				for(int i=0; i<mirrors.size();i++){
					mirrors[i].second = myapp->msg_size;
					priorities.push_back(mirrors[i]);
				}						
			}else{
				//printf("no bottleneck link?\n");
				cout << "no bottleneck link?\n";
				exit(1);
			}		
		}
		else{
			//bound on different links, select one link to resolve
			float gather_improve = gather_max_time - gather_sec_max;
			float apply_improve = apply_max_time - apply_sec_max;
			if(gather_improve > apply_improve){
				btldc = gather_btlneck;
				btllink = gather_link;
				for(int i=0; i<contained_vertices[btldc].size(); i++){
					int vid = contained_vertices[btldc][i];
					if((*dag->g).myvertex[vid]->master_location == btldc){
						masters.push_back(std::pair<int,float> (vid,0.0));
					}else{
						mirrors.push_back(std::pair<int,float> (vid,0.0));
					}
				}
				if(gather_link == 0){
					//uplink
					#pragma omp parallel for
					for(int i=0; i< mirrors.size(); i++){
						if(!myapp->BiDirected){
							//gather neighbor: in_edges					
							mirrors[i].second = in_nbr_distribution[mirrors[i].first][btldc] * myapp->msg_size;// * myapp->red_rate;					
						}else {
							//gather neighbor: all neighbors
							mirrors[i].second = (in_nbr_distribution[mirrors[i].first][btldc] + out_nbr_distribution[mirrors[i].first][btldc]) * myapp->msg_size;// * myapp->red_rate;							
						}	
#pragma omp critical
						priorities.push_back(mirrors[i]);
					}	
				}else if(gather_link == 1){
					//downlink
					#pragma omp parallel for
					for(int i=0; i< masters.size(); i++){
						if(!myapp->BiDirected){
							//gather neighbor: in_edges
							for(int di=0; di<DCs.size(); di++){
								if(btldc != di){
									masters[i].second += in_nbr_distribution[masters[i].first][di] * myapp->msg_size;// * myapp->red_rate;
								}
							}	
#pragma  omp critical
							priorities.push_back(masters[i]);
						}else{
							//gather neighbor: all neighbors
							for(int di=0; di<DCs.size(); di++){
								if(btldc != di){
									masters[i].second += (in_nbr_distribution[masters[i].first][di] + out_nbr_distribution[masters[i].first][di]) 
										* myapp->msg_size;// * myapp->red_rate;							
								}
							}
						}
					}	
				}else{
					//printf("no bottleneck link?\n");
					cout << "no bottleneck link?\n";
					exit(1);
				}	
			}
			else{
				btldc = apply_btlneck;
				btllink = apply_link;
				for(int i=0; i<contained_vertices[btldc].size(); i++){
					int vid = contained_vertices[btldc][i];
					if((*dag->g).myvertex[vid]->master_location == btldc){
						masters.push_back(std::pair<int,float> (vid,0.0));
					}else{
						mirrors.push_back(std::pair<int,float> (vid,0.0));
					}
				}
				if(apply_link == 0){
					//uplink
					// estimate the gain of apply stage, master replicas
					for(int i=0; i<masters.size();i++){
						masters[i].second += myapp->msg_size * ((*dag->g).myvertex[masters[i].first]->replica_location.size() - 1);
						priorities.push_back(masters[i]);
					}
				}else if(apply_link == 1){
					//downlink
					// estimate the gain of apply stage, mirror replicas
					for(int i=0; i<mirrors.size();i++){
						mirrors[i].second += myapp->msg_size;
						priorities.push_back(mirrors[i]);
					}
				}else{
					//printf("no bottleneck link?\n");
					cout << "no bottleneck link?\n";
					exit(1);
				}						
			}							
		}//endif bound on diff links
		//iteratively remove the head of priorities
		//until btldc is no longer the bottleneck
		std::sort(priorities.begin(),priorities.end(),mycompare);
		int candi_size = priorities.size();
		if(candi_size > 100){    
			//select the top 10-percent
			candi_size = std::ceil(candi_size*0.1);
			candi_size = candi_size>100?100:candi_size; 
			priorities.resize(candi_size);
		}
		// printf("prioritie queue to remove\n");
		// for(int pi=0; pi<priorities.size(); pi++){
			// printf("%d, %f\n",priorities[pi].first, priorities[pi].second);
		// }		
		//int suc = 0;
		for(int pvi = 0; pvi<priorities.size(); pvi++){
			bool mig_suc = false;
			int v_to_rm = priorities[pvi].first;
			int orig_in_nbr = in_nbr_distribution[v_to_rm][btldc];
			int orig_out_nbr = out_nbr_distribution[v_to_rm][btldc];
			
			float cur_time = EstimateTransferTime();
			float cur_cost = EstimateTransferCost();
			//cout<<"cur_cost"<<cur_cost<<endl;
			//printf("****************time before removing vertex %d from dc %d is %f*****************\n",v_to_rm,btldc,cur_time);
			
			if((*dag->g).myvertex[v_to_rm]->master_location == btldc){
				if(!myapp->BiDirected){
					//gather neighbor: in_edges
					for(int di=0; di<DCs.size(); di++){
						if(btldc != di){
							DCs[btldc]->g_dnload_data -= in_nbr_distribution[v_to_rm][di] * myapp->msg_size;// * myapp->red_rate;
						}
					}					
				}else {
					//gather neighbor: all neighbors
					for(int di=0; di<DCs.size(); di++){
						if(btldc != di){
							DCs[btldc]->g_dnload_data -= (in_nbr_distribution[v_to_rm][di] + out_nbr_distribution[v_to_rm][di]) 
								* myapp->msg_size;// * myapp->red_rate;							
						}
					}
				}
				DCs[btldc]->a_upload_data -= myapp->msg_size * ((*dag->g).myvertex[v_to_rm]->replica_location.size() - 1);
			}else{
				float delta_traffic = 0;
				if(!myapp->BiDirected){
					//gather neighbor: in_edges					
					delta_traffic = in_nbr_distribution[v_to_rm][btldc] * myapp->msg_size;// * myapp->red_rate;	
				}else{
					//gather neighbor: all neighbors
					delta_traffic = (in_nbr_distribution[v_to_rm][btldc] + out_nbr_distribution[v_to_rm][btldc]) * myapp->msg_size;// * myapp->red_rate;
				}
				DCs[btldc]->g_upload_data -= delta_traffic;
				DCs[(*dag->g).myvertex[v_to_rm]->master_location]->g_dnload_data -= delta_traffic;					
				DCs[btldc]->a_dnload_data -= myapp->msg_size;
				DCs[(*dag->g).myvertex[v_to_rm]->master_location]->a_upload_data -= myapp->msg_size;
			}		
			for(int i=0; i<(*dag->g).myvertex[v_to_rm]->replica_location.size(); i++){
				if((*dag->g).myvertex[v_to_rm]->replica_location[i] == btldc){
					(*dag->g).myvertex[v_to_rm]->replica_location.erase((*dag->g).myvertex[v_to_rm]->replica_location.begin()+i);
					break;
				}
			}
			//update master if neccesary
			std::vector<int> orig_masters;
			orig_masters.push_back((*dag->g).myvertex[v_to_rm]->master_location);
			in_nbr_distribution[v_to_rm][btldc] = 0;
			out_nbr_distribution[v_to_rm][btldc] = 0;
			
			std::vector<int > connected_edges;	
//			#pragma omp parallel for	
			for(int i=0; i<edge_distribution[btldc].size(); i++){
				int ecount = edge_distribution[btldc][i];
				//int src = source(dag->random_access_edges[ecount],*dag->g);
				int src = (*dag->g).source_edge(ecount);
				//int tgt = target(dag->random_access_edges[ecount],*dag->g);
				int tgt = (*dag->g).target_edge(ecount);
				if(src == v_to_rm || tgt == v_to_rm){
					if(src == v_to_rm ){
//					#pragma omp criticali
//						{
						in_nbr_distribution[tgt][btldc] --;
						orig_masters.push_back((*dag->g).myvertex[tgt]->master_location);
//						}
					}else{
//					#pragma omp critical
//							{
						out_nbr_distribution[src][btldc] --;
						orig_masters.push_back((*dag->g).myvertex[src]->master_location);
//						}
					} 	
//				#pragma omp critical
//					{				
					connected_edges.push_back(edge_distribution[btldc][i]);
					swap(edge_distribution[btldc][i],edge_distribution[btldc][edge_distribution[btldc].size()-1]);
					edge_distribution[btldc].pop_back();
				//	edge_distribution[btldc].erase(edge_distribution[btldc].begin()+i);
					i--;
//					}					
				}
			}
			for(int i=0; i<orig_masters.size(); i++){
				int gv = -1;
				if(i==0){
					gv = v_to_rm;
				}else{
					//int lsrc = source(dag->random_access_edges[connected_edges[i-1]],*dag->g);
					int lsrc = (*dag->g).source_edge(connected_edges[i-1]);
					//int ltgt = target(dag->random_access_edges[connected_edges[i-1]],*dag->g);		
					int ltgt = (*dag->g).target_edge(connected_edges[i-1]);
					gv = lsrc == v_to_rm ? ltgt : lsrc;
				}
				if(orig_masters[i] == btldc){
					int largest_degree = -1;
					for(int dc=0; dc<(*dag->g).myvertex[gv]->replica_location.size(); dc++){
						//if(dc != btldc){
							int newmaster = (*dag->g).myvertex[gv]->replica_location[dc];
							int local_degree = in_nbr_distribution[gv][newmaster] + out_nbr_distribution[gv][newmaster];
							if(local_degree > largest_degree){
								largest_degree = local_degree;
								(*dag->g).myvertex[gv]->master_location = newmaster;
							}
						//}
					}
					//printf("master location of v %d changed from %d to %d\n", gv,orig_masters[i],(*dag->g)[gv].master_location);	
				}				
			}
			//end of change status
			/********************Migrate edges: group k edges and migrate them to the same dc at the same time***********************************/
			std::pair<int,std::vector<int> > results;	
			std::vector<float> esttimes;
			std::vector<int> notmoveto;
			notmoveto.push_back(btldc);
			//cout << connected_edges.size() << "   " << " cur_time: " << cur_time << " cur_cost " << cur_cost << endl;	
			//migrate all edges at the same time
			if(cost)
				results  = CostAwareEdgeMigrate(connected_edges,notmoveto,cur_time,cur_cost);
			else
				results =  EdgeMigrate(connected_edges,notmoveto,cur_time);
			int dc_to_place = results.first;
			if(dc_to_place != -1){		
				mig_suc = true;	
				total_migrated += connected_edges.size();				
				//cout << "migrate " << connected_edges.size() << " edges to dc " << dc_to_place << endl;
				// change the status
				for(int ei=0; ei<connected_edges.size(); ei++){
					//int src = source(dag->random_access_edges[connected_edges[ei]],*dag->g);
					int src = (*dag->g).source_edge(connected_edges[ei]);
					//int tgt = target(dag->random_access_edges[connected_edges[ei]],*dag->g);
					int tgt = (*dag->g).target_edge(connected_edges[ei]);
					int tag = results.second[ei];
					if(tag == 0){
						if((*dag->g).myvertex[src]->master_location != dc_to_place && myapp->BiDirected){
							DCs[dc_to_place]->g_upload_data += myapp->msg_size;// * myapp->red_rate;
						}						
						if((*dag->g).myvertex[tgt]->master_location != dc_to_place){
							DCs[dc_to_place]->g_upload_data += myapp->msg_size;// * myapp->red_rate;
						}	
					}else if(tag == 1){
						(*dag->g).myvertex[src]->replica_location.push_back(dc_to_place);	
						if((*dag->g).myvertex[tgt]->master_location != dc_to_place)
							DCs[dc_to_place]->g_upload_data += myapp->msg_size;// * myapp->red_rate;
						//add a src mirror: increase g_upload_data and a_dnload_data
						if(myapp->BiDirected){
							DCs[dc_to_place]->g_upload_data += myapp->msg_size;									
							DCs[(*dag->g).myvertex[src]->master_location]->g_dnload_data += myapp->msg_size;
						}
						DCs[dc_to_place]->a_dnload_data += myapp->msg_size;	
						DCs[(*dag->g).myvertex[src]->master_location]->a_upload_data += myapp->msg_size;	
					}else if(tag == 2){
						(*dag->g).myvertex[tgt]->replica_location.push_back(dc_to_place);
						
						if((*dag->g).myvertex[src]->master_location != dc_to_place && myapp->BiDirected)
							DCs[dc_to_place]->g_upload_data += myapp->msg_size;// * myapp->red_rate;
						//add a tgt mirror: increase g_upload_data and a_dnload_data											
						DCs[dc_to_place]->g_upload_data += myapp->msg_size;	
						DCs[dc_to_place]->a_dnload_data += myapp->msg_size;	
						DCs[(*dag->g).myvertex[tgt]->master_location]->g_dnload_data += myapp->msg_size;
						DCs[(*dag->g).myvertex[tgt]->master_location]->a_upload_data += myapp->msg_size;		
					}else if(tag == 3){
						(*dag->g).myvertex[src]->replica_location.push_back(dc_to_place);
						(*dag->g).myvertex[tgt]->replica_location.push_back(dc_to_place);
						
						if(myapp->BiDirected){
							DCs[dc_to_place]->g_upload_data += myapp->msg_size;// * myapp->red_rate;					
							DCs[(*dag->g).myvertex[src]->master_location]->g_dnload_data += myapp->msg_size;
						}
						DCs[dc_to_place]->a_dnload_data += myapp->msg_size;
						DCs[(*dag->g).myvertex[src]->master_location]->a_upload_data += myapp->msg_size;
						DCs[dc_to_place]->g_upload_data += myapp->msg_size;// * myapp->red_rate;
						DCs[dc_to_place]->a_dnload_data += myapp->msg_size;			
						DCs[(*dag->g).myvertex[tgt]->master_location]->g_dnload_data += myapp->msg_size;
						DCs[(*dag->g).myvertex[tgt]->master_location]->a_upload_data += myapp->msg_size;
					}	
					in_nbr_distribution[tgt][dc_to_place] ++;
					out_nbr_distribution[src][dc_to_place] ++;
					edge_distribution[dc_to_place].push_back(connected_edges[ei]);
					//replica_location push_back src/tgt
					/*std::vector<int>::iterator srciter = find((*dag->g).myvertex[src]->replica_location.begin(),(*dag->g).myvertex[src]->replica_location.end(),src);
					if (srciter == (*dag->g).myvertex[src]->replica_location.end()){
						(*dag->g).myvertex[src]->replica_location.push_back(dc_to_place);
					}
					std::vector<int>::iterator tgtiter = find((*dag->g).myvertex[tgt]->replica_location.begin(),(*dag->g).myvertex[tgt]->replica_location.end(),tgt);
					if (tgtiter == (*dag->g).myvertex[tgt]->replica_location.end()){
						(*dag->g).myvertex[tgt]->replica_location.push_back(dc_to_place);
					}*/
					// if(in_nbr_distribution[src][(*dag->g)[src].master_location] == 0 && out_nbr_distribution[src][(*dag->g)[src].master_location] == 0)
						// (*dag->g)[src].master_location = dc_to_place;
					// if(in_nbr_distribution[tgt][(*dag->g)[tgt].master_location] == 0 && out_nbr_distribution[tgt][(*dag->g)[tgt].master_location] == 0)
						// (*dag->g)[tgt].master_location = dc_to_place;					
				}//end for each result	
				for(int ei=0; ei<connected_edges.size(); ei++){
					//int src = source(dag->random_access_edges[connected_edges[ei]],*dag->g);
					int src = (*dag->g).source_edge(connected_edges[ei]);
					//int tgt = target(dag->random_access_edges[connected_edges[ei]],*dag->g);
					int tgt = (*dag->g).target_edge(connected_edges[ei]);
					for(int it=0; it<2; it++){
						int node = -1;
						if(it == 0) node = src;
						else node = tgt;
						int master = (*dag->g).myvertex[node]->master_location;
						if(std::find((*dag->g).myvertex[node]->replica_location.begin(),(*dag->g).myvertex[node]->replica_location.end(),master)==(*dag->g).myvertex[node]->replica_location.end()){
							int largest_degree = -1;
							for(int di=0; di<(*dag->g).myvertex[node]->replica_location.size(); di++){							
								int local_degree = in_nbr_distribution[node][(*dag->g).myvertex[node]->replica_location[di]] + out_nbr_distribution[node][(*dag->g).myvertex[node]->replica_location[di]];
								if(local_degree > largest_degree){
									largest_degree = local_degree;
									(*dag->g).myvertex[node]->master_location = (*dag->g).myvertex[node]->replica_location[di];
								}
							}
						}
					}
				}				
			}
			else{
				//don't remove, recover context
				(*dag->g).myvertex[v_to_rm]->replica_location.push_back(btldc);
				for(int i=0; i<orig_masters.size(); i++){
					int gv = -1;
					if(i==0){
						gv = v_to_rm;
					}else{
						//int lsrc = source(dag->random_access_edges[connected_edges[i-1]],*dag->g);
						int lsrc = (*dag->g).source_edge(connected_edges[i-1]);
						//int ltgt = target(dag->random_access_edges[connected_edges[i-1]],*dag->g);		
						int ltgt = (*dag->g).target_edge(connected_edges[i-1]);
						gv = lsrc == v_to_rm ? ltgt : lsrc;
					}
					if(orig_masters[i] == btldc){						
						(*dag->g).myvertex[gv]->master_location = btldc;
					}	
				}
				in_nbr_distribution[v_to_rm][btldc] = orig_in_nbr;
				out_nbr_distribution[v_to_rm][btldc] = orig_out_nbr;
				
				for(int i=0; i<connected_edges.size(); i++){
					edge_distribution[btldc].push_back(connected_edges[i]);
					//int lsrc = source(dag->random_access_edges[connected_edges[i]],*dag->g);
					int lsrc = (*dag->g).source_edge(connected_edges[i]);
					//int ltgt = target(dag->random_access_edges[connected_edges[i]],*dag->g);		
					int ltgt = (*dag->g).target_edge(connected_edges[i]);
					if(lsrc == v_to_rm ){
						in_nbr_distribution[ltgt][btldc] ++;
					}else{
						out_nbr_distribution[lsrc][btldc] ++;
					} 
				}				
				
				if(orig_masters[0] == btldc){
					if(!myapp->BiDirected){
						//gather neighbor: in_edges
						for(int di=0; di<DCs.size(); di++){
							if(btldc != di){
								DCs[btldc]->g_dnload_data += in_nbr_distribution[v_to_rm][di] * myapp->msg_size;// * myapp->red_rate;
							}
						}					
					}else {
						//gather neighbor: all neighbors
						for(int di=0; di<DCs.size(); di++){
							if(btldc != di){
								DCs[btldc]->g_dnload_data += (in_nbr_distribution[v_to_rm][di] + out_nbr_distribution[v_to_rm][di]) 
									* myapp->msg_size;// * myapp->red_rate;							
							}
						}
					}
					DCs[btldc]->a_upload_data += myapp->msg_size * ((*dag->g).myvertex[v_to_rm]->replica_location.size() - 1);
				}else{
					float delta_traffic = 0;
					if(!myapp->BiDirected){
						//gather neighbor: in_edges					
						delta_traffic = in_nbr_distribution[v_to_rm][btldc] * myapp->msg_size;// * myapp->red_rate;	
					}else{
						//gather neighbor: all neighbors
						delta_traffic = (in_nbr_distribution[v_to_rm][btldc] + out_nbr_distribution[v_to_rm][btldc]) * myapp->msg_size;// * myapp->red_rate;
					}
					DCs[btldc]->g_upload_data += delta_traffic;
					DCs[orig_masters[0]]->g_dnload_data += delta_traffic;					
					DCs[btldc]->a_dnload_data += myapp->msg_size;
					DCs[orig_masters[0]]->a_upload_data += myapp->msg_size;
				}
			}
			vcount++;
			//cout << "vcount=" << vcount << endl;
		}
	}//end while cont
	
	for(int v=0; v<dag->num_vertices; v++){
		int master = (*dag->g).myvertex[v]->master_location;	
		if(master == -1){
			(*dag->g).myvertex[v]->master_location = (*dag->g).myvertex[v]->location_id;
		}
		if(std::find((*dag->g).myvertex[v]->replica_location.begin(),(*dag->g).myvertex[v]->replica_location.end(),master) == (*dag->g).myvertex[v]->replica_location.end()){
			//printf("vertex %d master location error in migration\n",v);
			cout <<"vertex " << v << " master location error in migration\n";
		}
	}
	//printf("In total %d edges are migrated.\n",total_migrated);
	cout <<"In total " <<total_migrated << " edges are migrated.\n";
	fileResult << "In total " << total_migrated << " edges are migrated.\n";
	float cur_cost=5*EstimateTransferCost();
	cout<<"curr cost: "<<cur_cost<<endl;
}
void Optimizer::SampleBasedPartitionPlacement(int anum){
	
	int vertexsize = dag->num_vertices;
	in_nbr_distribution = std::vector<std::vector<int> >(vertexsize,std::vector<int>(DCs.size(),0));
	out_nbr_distribution = std::vector<std::vector<int> >(vertexsize,std::vector<int>(DCs.size(),0));

	omp_set_num_threads(N_THREADS);
	for(int dc=0; dc<DCs.size(); dc++){
		for(int ei=0; ei<edge_distribution[dc].size(); ei++){
			//Edge e = dag->random_access_edges[edge_distribution[dc][ei]];
			//int src = source(e, *dag->g);
			//int tgt = target(e, *dag->g);
			int src = (*dag->g).source_edge(edge_distribution[dc][ei]);
			int tgt = (*dag->g).target_edge(edge_distribution[dc][ei]);
			in_nbr_distribution[tgt][dc]++;
			out_nbr_distribution[src][dc]++;
		}		
	}
	std::vector<int> tmp_master = std::vector<int>(vertexsize,-1);
#pragma omp parallel for
	for(int i=0; i<vertexsize; i++){
		tmp_master[i] = (*dag->g).myvertex[i]->master_location;
	}
	std::vector<std::vector<int> > contained_vertices = std::vector<std::vector<int> >(DCs.size());
	/*for (int i = 0; i < contained_vertices.size(); ++i)
	{
		cout << contained_vertices[i].size() << endl; cout << "contained_vertices[i]" << endl;
	}*/
#pragma omp parallel for
	for(int dc = 0; dc < DCs.size(); dc++){
		std::vector<int> p_contained_vertices;
		for(int ei=0; ei<edge_distribution[dc].size(); ei++){
			int ecount = edge_distribution[dc][ei];
			//int src = source(dag->random_access_edges[ecount], *dag->g);
			//int tgt = target(dag->random_access_edges[ecount], *dag->g);	
			int src = (*dag->g).source_edge(ecount);
			int tgt = (*dag->g).target_edge(ecount);
			p_contained_vertices.push_back(src);
			p_contained_vertices.push_back(tgt);
			//if(src == 8296 || tgt == 8296)
			// cout << "baohan" << "  dc  " << dc << " src " << src << "  tgt  " << tgt << endl;
		}
		for(int vi=0; vi<vertexsize; vi++){
			if(anum)
			if((*dag->g).myvertex[vi]->location_id == dc)
				p_contained_vertices.push_back(vi);
		}
		std::sort(p_contained_vertices.begin(),p_contained_vertices.end());
		std::vector<int>::iterator it = std::unique(p_contained_vertices.begin(),p_contained_vertices.end());
		p_contained_vertices.resize(std::distance(p_contained_vertices.begin(),it));
		contained_vertices[dc]=p_contained_vertices;
	}
	/*for (int i = 0; i < DCs.size(); ++i)
	{
		 vector<int>::iterator result = find( contained_vertices[i].begin(),contained_vertices[i].end(),8296 );
		if (result != contained_vertices[i].end())
		{
			 cout << i << endl; cout << "find" << endl;
		}
	}
	for (int i = 0; i < DCs.size(); ++i)
	{
		 cout << contained_vertices[i].size();
		for (int j = 0 ; j < contained_vertices[i].size(); j++)
		{
			cout << contained_vertices[i][j] << ", ";
		}
		cout << endl;
	}*/
	
	
	/**
	* Step 1: sample k pairs of partitions and select the best pair to switch their placement
	*/
	//record the flow in each DC
	float totalcost = 0;
	double sumsize = 0;
	int ss1=0,ss2=0;
	#pragma omp parallel for
	for(int dc=0; dc<DCs.size(); dc++){
		DCs[dc]->g_dnload_data = 0.0;
		DCs[dc]->g_upload_data = 0.0;
		DCs[dc]->a_dnload_data = 0.0;
		DCs[dc]->a_upload_data = 0.0;	
		for(int vi=0; vi<contained_vertices[dc].size(); vi++){
			//gather: master download, non-master upload
			//apply: master upload, non-master download
			int gv = contained_vertices[dc][vi];
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
							DCs[dc]->g_dnload_data += (in_nbr_distribution[gv][di] + out_nbr_distribution[gv][di]) * myapp->msg_size;// * myapp->red_rate;						
						}
					}
				}
				DCs[dc]->a_upload_data += myapp->msg_size * ((*dag->g).myvertex[gv]->replica_location.size() - 1);
				#pragma omp critical
					ss1++;
			}else{
				if(!myapp->BiDirected){
					//gather neighbor: in_edges					
					DCs[dc]->g_upload_data += in_nbr_distribution[gv][dc] * myapp->msg_size;// * myapp->red_rate;				
					 #pragma omp critical
                                                ss2 += in_nbr_distribution[gv][dc];
	
				}else{
					//gather neighbor: all neighbors
					DCs[dc]->g_upload_data += (in_nbr_distribution[gv][dc] + out_nbr_distribution[gv][dc]) * myapp->msg_size;// * myapp->red_rate;						
					 #pragma omp critical
                                                ss2 += in_nbr_distribution[gv][dc]+out_nbr_distribution[gv][dc];;
	
				}
				DCs[dc]->a_dnload_data += myapp->msg_size;
			}
		}//end for each vi	
		totalcost += DCs[dc]->upload_price * (DCs[dc]->a_upload_data + DCs[dc]->g_upload_data) / 1000.0; //per GB
		sumsize += DCs[dc]->a_upload_data+DCs[dc]->g_upload_data;
	}//end for each dc
	//cout << "ss1: " << ss1 << "  ss2: " << ss2 << endl;
	//for (int i =0; i < DCs.size(); i++)
		//cout << DCs[i]->a_upload_data << endl;
	//cout << "sumsize: " << sumsize << endl;
	printf("done prep\n");
			
	int k = 2;
	int count = 0;
	int switch_count = 0;
	while(count < std::pow(DCs.size(),4.0) && switch_count < std::pow(DCs.size(),2.0)){
		std::vector<std::pair<int,int> > partition_pair;
		for(int i=0; i<k; i++){
			int p1 = dag->rnd_generator(0,DCs.size());
			int p2 = dag->rnd_generator(0,DCs.size());
			if(p2 == p1)
				p2 = dag->rnd_generator(0,DCs.size());
			//if(DCs[p2]->upload_price == DCs[p1]->upload_price)
			//	i--;
		//	else
			partition_pair.push_back(std::pair<int,int>(p1,p2));
		}
		int best_p = -1;
		float delta_time = 0; //reduced time, must be positive
		float delta_cost = 0; //reduced cost, can be negative if the budget is met	
		for(int i=0; i<k; i++){
			int p1 = partition_pair[i].first;
			int p2 = partition_pair[i].second;
			//printf("sampling iteration %d: DC %d, DC %d\n",i,p1,p2);
			if(i == 0 || p1 != partition_pair[i-1].first || p2 != partition_pair[i-1].second ){
				//if(p1*p2==0 && p1+p2!=5){
				//	printf("see what happens");
				//}
				/** strategy:
					1. if the cost violates budget, if cost reduced, switch					
					2. else if the time is reduced by switching and the cost satisfies budget, switch
				*/
				//first, calculate the time and cost before switch
				float g_upload_time = DCs[p1]->g_upload_data / DCs[p1]->upload_band > DCs[p2]->g_upload_data / DCs[p2]->upload_band?DCs[p1]->g_upload_data / DCs[p1]->upload_band:DCs[p2]->g_upload_data / DCs[p2]->upload_band;
				float g_dnload_time = DCs[p1]->g_dnload_data / DCs[p1]->download_band>DCs[p2]->g_dnload_data / DCs[p2]->download_band?DCs[p1]->g_dnload_data / DCs[p1]->download_band:DCs[p2]->g_dnload_data / DCs[p2]->download_band;
				float a_upload_time = DCs[p1]->a_upload_data / DCs[p1]->upload_band > DCs[p2]->a_upload_data / DCs[p2]->upload_band?DCs[p1]->a_upload_data / DCs[p1]->upload_band:DCs[p2]->a_upload_data / DCs[p2]->upload_band;
				float a_dnload_time = DCs[p1]->a_dnload_data / DCs[p1]->download_band>DCs[p2]->a_dnload_data / DCs[p2]->download_band?DCs[p1]->a_dnload_data / DCs[p1]->download_band:DCs[p2]->a_dnload_data / DCs[p2]->download_band;
				float gather_max_time = g_upload_time > g_dnload_time ? g_upload_time : g_dnload_time;
				float apply_max_time = a_upload_time > a_dnload_time ? a_upload_time : a_dnload_time;
				float cur_time = gather_max_time+apply_max_time;	
				float cur_cost = ((DCs[p1]->g_upload_data+DCs[p1]->a_upload_data) * DCs[p1]->upload_price + (DCs[p2]->g_upload_data+DCs[p2]->a_upload_data)  * DCs[p2]->upload_price)/1000.0;	//price per GB	
				//if switched, calculate the new time and cost
				g_upload_time = DCs[p1]->g_upload_data / DCs[p2]->upload_band > DCs[p2]->g_upload_data / DCs[p1]->upload_band?DCs[p1]->g_upload_data / DCs[p2]->upload_band:DCs[p2]->g_upload_data / DCs[p1]->upload_band;
				g_dnload_time = DCs[p1]->g_dnload_data / DCs[p2]->download_band>DCs[p2]->g_dnload_data / DCs[p1]->download_band?DCs[p1]->g_dnload_data / DCs[p2]->download_band:DCs[p2]->g_dnload_data / DCs[p1]->download_band;
				a_upload_time = DCs[p1]->a_upload_data / DCs[p2]->upload_band > DCs[p2]->a_upload_data / DCs[p1]->upload_band?DCs[p1]->a_upload_data / DCs[p2]->upload_band:DCs[p2]->a_upload_data / DCs[p1]->upload_band;
				a_dnload_time = DCs[p1]->a_dnload_data / DCs[p2]->download_band>DCs[p2]->a_dnload_data / DCs[p1]->download_band?DCs[p1]->a_dnload_data / DCs[p2]->download_band:DCs[p2]->a_dnload_data / DCs[p1]->download_band;
				float new_gather_max_time = g_upload_time > g_dnload_time ? g_upload_time : g_dnload_time;
				float new_apply_max_time = a_upload_time > a_dnload_time ? a_upload_time : a_dnload_time;
				float new_time = new_gather_max_time + new_apply_max_time;	
				float new_cost = ((DCs[p1]->g_upload_data+DCs[p1]->a_upload_data) * DCs[p2]->upload_price + (DCs[p2]->g_upload_data+DCs[p2]->a_upload_data) * DCs[p1]->upload_price)/1000.0; //price per GB					
				//decide whether to switch or not
				bool do_switch = false;
				if(totalcost > (myapp->budget)){
					if(delta_cost < (cur_cost - new_cost)){
				//		cout << cur_cost-new_cost << endl;
						do_switch = true;
						delta_cost = cur_cost-new_cost;
					}
				}else if(cur_time > new_time && (cur_cost >= new_cost || (totalcost-cur_cost+new_cost)<myapp->budget)){
					if(delta_time < (cur_time - new_time)){
						do_switch = true;
						delta_time = cur_time-new_time;
						delta_cost=cur_cost-new_cost;		   
					}
				}//else if(cur_time == new_time && cur_cost > new_cost){
					// if(delta_cost < (cur_cost - new_cost)){
						// do_switch = true;
						// delta_time = cur_time-new_time;
					// }
				// }	
				if(do_switch){
					printf("curr time: %.8f, new time: %.8f, curr cost: %.8f, new cost: %.8f\n",cur_time,new_time,totalcost,totalcost-cur_cost+new_cost);
					best_p = i;					
				}
			}
		}
		if(best_p != -1){
			int btlneck = partition_pair[best_p].first;
			int dc_to_switch = partition_pair[best_p].second;
			//debug: print everything before switch
			std::vector<std::vector<int> > orig_rep;
			bool debug=false;
			if(debug){				
				for(int vi=0; vi<contained_vertices[btlneck].size(); vi++){
					int gv = contained_vertices[btlneck][vi];
					orig_rep.push_back((*dag->g).myvertex[gv]->replica_location);					
				}				
			}
			//do the switch
			switch_count ++;
			std::printf("partition mapping switch DC %d with %d\n",btlneck,dc_to_switch);
			totalcost -= delta_cost;
			//update vertex replica location and in/out nbrs			
		#pragma omp parallel for
			for(int vi=0; vi<contained_vertices[btlneck].size(); vi++){
				int gv = contained_vertices[btlneck][vi];
				in_nbr_distribution[gv][btlneck] = 0;
				out_nbr_distribution[gv][btlneck] = 0;
				int loc = -1;
				for(int ri =0; ri < (*dag->g).myvertex[gv]->replica_location.size(); ri++){	
					if((*dag->g).myvertex[gv]->replica_location[ri] == btlneck){
						/*for(int ri1=0; ri1 < (*dag->g).myvertex[gv]->replica_location.size(); ri1++)
						printf("%d, ",(*dag->g).myvertex[gv]->replica_location[ri1]);
						printf("\n");
						cout << gv << " ri "  << ri << " (*dag->g).myvertex[gv]->replica_location[ri] " << (*dag->g).myvertex[gv]->replica_location[ri] << " dc_to_switch " << dc_to_switch<< endl;
						*/
						(*dag->g).myvertex[gv]->replica_location[ri] = dc_to_switch; 
						/*for(int ri1=0; ri1 < (*dag->g).myvertex[gv]->replica_location.size(); ri1++)
						printf("%d, ",(*dag->g).myvertex[gv]->replica_location[ri1]);
						printf("\n");*/
						/*std::sort((*dag->g).myvertex[gv]->replica_location.begin(), (*dag->g).myvertex[gv]->replica_location.end());
						(*dag->g).myvertex[gv]->replica_location.erase(std::unique((*dag->g).myvertex[gv]->replica_location.begin(), (*dag->g).myvertex[gv]->replica_location.end()),(*dag->g).myvertex[gv]->replica_location.end());
						for(int ri1=0; ri1 < (*dag->g).myvertex[gv]->replica_location.size(); ri1++)
						printf("%d, ",(*dag->g).myvertex[gv]->replica_location[ri1]);
						printf("\n");*/
						loc = ri;
						break;
					}
				}				
				if((*dag->g).myvertex[gv]->master_location == btlneck)
					tmp_master[gv] = dc_to_switch;
				// for(int ri=0; ri<(*dag->g)[gv].replica_location.size(); ri++){	
					// if((*dag->g)[gv].replica_location[ri] == dc_to_switch && ri != loc){
					//	//delete the duplicated replica						
						// (*dag->g)[gv].replica_location.erase((*dag->g)[gv].replica_location.begin()+ri);						
						// break;
					// }
				// }
			}
		#pragma omp parallel for
			for(int vi=0; vi<contained_vertices[dc_to_switch].size(); vi++){				
				int gv = contained_vertices[dc_to_switch][vi];
				in_nbr_distribution[gv][dc_to_switch] = 0;
				out_nbr_distribution[gv][dc_to_switch] = 0;
				int loc = -1;
				for(int ri =0; ri < (*dag->g).myvertex[gv]->replica_location.size(); ri++){
					if((*dag->g).myvertex[gv]->replica_location[ri] == dc_to_switch){
						/*for(int ri1=0; ri1 < (*dag->g).myvertex[gv]->replica_location.size(); ri1++)
						printf("%d, ",(*dag->g).myvertex[gv]->replica_location[ri1]);
						printf("\n");
						cout << gv << " ri "  << ri << " (*dag->g).myvertex[gv]->replica_location[ri] " << (*dag->g).myvertex[gv]->replica_location[ri] << " btlneck " << btlneck<< endl;
						*/
						(*dag->g).myvertex[gv]->replica_location[ri] = btlneck;
						/*for(int ri1=0; ri1 < (*dag->g).myvertex[gv]->replica_location.size(); ri1++)
						printf("%d, ",(*dag->g).myvertex[gv]->replica_location[ri1]);
						printf("\n");*/
						/*std::sort((*dag->g).myvertex[gv]->replica_location.begin(), (*dag->g).myvertex[gv]->replica_location.end());
						(*dag->g).myvertex[gv]->replica_location.erase(std::unique((*dag->g).myvertex[gv]->replica_location.begin(), (*dag->g).myvertex[gv]->replica_location.end()),(*dag->g).myvertex[gv]->replica_location.end());
						for(int ri1=0; ri1 < (*dag->g).myvertex[gv]->replica_location.size(); ri1++)
						printf("%d, ",(*dag->g).myvertex[gv]->replica_location[ri1]);
						printf("\n");*/
						loc = ri;
						break;
					}
				}
				if((*dag->g).myvertex[gv]->master_location == dc_to_switch)
					tmp_master[gv] = btlneck;
				// for(int ri=0; ri<(*dag->g)[gv].replica_location.size(); ri++){	
					// if((*dag->g)[gv].replica_location[ri] == btlneck && ri != loc){
					//	//delete the duplicated replica
						// (*dag->g)[gv].replica_location.erase((*dag->g)[gv].replica_location.begin()+ri);
						// break;
					// }
				// }
			}
		
		#pragma omp parallel for
			for(int vi=0; vi<contained_vertices[btlneck].size(); vi++){
				int gv = contained_vertices[btlneck][vi];
				/*for(int ri1=0; ri1 < (*dag->g).myvertex[gv]->replica_location.size(); ri1++)
						printf("%d, ",(*dag->g).myvertex[gv]->replica_location[ri1]);
						printf("\n");
				cout << gv << " (*dag->g).myvertex[gv]->master_location " << (*dag->g).myvertex[gv]->master_location << " tmp_master[gv] " << tmp_master[gv]<< endl;
				*/
				(*dag->g).myvertex[gv]->master_location = tmp_master[gv];
				/*for(int ri1=0; ri1 < (*dag->g).myvertex[gv]->replica_location.size(); ri1++)
						printf("%d, ",(*dag->g).myvertex[gv]->replica_location[ri1]);
						printf("\n");
						*/
			}
		#pragma omp parallel for
			for(int vi=0; vi<contained_vertices[dc_to_switch].size(); vi++){
				int gv = contained_vertices[dc_to_switch][vi];
				/*for(int ri1=0; ri1 < (*dag->g).myvertex[gv]->replica_location.size(); ri1++)
						printf("%d, ",(*dag->g).myvertex[gv]->replica_location[ri1]);
						printf("\n");
				cout << gv << " (*dag->g).myvertex[gv]->master_location " << (*dag->g).myvertex[gv]->master_location << " tmp_master[gv] " << tmp_master[gv]<< endl;
				*/
				(*dag->g).myvertex[gv]->master_location = tmp_master[gv];
				/*for(int ri1=0; ri1 < (*dag->g).myvertex[gv]->replica_location.size(); ri1++)
						printf("%d, ",(*dag->g).myvertex[gv]->replica_location[ri1]);
						printf("\n");
						*/
			}		
			std::vector<int> tmp = edge_distribution[btlneck];
			edge_distribution[btlneck] = edge_distribution[dc_to_switch];
			edge_distribution[dc_to_switch] = tmp;
			for(int ei=0; ei<edge_distribution[btlneck].size(); ei++){
				int ecount = edge_distribution[btlneck][ei];
				//int src = source(dag->random_access_edges[ecount], *dag->g);
				int src = (*dag->g).source_edge(ecount);
				//int tgt = target(dag->random_access_edges[ecount], *dag->g);
				int tgt = (*dag->g).target_edge(ecount);
				in_nbr_distribution[tgt][btlneck]++;
				out_nbr_distribution[src][btlneck]++;
			}
			for(int ei=0; ei<edge_distribution[dc_to_switch].size(); ei++){
				int ecount = edge_distribution[dc_to_switch][ei];
				//int src = source(dag->random_access_edges[ecount], *dag->g);
				int src = (*dag->g).source_edge(ecount);
				//int tgt = target(dag->random_access_edges[ecount], *dag->g);
				int tgt = (*dag->g).target_edge(ecount);
				in_nbr_distribution[tgt][dc_to_switch]++;
				out_nbr_distribution[src][dc_to_switch]++;
			}
			float tmp_g_upload = DCs[btlneck]->g_upload_data;
			float tmp_g_dnload = DCs[btlneck]->g_dnload_data;
			float tmp_a_upload = DCs[btlneck]->a_upload_data;
			float tmp_a_dnload = DCs[btlneck]->a_dnload_data;
			DCs[btlneck]->g_upload_data = DCs[dc_to_switch]->g_upload_data;
			DCs[btlneck]->g_dnload_data = DCs[dc_to_switch]->g_dnload_data;
			DCs[btlneck]->a_upload_data= DCs[dc_to_switch]->a_upload_data;
			DCs[btlneck]->a_dnload_data = DCs[dc_to_switch]->a_dnload_data; 
			DCs[dc_to_switch]->g_upload_data = tmp_g_upload;
			DCs[dc_to_switch]->g_dnload_data = tmp_g_dnload;
			DCs[dc_to_switch]->a_upload_data = tmp_a_upload;
			DCs[dc_to_switch]->a_dnload_data = tmp_a_dnload;			
			tmp = contained_vertices[btlneck];
			contained_vertices[btlneck] = contained_vertices[dc_to_switch];
			contained_vertices[dc_to_switch] = tmp;
			// ContextSwitch(btlneck,dc_to_switch,DCs);	
			//debug: print everything after the switch
			if(debug){
				int reptotal = 0;
				for(int vi=0; vi<contained_vertices[dc_to_switch].size(); vi++){
					int gv = contained_vertices[dc_to_switch][vi];
					int rep = (*dag->g).myvertex[gv]->replica_location.size();
					vector<int> tmp_rep = (*dag->g).myvertex[gv]->replica_location;
					std::sort(tmp_rep.begin(),tmp_rep.end());
					std::vector<int>::iterator it = std::unique(tmp_rep.begin(),tmp_rep.end());
					tmp_rep.resize(std::distance(tmp_rep.begin(),it));
					if(tmp_rep.size() != (*dag->g).myvertex[gv]->replica_location.size()){
						printf("%d had %ld replicas: ",gv,orig_rep[vi].size());
						for(int rindex=0; rindex < orig_rep[vi].size(); rindex++)
							printf("%d, ", orig_rep[vi][rindex]);
						printf("master is %d\n",(*dag->g).myvertex[gv]->master_location);
						printf("%d has %d replicas: ",gv,rep);
						for(int rindex=0; rindex < rep; rindex++)
							printf("%d, ", (*dag->g).myvertex[gv]->replica_location[rindex]);
						printf("master is %d\n",(*dag->g).myvertex[gv]->master_location);
					}					
					reptotal += rep;
				}
				//printf("in total %d replicas\n",reptotal);				
				//break; //switch once and check results				
			}			
		}		
		count ++;
		//printf("count %d\n",count);
	}	
	if(totalcost > myapp->budget) printf("!!!!!!!!!!!budget is too low, or try to increase the switch count in placement stage!!!!!!!!!!!!!!\n");
}
