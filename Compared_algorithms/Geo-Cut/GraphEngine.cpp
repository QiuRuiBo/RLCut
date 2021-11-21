#include "Simulator.h"
#include <omp.h>
#include <algorithm>
#include <iostream>  
#include <vector>  
#include <iterator>  
#include <iomanip>
#include <set>
using namespace std;

extern int N_THREADS;
extern ofstream fileResult;
void GraphEngine::Simulate() {

	/**
	* Clear memory.
	*/
	Threads.clear();
	for (int i = 0; i < num_threads; i++) {
		Threads.push_back(new Thread());
		Threads[i]->l_dag = new distributed_graph();
		Threads[i]->l_dag->g = new Graph();
	}

	float t_staging = 0.0;
	
	omp_set_num_threads(N_THREADS);
	/**
	* If there are some isolated vertices, which are not added into contained_vertices due to no edge assignment
	* we have to add them to the container according to their initial location
	*/
	for (int i = 0; i < myopt->dag->num_vertices; i++) {
		//int in_nbr = myopt->dag->get_in_nbrs(i).size();
		//int in_nbr = myopt->dag->get_in_nbrs_size(i);
		//int out_nbr = myopt->dag->get_out_nbrs(i).size();
		//int out_nbr = myopt->dag->get_out_nbrs_size(i);
		//if (in_nbr == 0 && out_nbr == 0) {
			//(*myopt->dag->g).myvertex[i]->master_location = (*myopt->dag->g).myvertex[i]->location_id;
		//}
	}
#pragma omp parallel for
	for (int i = 0; i < num_threads; i++) {
		if (num_threads != DCs.size()) {
			std::cout << "the number of threads has to be the same as the number of DCs" << std::endl;
			fileResult << "the number of threads has to be the same as the number of DCs\n" ;
			exit(1);
		}
		//printf("constructing local graph for thread %d\n", i);
		fileResult << "constructing local graph for thread " << i << "\n";
		Threads[i]->DC_loc_id = i;
		int local_id = 0;
		for (int vi = 0; vi < myopt->dag->num_vertices; vi++) {
			if(true){//debug
				if((*myopt->dag->g).myvertex[vi]->replica_location.empty()){
					printf("%d has no replica location\n",vi);
				}
				if(std::find((*myopt->dag->g).myvertex[vi]->replica_location.begin(),(*myopt->dag->g).myvertex[vi]->replica_location.end(),(*myopt->dag->g).myvertex[vi]->master_location)
					== (*myopt->dag->g).myvertex[vi]->replica_location.end()){
					printf("master location %d of %d not in its replica\n",(*myopt->dag->g).myvertex[vi]->master_location,vi);
					for(int ri=0; ri < (*myopt->dag->g).myvertex[vi]->replica_location.size(); ri++)
						printf("%d, ",(*myopt->dag->g).myvertex[vi]->replica_location[ri]);
					printf("\n");
				}
			}
			if (std::find((*myopt->dag->g).myvertex[vi]->replica_location.begin(), (*myopt->dag->g).myvertex[vi]->replica_location.end(), i)
				!= (*myopt->dag->g).myvertex[vi]->replica_location.end()) {
				//MyVertex* v = new MyVertex((*myopt->dag->g).myvertex[vi]);   //新的v是不一样的。 有另外新建
				MyVertex* v = new MyVertex();
				v->local_vid = local_id;
				v->location_id = (*myopt->dag->g).myvertex[vi]->location_id;
				v->vertex_id = (*myopt->dag->g).myvertex[vi]->vertex_id;
				v->master_location = (*myopt->dag->g).myvertex[vi]->master_location;
				v->data = (*myopt->dag->g).myvertex[vi]->data;
				v->accum = (*myopt->dag->g).myvertex[vi]->accum;
				if ((*myopt->dag->g).myvertex[vi]->master_location == i) {
					v->is_master = true;
				}
				else v->is_master = false;

				Threads[i]->newVertexNum[vi] = Threads[i]->l_dag->g->myvertex.size();
				Threads[i]->set_vertices.insert(vi);
				Threads[i]->l_dag->g->myvertex.push_back(v);
   		//		Threads[i]->set_vertices.clear();
				Threads[i]->vertexVector.push_back(v->vertex_id);
				local_id++;
			}
		}

		//add edges
		for (int ei = 0; ei < myopt->edge_distribution[i].size(); ei++) {
			//int gsrc = source(myopt->dag->random_access_edges[myopt->edge_distribution[i][ei]],*myopt->dag->g);
			int gsrc = (*myopt->dag->g).source_edge(myopt->edge_distribution[i][ei]);
			//int gtgt = target(myopt->dag->random_access_edges[myopt->edge_distribution[i][ei]],*myopt->dag->g);
			int gtgt = (*myopt->dag->g).target_edge(myopt->edge_distribution[i][ei]);
			Threads[i]->random_access_edges.push_back(make_pair(gsrc, gtgt));
			Threads[i]->set_vertices.insert(gsrc);
			Threads[i]->set_vertices.insert(gtgt);

		}
/*		int j = 0;
		for (set<int>::iterator it2 = Threads[i]->set_vertices.begin(); it2 != Threads[i]->set_vertices.end(); j++,it2++)
			Threads[i]->newVertexNum[*it2] = j;
*/		

		Threads[i]->numEdges = Threads[i]->random_access_edges.size();
		Threads[i]->numVertexVector = Threads[i]->vertexVector.size();
		printf("Thread %d\n #vertices: %d\n #edges: %d\n", i, Threads[i]->numVertexVector, Threads[i]->numEdges);
		fileResult << "Thread " << i << "\n" << "#vertices: " << Threads[i]->numVertexVector << "\n" << "#edges: " << Threads[i]->numEdges << "\n";
		//debug 
		//std::cout << "gather upload data size: " << DCs[i]->g_upload_data << std::endl
		//		  << "gather download data size: " << DCs[i]->g_dnload_data << std::endl
		//		  << "apply upload data size: " << DCs[i]->a_upload_data << std::endl
		//		  << "apply download data size: " << DCs[i]->a_dnload_data << std::endl;
	}
	long totalnodes = 0; long totaledges = 0;
	for (int i = 0; i < num_threads; i++) {
		totalnodes += Threads[i]->numVertexVector;
		totaledges += Threads[i]->numEdges;
	}
	float variance = 0.0;
	for (int i = 0; i < num_threads; i++) {
		variance += std::pow((float)Threads[i]->numVertexVector - (float)totalnodes / (float)num_threads, 2.0);
	}
	std::cout << "vertex replication rate:	" << (float)totalnodes / myopt->dag->num_vertices << std::endl
		<< "edge replication rate:	" << (float)totaledges / myopt->dag->num_edges << std::endl
		<< "load balancing variance:	" << std::sqrt(variance) << std::endl;
	fileResult << "vertex replication rate:	" << (float)totalnodes / myopt->dag->num_vertices << "\n"
		<< "edge replication rate:	" << (float)totaledges / myopt->dag->num_edges << "\n"
		<< "load balancing variance:	" << std::sqrt(variance) << "\n";
	int global_nodes = myopt->dag->num_vertices;
	//release memory space
	//delete myopt->dag;   //edit by shenbk   暂时先不删除，看看大小
	for (int i = 0; i<DCs.size(); i++)
		myopt->edge_distribution[i].clear();
	myopt->edge_distribution.clear();


	/** Start the engine, each thread executes at the same time */
	float t_step = 0.0;
	int iter_counter = 0;
	float totalcost = 0.0;
	float wan_usage = 0.0;
	//log
//	std::vector<float> up_data = std::vector<float>(global_nodes, 0.0);
	//std::vector<float> dn_data = std::vector<float>(global_nodes, 0.0);
        //std::vector<float> up_data = std::vector<float>(global_nodes, 0.0);
	float *dn_data = (float *) malloc(sizeof(float)*global_nodes);
	float *up_data = (float *) malloc(sizeof(float)*global_nodes);
	for(int i=0; i<global_nodes; i++){
		dn_data[i]=0.0;
		up_data[i]=0.0;
	}
	// #pragma omp parallel for
	for (int tr = 0; tr<num_threads; tr++) {
		//std::vector<int> sumvectorOne;
		//std::vector<int> partvectorOne;
		//printf("%d\n", tr);
		for (int viter = 0; viter < Threads[tr]->numVertexVector; viter++) {
			//sort(Threads[tr]->vertexVector.begin(), Threads[tr]->vertexVector.end());
			//set_intersection((*myopt->dag->g).myvertex[viter].in_neighbour.begin(), (*myopt->dag->g).myvertex[viter].in_neighbour.end(), Threads[tr]->vertexVector.begin(), Threads[tr]->vertexVector.end(), back_inserter(Threads[tr]->l_dag[Threads[tr]->newVertexNum[viter]]->in_neighbour));
			//set_intersection((*myopt->dag->g).myvertex[viter].out_neighbour.begin(), (*myopt->dag->g).myvertex[viter].out_neighbour.end(), Threads[tr]->vertexVector.begin(), Threads[tr]->vertexVector.end(), back_inserter(Threads[tr]->l_dag[Threads[tr]->newVertexNum[viter]]->out_neighbour));
			Threads[tr]->l_dag->g->myvertex[viter]->status = activated;
			Threads[tr]->l_dag->g->myvertex[viter]->next_status = deactivated;
			//myapp->mytype = sssp;
			/*if (myapp->mytype == pagerank){
				static_cast<PageRankVertexData*>(Threads[tr]->l_dag->g->myvertex[viter]->data)->rank = 0;
			}else if(myapp->mytype == sssp){
				static_cast<SSSP*>(Threads[tr]->l_dag->g->myvertex[viter]->data)->rank = 0;
			}else if(myapp->mytype == subgraph){
				static_cast<SubgraphIsom*>(Threads[tr]->l_dag->g->myvertex[viter]->data)->rank = 0;
			}*/
		}
		for (int eiter = 0; eiter < Threads[tr]->numEdges; eiter++) {
			int a = Threads[tr]->random_access_edges[eiter].second;
			int b = Threads[tr]->random_access_edges[eiter].first;
			int c = Threads[tr]->newVertexNum[b];
			int d = Threads[tr]->newVertexNum[a];
			Threads[tr]->l_dag->g->myvertex[c]->out_neighbour.push_back(a);
	//		Threads[tr]->l_dag->g->myvertex[c]->in_neighbour.push_back(a);
			Threads[tr]->l_dag->g->myvertex[d]->in_neighbour.push_back(b);
	//		Threads[tr]->l_dag->g->myvertex[d]->out_neighbour.push_back(b);
		}
	}
	float traffic[4][8];
	for(int ii = 0; ii < 4 ; ii++)
	{
		for(int jj = 0; jj < 8 ; jj++){
			traffic[ii][jj] = 0;
		}
	}
	/**
	* Start the engine
	* currently only implemented the synchronous engine
	*/
	if (type == synchronous) {
		if (myapp->ITERATIONS != 0) {
			/* converge within the fixed number of iterations */
			iter_counter = myapp->ITERATIONS;
			//vector<int>::iterator iElement;
			while (iter_counter > 0) {
				/**
				* Status setting
				*/
				for (int tr = 0; tr < num_threads; tr++) {
					for (int v = 0; v < Threads[tr]->numVertexVector; v++) {
						Threads[tr]->l_dag->g->myvertex[v]->status = activated;
						Threads[tr]->l_dag->g->myvertex[v]->next_status = deactivated;
					}
				}

				/**
				* Execute Gather on all active vertices
				* 1) vprogram local gather
				* 2) sync gather: if master, rcv and accum;
				*				  otherwise, send local gather result to master
				*/
				std::cout << "-------------- gather stage of iteration " << myapp->ITERATIONS - iter_counter << " -------------" << std::endl;
				fileResult << "-------------- gather stage of iteration " << myapp->ITERATIONS - iter_counter << " -------------\n";
				float gather_step = 0.0;
				float gather_cost = 0.0;
				float gather_wan = 0.0;
				std::vector<float> tr_gather_step = std::vector<float>(num_threads, 0.0);
			
	int numoflocks = num_threads;
				std::vector<omp_lock_t> writelock(numoflocks);
				for (int lk = 0; lk<numoflocks; lk++) {
					omp_init_lock(&writelock[lk]);
				}
				int numa[20];
				for(int i =0; i< 20 ; i++){
					numa[i]=0;
				}
				int ssi =0;
#pragma omp parallel for
				for (int tr = 0; tr<num_threads; tr++) {
					std::vector<float> sum_msg_size = std::vector<float>(N_THREADS, 0.0);
#pragma omp parallel for
					for (int v = 0; v < Threads[tr]->numVertexVector; v++) {
						//printf("%d\n", v);
						//std::vector<int> nbrs;
						
						if(Threads[tr]->l_dag->g->myvertex[v]->status == activated){
							std::vector<int> nbrs = myapp->get_gather_nbrs(v,Threads[tr]->l_dag);
							//std::vector<int> nbrs = Threads[tr]->l_dag->g->myvertex[v]->in_neighbour;
							VertexData* l_accum;
							//l_accum = new PageRankVertexData();
							if (myapp->mytype == pagerank) {
								l_accum = new PageRankVertexData();
							}
							else if (myapp->mytype == sssp) {
								l_accum = new SSSPVertexData();
							}
							else if (myapp->mytype == subgraph) {
								l_accum = new SubgraphVertexData();
							}
							VertexData* lvalue;
							//lvalue = new PageRankVertexData();
							if (myapp->mytype == pagerank) {
								lvalue = new PageRankVertexData();
							}
							else if (myapp->mytype == sssp) {
								lvalue = new SSSPVertexData();
							}
							else if (myapp->mytype == subgraph) {
								lvalue = new SubgraphVertexData();
							}
							for (int n = 0; n<nbrs.size(); n++) {
	
								//lvalue = myapp->gather(v,nbrs[n],Threads[tr]->l_dag);
								myapp->gather(v,Threads[tr]->newVertexNum[nbrs[n]],Threads[tr]->l_dag, lvalue);
								
								myapp->sum(l_accum, lvalue,l_accum);
								
							}
							delete lvalue;

							//if (!(*Threads[tr]->l_dag->g).myvertex[v].is_master) {
							if(!Threads[tr]->l_dag->g->myvertex[v]->is_master){
								/* send l_accum to master */
								Thread::mpi_message msg;
								msg.src_thread = tr;
								//msg.tgt_thread = (*Threads[tr]->l_dag->g).myvertex[v].master_location;
								msg.tgt_thread = Threads[tr]->l_dag->g->myvertex[v]->master_location;
								msg.src_l_vid = v;
								//std::unordered_map<int, int>::const_iterator got = Threads[msg.tgt_thread]->vid2lvid.find((*Threads[tr]->l_dag->g).myvertex[v].vertex_id);
								//std::unordered_map<int, int>::const_iterator got = Threads[msg.tgt_thread]->vid2lvid.find(Threads[tr]->l_dag[v]->vertex_id);								
								std::map<int,int>::iterator got = Threads[msg.tgt_thread]->newVertexNum.find(Threads[tr]->l_dag->g->myvertex[v]->vertex_id);
								if (got == Threads[msg.tgt_thread]->newVertexNum.end()) {
									//printf("cannot find the master vertex of %d in thread %d. \n", (*Threads[tr]->l_dag->g).myvertex[v].vertex_id, msg.tgt_thread);
									//printf("cannot find the master vertex of %d in thread %d. \n", Threads[tr]->l_dag->g->myvertex[v]->vertex_id, msg.tgt_thread);
									fileResult << "cannot find the master vertex of " << Threads[tr]->l_dag->g->myvertex[v]->vertex_id << " in thread " << msg.tgt_thread <<" \n";
									exit(1);
								}
								else
									msg.tgt_l_vid = got->second;

								if (myapp->mytype == pagerank) {
									msg.msg_value = new PageRankVertexData();
									numa[tr]++;
									*static_cast<PageRankVertexData*>(msg.msg_value) = *(static_cast<PageRankVertexData*>(l_accum));
								}
								else if (myapp->mytype == sssp) {
									msg.msg_value = new SSSPVertexData();
									numa[tr]++;
									*static_cast<SSSPVertexData*>(msg.msg_value) = *(static_cast<SSSPVertexData*>(l_accum)); 
								}
								else if (myapp->mytype == subgraph) {
									msg.msg_value = new SubgraphVertexData();
									numa[tr]++;
									*static_cast<SubgraphVertexData*>(msg.msg_value) = *(static_cast<SubgraphVertexData*>(l_accum)); 
								}
								//*msg.msg_value = *l_accum;
								//cout << myapp->msg_size << endl;
						//		cout << nbrs.size() << endl; 
								msg.msg_size = myapp->msg_size * nbrs.size();// *myapp->red_rate;			
							#pragma omp critical
							ssi+=nbrs.size();				
																			 // msg.msg_size = l_accum.matches.size();								
																			 // printf("msg size from %d to %d using msg_size estimation and sizeof calculation: %f, %f\n",msg.src_thread,msg.tgt_thread,est_msg_size,msg.msg_size);
																			 /* printf("send accum from %d to master at %d\n",msg.src_thread,msg.tgt_thread);
																			 for(int it=0; it<msg.msg_value.matches.size(); it++){
																			 for(int tt=0; tt<msg.msg_value.matches[it].pairs.size(); tt++){
																			 printf("%d, %d\t",msg.msg_value.matches[it].pairs[tt].first,msg.msg_value.matches[it].pairs[tt].second);
																			 }
																			 printf("\n");
																			 } */
								int threadid = omp_get_thread_num();
								sum_msg_size[threadid] += msg.msg_size;
								//up_data[(*Threads[tr]->l_dag->g).myvertex[v].vertex_id] += msg.msg_size;
								up_data[Threads[tr]->l_dag->g->myvertex[v]->vertex_id] += msg.msg_size;
								//msg.transfer_time = msg.msg_size / DCs[Threads[tr]->DC_loc_id]->net_band[msg.tgt_thread] + DCs[Threads[tr]->DC_loc_id]->net_latency[msg.tgt_thread];
								msg.transfer_time = msg.msg_size / DCs[Threads[tr]->DC_loc_id]->upload_band;//+ DCs[Threads[tr]->DC_loc_id]->net_latency[msg.tgt_thread];
								omp_set_lock(&writelock[msg.tgt_thread]);
								Threads[msg.tgt_thread]->messages.push_back(msg);
								//printf("msg accum %f \n",static_cast<PageRankVertexData*>(msg.msg_value)->rank);
								omp_unset_lock(&writelock[msg.tgt_thread]);
								// std::cout << "msg sent from thread " << msg.src_thread << ", local vid "<< v <<" --> thread " << msg.tgt_thread << ", local vid "<<msg.tgt_l_vid <<std::endl;
								// gather_cost += msg.msg_size * DCs[Threads[tr]->DC_loc_id]->net_price / 1000.0; //per GB
								// gather_wan += msg.msg_size; //gather msg size, depending on the local degree
							}
							else {
								//(*Threads[tr]->l_dag->g)[v].data.accum = l_accum;								
								//*(dynamiWc_cast<PageRankVertexData*>((*Threads[tr]->l_dag->g)[v].accum)) = *(static_cast<PageRankVertexData*>(l_accum));
								if (myapp->mytype == pagerank) {
								//	*static_cast<PageRankVertexData*>((*Threads[tr]->l_dag->g).myvertex[v].accum) = *(static_cast<PageRankVertexData*>(l_accum));
									*static_cast<PageRankVertexData*>(Threads[tr]->l_dag->g->myvertex[v]->accum) = *(static_cast<PageRankVertexData*>(l_accum));
								}
								else if (myapp->mytype == sssp) {
									*static_cast<SSSPVertexData*>(Threads[tr]->l_dag->g->myvertex[v]->accum) = *(static_cast<SSSPVertexData*>(l_accum)); 
								}
								else if (myapp->mytype == subgraph) {
									*static_cast<SubgraphVertexData*>(Threads[tr]->l_dag->g->myvertex[v]->accum) = *(static_cast<SubgraphVertexData*>(l_accum)); 
								}
								//*(*Threads[tr]->l_dag->g)[v].accum= *l_accum;
								//printf("master accum %f \n",static_cast<PageRankVertexData*>((*Threads[tr]->l_dag->g)[v].accum)->rank);
							}
							delete l_accum;
							nbrs.clear();
							//printf("accum %f \n",static_cast<PageRankVertexData*>((*Threads[tr]->l_dag->g)[v].accum)->rank);
						}//end if vertex activated												
					}//end for each v in l_dag
					float sum_msg_size_total = 0.0;
					//cout << sum_msg_size.size() << endl;
					for (int v = 0; v<sum_msg_size.size(); v++){
						sum_msg_size_total += sum_msg_size[v];
						//cout << sum_msg_size[v] << endl;
					}
					tr_gather_step[tr] = sum_msg_size_total / DCs[Threads[tr]->DC_loc_id]->upload_band;
					gather_wan += sum_msg_size_total;
					gather_cost += sum_msg_size_total * DCs[Threads[tr]->DC_loc_id]->upload_price / 1000.0; //per GB
					printf(" data center %d uploading %.4f data. \n", tr, sum_msg_size_total);
					traffic[0][tr]+=sum_msg_size_total;
					fileResult << "data center " << tr << " uploading " << setprecision(4) << sum_msg_size_total << " data. \n";
				}//end for each thread
				for(int i =0; i< 20; i++){
					//printf("1new%d thread: %d\n", i , numa[i]);
				}
			//	cout << " ssi: " << ssi <<endl;
				ssi = 0;
				for(int i =0; i< 20 ; i++){
					numa[i]=0;
				}
				 
				 // After all mirrors have sent their local accum
				 // let the master rcv and accumulate
				 //
#pragma omp parallel for
				for (int tr = 0; tr<num_threads; tr++) {
					float local_step = 0.0;
					float sum_msg_size = 0.0;
					//printf("%d messages for thread %d\n", Threads[tr]->messages.size(), tr);
					fileResult << Threads[tr]->messages.size() << " message for thread " << tr << " \n";
					for (int m = 0; m<Threads[tr]->messages.size(); m++) {
						int vid = Threads[tr]->messages[m].tgt_l_vid;
						//if (!(*Threads[tr]->l_dag->g).myvertex[vid].is_master) {
						if (!(Threads[tr]->l_dag->g->myvertex[vid]->is_master)) {
							//printf("sth wrong with gather msg passing: %d is not a master on thread %d\n", vid, tr);
							fileResult << "sth wrong with gather msg passing:" << vid << " is not a master on thread " << tr << "\n";
							exit(1);
						}
						float local_down_time = Threads[tr]->messages[m].msg_size / DCs[Threads[tr]->DC_loc_id]->download_band;// + DCs[Threads[tr]->DC_loc_id]->net_latency[Threads[tr]->messages[m].src_thread];
						local_step += local_down_time > Threads[tr]->messages[m].transfer_time ? local_down_time : Threads[tr]->messages[m].transfer_time;
						//(*Threads[tr]->l_dag->g)[vid].data.accum = myapp->sum((*Threads[tr]->l_dag->g)[vid].data.accum,Threads[tr]->messages[m].msg_value);						//printf("vertex %d: sum %f and %f\n",vid,static_cast<PageRankVertexData*>((*Threads[tr]->l_dag->g)[vid].accum)->rank, static_cast<PageRankVertexData*>(Threads[tr]->messages[m].msg_value)->rank);
						myapp->sum(Threads[tr]->l_dag->g->myvertex[vid]->accum, Threads[tr]->messages[m].msg_value,(*Threads[tr]->l_dag->g).myvertex[vid]->accum);
						//static_cast<PageRankVertexData*>(Threads[tr]->l_dag->g->myvertex[vid]->accum)->rank = static_cast<PageRankVertexData*>(Threads[tr]->l_dag->g->myvertex[vid]->accum)->rank + static_cast<PageRankVertexData*>(Threads[tr]->messages[m].msg_value)->rank;
						sum_msg_size += Threads[tr]->messages[m].msg_size;
						//dn_data[(*Threads[tr]->l_dag->g).myvertex[vid].vertex_id] += Threads[tr]->messages[m].msg_size;
						dn_data[Threads[tr]->l_dag->g->myvertex[vid]->vertex_id] += Threads[tr]->messages[m].msg_size;
					}
					/* barrier */
					//if(local_step > gather_step)
					//	gather_step = local_step;
					printf(" data center %d downloading %.4f data. \n", tr, sum_msg_size);
					traffic[1][tr]+= sum_msg_size;
					fileResult << "data center " << tr << " downloading " << setprecision(4) << sum_msg_size << " data. \n";
					float down_time = sum_msg_size / DCs[Threads[tr]->DC_loc_id]->download_band;
					if (down_time > tr_gather_step[tr])
						tr_gather_step[tr] = down_time;
					//remove the msgs have been processed
					/*for (int mi = 0; mi<Threads[tr]->messages.size(); mi++) {
						delete Threads[tr]->messages[mi].msg_value;
						numa[tr]++;
					}*/
					//Threads[tr]->messages.clear();
				}//end for each thread

				for (int tr = 0; tr<num_threads; tr++) {
					for (int mi = 0; mi<Threads[tr]->messages.size(); mi++) {
						delete Threads[tr]->messages[mi].msg_value;
						numa[tr]++;
					}
					Threads[tr]->messages.clear();
				}

				for(int i =0; i< 20; i++){
					//printf("2delete%d thread: %d\n", i , numa[i]);
				}

				for(int i =0; i< 20 ; i++){
					numa[i]=0;
				}


				for (int tr = 0; tr<num_threads; tr++)
					if (tr_gather_step[tr] > gather_step)
						gather_step = tr_gather_step[tr];
				t_step += gather_step;
				totalcost += gather_cost;
				wan_usage += gather_wan;
				/**
				* Execute Apply on all active vertices
				* 1) master vprogram get gather accum and apply()
				* 2) master send apply() result to all replicas
				*/
				std::cout << "-------------- apply stage  " << myapp->ITERATIONS - iter_counter << " -------------" << std::endl;
				fileResult << "-------------- apply stage  " << myapp->ITERATIONS - iter_counter << " -------------\n";
				float apply_step = 0.0;
				float apply_cost = 0.0;
				float apply_wan = 0.0;
				std::vector<float> tr_apply_step = std::vector<float>(num_threads, 0.0);
				//#pragma omp parallel
				for (int tr = 0; tr<num_threads; tr++) {
					std::vector<float> sum_msg_size = std::vector<float>(N_THREADS, 0.0);
#pragma omp parallel for
					//for (int v = 0; v<Threads[tr]->l_dag->num_vertices; v++) {
					for (int v = 0; v < Threads[tr]->numVertexVector; v++){
						float local_step = 0.0;
						if (Threads[tr]->l_dag->g->myvertex[v]->is_master) {
							myapp->apply(v, (*Threads[tr]->l_dag->g).myvertex[v]->accum, Threads[tr]->l_dag);
							#pragma omp critical 
								ssi++;

							int vid = Threads[tr]->l_dag->g->myvertex[v]->vertex_id;
							//for (int n = 0; n<Threads[tr]->l_dag[v]->replica_location.size(); n++) {
							for (int n = 0; n< (*myopt->dag->g).myvertex[vid]->replica_location.size(); n++) {
								
								int rep_tr = (*myopt->dag->g).myvertex[vid]->replica_location[n];
								if (rep_tr != tr) {
									//std::unordered_map<int, int>::const_iterator got = Threads[rep_tr]->vid2lvid.find(vid);
									std::map<int,int>::iterator got = Threads[rep_tr]->newVertexNum.find(vid);
									if (got == Threads[rep_tr]->newVertexNum.end()) {
										//printf("cannot find the mirror vertex of %d in thread %d\n", vid, rep_tr);
										fileResult << "cannot find the mirror vertex of " << vid << " in thread " << rep_tr << "\n";
										exit(1);
									}
									else {
										Thread::mpi_message msg;
										msg.src_thread = tr;
										msg.tgt_thread = rep_tr;
										msg.src_l_vid = v;
										msg.tgt_l_vid = got->second;
										if (myapp->mytype == pagerank) {
											msg.msg_value = new PageRankVertexData();
											numa[tr]++;
											*static_cast<PageRankVertexData*>(msg.msg_value) = *static_cast<PageRankVertexData*>(Threads[tr]->l_dag->g->myvertex[v]->data);
										}
										else if (myapp->mytype == sssp) {
											msg.msg_value = new SSSPVertexData();
											numa[tr]++;
											*static_cast<SSSPVertexData*>(msg.msg_value) = *static_cast<SSSPVertexData*>(Threads[tr]->l_dag->g->myvertex[v]->data); 
										}
										else if (myapp->mytype == subgraph) {
											msg.msg_value = new SubgraphVertexData();											
											numa[tr]++;
											*static_cast<SubgraphVertexData*>(msg.msg_value) = *static_cast<SubgraphVertexData*>(Threads[tr]->l_dag->g->myvertex[v]->data); 
										}
										//*msg.msg_value = *(*Threads[tr]->l_dag->g)[v].data; 
										msg.msg_size = myapp->msg_size;
										int threadid = omp_get_thread_num();
										sum_msg_size[threadid] += msg.msg_size;
										up_data[Threads[tr]->l_dag->g->myvertex[v]->vertex_id] += msg.msg_size;
										msg.transfer_time = msg.msg_size / DCs[Threads[tr]->DC_loc_id]->upload_band;// + DCs[Threads[tr]->DC_loc_id]->net_latency[msg.tgt_thread];
										omp_set_lock(&writelock[msg.tgt_thread]);
										Threads[msg.tgt_thread]->messages.push_back(msg);
										omp_unset_lock(&writelock[msg.tgt_thread]);
										// apply_cost += myapp->msg_size * DCs[Threads[tr]->DC_loc_id]->net_price / 1000.0; //per GB
										// apply_wan += myapp->msg_size;
									}
								}
							}
							//(*Threads[tr]->l_dag->g)[v].accum = VertexData();
						}
						/* barrier */
						//if(local_step > apply_step)
						//	apply_step = local_step;
					}
					float sum_msg_size_total = 0.0;
					for (int v = 0; v<sum_msg_size.size(); v++)
						sum_msg_size_total += sum_msg_size[v];
					tr_apply_step[tr] = sum_msg_size_total / DCs[Threads[tr]->DC_loc_id]->upload_band;
					apply_cost += sum_msg_size_total * DCs[Threads[tr]->DC_loc_id]->upload_price / 1000.0; //per GB
					apply_wan += sum_msg_size_total;
					printf(" data center %d uploading %.4f data. \n", tr, sum_msg_size_total);
					traffic[2][tr]+= sum_msg_size_total;
					fileResult << "data center " << tr << " uploading " << setprecision(4) << sum_msg_size_total << "data. \n";
				}
				for(int i =0; i< 20; i++){
					//printf("3new %d thread: %d\n", i , numa[i]);
				}
			//	cout <<"ssi apply : " << ssi << endl;
				for(int i =0; i< 20 ; i++){
					numa[i]=0;
				}
				for (int lk = 0; lk<numoflocks; lk++) {
					omp_destroy_lock(&writelock[lk]);
				}
				/**
				* After masters have sent the updated data
				* all mirrors recv and updated
				*/
				for (int tr = 0; tr<num_threads; tr++) {
					float sum_msg_size = 0.0;
					for (int m = 0; m<Threads[tr]->messages.size(); m++) {
						int vid = Threads[tr]->messages[m].tgt_l_vid;
						if (Threads[tr]->l_dag->g->myvertex[vid]->is_master) {
							//printf("sth wrong with apply msg passing: %d is a master on thread %d\n", vid, tr);
							fileResult << "sth wrong with apply msg passing: " << vid << " is a master on thread " << tr << "\n";
							exit(1);
						}
						float local_down_time = Threads[tr]->messages[m].msg_size / DCs[Threads[tr]->DC_loc_id]->download_band;// + DCs[Threads[tr]->DC_loc_id]->net_latency[Threads[tr]->messages[m].src_thread];
						if (myapp->mytype == pagerank) {
							*static_cast<PageRankVertexData*>((Threads[tr]->l_dag->g->myvertex[vid]->data)) = *static_cast<PageRankVertexData*>(Threads[tr]->messages[m].msg_value);
						}
						else if (myapp->mytype == sssp) {
							*static_cast<SSSPVertexData*>((Threads[tr]->l_dag->g->myvertex[vid]->data)) = *static_cast<SSSPVertexData*>(Threads[tr]->messages[m].msg_value); 
						}
						else if (myapp->mytype == subgraph) {
							*static_cast<SubgraphVertexData*>((Threads[tr]->l_dag->g->myvertex[vid]->data)) = *static_cast<SubgraphVertexData*>(Threads[tr]->messages[m].msg_value); 
						}
						//*((*Threads[tr]->l_dag->g)[vid].data) = *Threads[tr]->messages[m].msg_value;										
						sum_msg_size += Threads[tr]->messages[m].msg_size;
						dn_data[Threads[tr]->l_dag->g->myvertex[vid]->vertex_id] += Threads[tr]->messages[m].msg_size;
					}
					printf(" data center %d downloading %.4f data. \n", tr, sum_msg_size);
					traffic[3][tr] += sum_msg_size;
					fileResult << "data center " << tr << " downloading " << setprecision(4) << sum_msg_size << " data. \n";
					float down_time = sum_msg_size / DCs[Threads[tr]->DC_loc_id]->download_band;
					if (down_time > tr_apply_step[tr])
						tr_apply_step[tr] = down_time;
					//remove the msgs have been processed
					/*for (int mi = 0; mi<Threads[tr]->messages.size(); mi++) {
						delete Threads[tr]->messages[mi].msg_value;
						numa[tr]++;
					}
					Threads[tr]->messages.clear();*/
				}
				for (int tr = 0; tr<num_threads; tr++) {
					for (int mi = 0; mi<Threads[tr]->messages.size(); mi++) {
						delete Threads[tr]->messages[mi].msg_value;
						numa[tr]++;
					}
					Threads[tr]->messages.clear();
				}
				for(int i =0; i< 20; i++){
					//printf("4delete %d thread: %d\n", i , numa[i]);
				}

				for(int i =0; i< 20 ; i++){
					numa[i]=0;
				}
				for (int tr = 0; tr<num_threads; tr++) {
					if (tr_apply_step[tr] > apply_step)
						apply_step = tr_apply_step[tr];
				}
				t_step += apply_step;
				totalcost += apply_cost;
				wan_usage += apply_wan;
				/**
				* Execute Scatter
				* all vprograms, including master and mirrors, scatter
				*/
				float scatter_step = 0.0;
				float scatter_cost = 0.0;
				for (int tr = 0; tr<num_threads; tr++) {
					std::vector<int> nbrs;
					for (int v = 0; v<Threads[tr]->numVertexVector; v++) {
						std::vector<int> nbrs = myapp->get_scatter_nbrs(v, Threads[tr]->l_dag);				
						for (int n = 0; n < nbrs.size(); n++) {	
							myapp->scatter(v, Threads[tr]->newVertexNum[nbrs[n]], Threads[tr]->l_dag);
							//myapp->scatter(v,nbrs[n],Threads[tr]->l_dag);
						}
					}
					nbrs.clear();
				}
				//t_step += scatter_step;
				//totalcost += scatter_cost;
				iter_counter--;
			}

		}
		else {
			/* converge according to the tolerance */

			iter_counter++;
		}
	}
	else if (type == asynchronous) {
		//std::cout << "asynchronous engine is not implemented. " << std::endl;
		fileResult << "asynchronous engine is not implemented. \n";
	}
	
	float movecost = 0.0;
	float wanmove = 0.0;
	for (int tr = 0; tr<num_threads; tr++) {
		int num_instances = Threads[tr]->numVertexVector;
		for (int vi = 0; vi < num_instances; vi++) {
			if (Threads[tr]->l_dag->g->myvertex[vi]->master_location != tr) {
				movecost += DCs[Threads[tr]->l_dag->g->myvertex[vi]->master_location]->upload_price * Threads[tr]->l_dag->g->myvertex[vi]->data->input_size / 1000.0; //Per GB
				wanmove += Threads[tr]->l_dag->g->myvertex[vi]->data->input_size;
			}
		}
	}
	//delete Threads
	for(int i=0; i < num_threads; i++){
		for (int j=0; j < Threads[i]->l_dag->g->myvertex.size(); j++ ){
			delete Threads[i]->l_dag->g->myvertex[j];
		}
		//delete Threads[i]->l_dag->g;
		delete Threads[i]->l_dag;
		delete Threads[i];
	}
/*	for(int ii = 0; ii < 2 ; ii++){
		for(int jj = 0; jj < 8; jj++){
			traffic[ii][jj] += traffic[ii+2][jj];
			cout <<  ii << ",  " << jj << ",  "  << traffic[ii][jj] << endl;
		}
	}
*/	std::cout << "the simulated graph processing time is " << t_step << std::endl
		<< "the networking monetary cost is " << totalcost << std::endl
		<< "the data movement/replication cost is " << movecost << std::endl
		<< "the total wan communication is " << wan_usage << std::endl
		<< "the wan usage for data movement/replication is " << wanmove << std::endl;

	fileResult << "the simulated graph processing time is " << t_step << "\n"
		<< "the networking monetary cost is " << totalcost << "\n"
		<< "the data movement/replication cost is " << movecost << "\n"
		<< "the total wan communication is " << wan_usage << "\n"
		<< "the wan usage for data movement/replication is " << wanmove << "\n";
	cout << "simulate return" << endl;
	return;
}
