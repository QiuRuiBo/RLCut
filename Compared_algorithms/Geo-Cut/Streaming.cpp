#include "Simulator.h"
#include <omp.h>
/**
* the expected number of replicas for a vertex
* can be fixed by a user (we use 4 as an example)
* or calculated using methods in PowerGraph paper
*/
extern int N_THREADS;

void Optimizer::
StreamEdgeAssignment(Stream_Policy policy){
	
	/**
	* The edge assignment rules:
	* 1) If R(v) and R(u) intersect, place edge (u,v) into one of
	  the intersected dcs r with the lowest g_r(i);
	  2) If R(v) and R(u) are not empty and do not intersect,
	  place (u,v) in a dc in R(v) if we have:
	  a_u(i)+g_m(i) < av_(i)+g_n(i); for any n in R(u) (8)
	  3) If only R(v) or R(u) is not empty, choose one dc
	  r from the non-empty set with the lowest g_r(i).
	  4) If both R(v) and R(u) are empty, place (u;v) in any
	  dc r with the lowest g_r(i).
	*/
	int vertexsize = dag->num_vertices;

	std::vector<int> orig_v = std::vector<int>(DCs.size(),0);
	for(int vi=0; vi<vertexsize; vi++){
		(*dag->g).myvertex[vi]->replica_location.clear();
		(*dag->g).myvertex[vi]->replica_location.push_back((*dag->g).myvertex[vi]->location_id);
		//if ((*dag->g).myvertex[vi]->master_location == -1)
		(*dag->g).myvertex[vi]->master_location = (*dag->g).myvertex[vi]->location_id;
		orig_v[(*dag->g).myvertex[vi]->location_id] ++;
	}
	for(int i=0; i<DCs.size(); i++){
		printf("datacenter %d: %d vertices\n",i,orig_v[i]);
	}
	//int numedges = num_edges(*dag->g);
	int numedges = dag->num_edges;
	edge_distribution = std::vector<std::vector<int> >(DCs.size());
	
	omp_set_num_threads(N_THREADS);
	int numoflocks = dag->num_vertices;
	std::vector<omp_lock_t> writelock(numoflocks);
	for(int i=0; i<numoflocks; i++){
		omp_init_lock(&writelock[i]);
	}
	int numoflocks1 = DCs.size();
	std::vector<omp_lock_t> writelock1(numoflocks1);
	for(int i=0; i<numoflocks1; i++){
		omp_init_lock(&writelock1[i]);
	}
#pragma omp parallel for
	for(int ecount=0; ecount<numedges; ecount++){
		//Edge e = dag->random_access_edges[ecount];
		//int src = source(e, *dag->g);
		//int tgt = target(e, *dag->g);	
		int src = (*dag->g).source_edge(ecount);
		int tgt = (*dag->g).target_edge(ecount);

		int threadid = omp_get_thread_num();
		int lockid1, lockid2;
		lockid1 = src<tgt?std::fmod(src,(double)numoflocks):std::fmod(tgt,(double)numoflocks);
		lockid2 = src>tgt?std::fmod(src,(double)numoflocks):std::fmod(tgt,(double)numoflocks);
		// int lockid = std::fmod(pairing_function(src,tgt),(double)numoflocks);
		// printf("thread %d acquiring lock %d and %d: %d-->%d\n",threadid,lockid1,lockid2,src,tgt);
        omp_set_lock(&writelock[lockid1]);	
		omp_set_lock(&writelock[lockid2]);
		
		std::vector<int> notmoveto;
		std::pair<int,int> result ;
		if(policy == 0)
			result = MinCostAssign(src,tgt,notmoveto,orig_v);
		else if(policy == 1)
			result = MinTimeAssign(src,tgt,notmoveto,orig_v);
		else if(policy == 2)
			result = CostWeightedMinTimeAssign(src,tgt,notmoveto,orig_v);
		else if(policy == 3)
			result = EdgeAssign(src,tgt,notmoveto,orig_v);
		else{
			printf("not a valid streaming policy\n");
			exit(1);
		}
		int dc_to_assign = result.first;
		//cout << "dc_to_assign=" << dc_to_assign << endl;
		int tag = result.second;
		omp_set_lock(&writelock1[dc_to_assign]);
		edge_distribution[dc_to_assign].push_back(ecount);		
		if(tag == 1 || tag == 2)
			orig_v[dc_to_assign] ++;
		if(tag == 3)
			orig_v[dc_to_assign] += 2;
		omp_unset_lock(&writelock1[dc_to_assign]);
		//printf("thread %d: edge %d of %d, place in dc %d\n",threadid,ecount,numedges,dc_to_assign);
		
		//if(ecount % 100000 == 0){
		//	printf("thread %d addressed %d edges\n", threadid, ecount);
		//}
		omp_unset_lock(&writelock[lockid2]);	
		omp_unset_lock(&writelock[lockid1]);	
	}	
	
	/* master location is selected as the replica with largest local degree */
	/*
	//update contained vertices for each dc
	std::vector<std::vector<int> > local_degree_counter;
	for(int i=0; i<vertexsize; i++){
		local_degree_counter.push_back(std::vector<int>(DCs.size(),0));
	}	
	#pragma omp parallel for
	for(int dc = 0; dc<DCs.size(); dc++){
		for(int ei=0; ei<edge_distribution[dc].size(); ei++){
			int e = edge_distribution[dc][ei];
			int src = source(dag->random_access_edges[e],*dag->g);
			int tgt = target(dag->random_access_edges[e],*dag->g);
			omp_set_lock(&writelock1[dc]);
			local_degree_counter[src][dc]++;
			local_degree_counter[tgt][dc]++;
			omp_unset_lock(&writelock1[dc]);
		}
	}
	for(int i=0; i<numoflocks; i++)
		omp_destroy_lock(&writelock[i]);
	for(int i=0; i<numoflocks1; i++)
		omp_destroy_lock(&writelock1[i]);	

	
#pragma omp parallel for
	for(int vi=0; vi<vertexsize; vi++){
		int master = -1;//(*dag->g)[vi].location_id;
		int largest_degree = 0;
		for(int dc=0; dc<DCs.size(); dc++){
			if(local_degree_counter[vi][dc] > largest_degree){
				largest_degree = local_degree_counter[vi][dc];
				master = dc;
			}
		}
		(*dag->g)[vi].master_location = master;
		// no edge to the v
		if(master == -1){
			(*dag->g)[vi].master_location = (*dag->g)[vi].location_id;
		}
	}*/
}

std::pair<int,int> Optimizer::MinCostAssign(int src, int tgt, std::vector<int> notmoveto, std::vector<int> loads){
	/**
	* The edge assignment rules:
	* 1) If R(v) and R(u) intersect, place edge (u,v) into one of
	  the intersected dcs r with the lowest C_gather;
	  2) If R(v) and R(u) do not intersect, place (u,v) in a dc r
	  with the lowest C_gather+C_sync
	*/
	std::vector<int> src_replicas = (*dag->g).myvertex[src]->replica_location;
	std::vector<int> tgt_replicas = (*dag->g).myvertex[tgt]->replica_location;
	std::vector<int> intersection(src_replicas.size()+tgt_replicas.size());
	std::vector<int>::iterator it;
	std::sort(src_replicas.begin(),src_replicas.end());
	std::sort(tgt_replicas.begin(),tgt_replicas.end());
	it = std::set_intersection(src_replicas.begin(), src_replicas.end(), tgt_replicas.begin(), tgt_replicas.end(), intersection.begin());
	intersection.resize(it-intersection.begin()); 

	int dc_to_place = -1;
	int tag = -1; //
	float LARGE_FLOAT = 1e18;
	if(intersection.size() > 0){
		tag = 0;
		float lowest_cost = LARGE_FLOAT;
		for(int i=0; i< intersection.size(); i++){
			if(std::find(notmoveto.begin(),notmoveto.end(),intersection[i]) == notmoveto.end()){
				/*cout << src << "accum  src " << (*dag->g).myvertex[src]->accum->size_of() << endl;
				cout << tgt << "accum  tgt " << (*dag->g).myvertex[tgt]->accum->size_of() << endl;
				cout << src << "data  src " << (*dag->g).myvertex[src]->data->size_of() << endl;
				cout << tgt << "data  tgt " << (*dag->g).myvertex[tgt]->data->size_of() << endl;*/
				float delta_traffic =/* (*dag->g).myvertex[src]->accum->size_of()+(*dag->g).myvertex[tgt]->accum->size_of(); */ DeltaGatherTraffic(src,tgt,intersection[i],0);
				float gathercost = 0;
				if(intersection[i] != (*dag->g).myvertex[tgt]->master_location)
					gathercost = DCs[intersection[i]]->upload_price * delta_traffic / 1000.0;
				if(gathercost < lowest_cost){ 
					lowest_cost = gathercost;
					dc_to_place = intersection[i];						
				}
			}				
		}
	}else{
		float lowest_cost = LARGE_FLOAT;
		for (int i=0; i<DCs.size(); i++){
			if(std::find(notmoveto.begin(),notmoveto.end(),i) == notmoveto.end()){
				float delta_traffic = DeltaGatherTraffic(src,tgt,i,0);
				float gathercost = 0;
				if(i != (*dag->g).myvertex[tgt]->master_location)
					gathercost = DCs[i]->upload_price * delta_traffic / 1000.0;
				//calculate sync cost
				float syncost = 0;
				int local_tag = -1;
				if(std::find(src_replicas.begin(),src_replicas.end(), i) != src_replicas.end()){
					//vertex msg size of tgt
					syncost = myapp->msg_size * DCs[(*dag->g).myvertex[tgt]->master_location]->upload_price /1000.0;
					local_tag = 2;
				}else if(std::find(tgt_replicas.begin(),tgt_replicas.end(),i) != tgt_replicas.end()){
					syncost = myapp->msg_size * DCs[(*dag->g).myvertex[src]->master_location]->upload_price /1000.0;
					local_tag = 1;
				}else{
					syncost = myapp->msg_size * (DCs[(*dag->g).myvertex[tgt]->master_location]->upload_price + DCs[(*dag->g).myvertex[src]->master_location]->upload_price) /1000.0;
					local_tag = 3;
				}
				if(gathercost+syncost < lowest_cost){
					lowest_cost = gathercost+syncost;
					dc_to_place = i;
					tag = local_tag;
				}
			}
		}
		if(tag == 1){
			//src is replicated 
			(*dag->g).myvertex[src]->replica_location.push_back(dc_to_place);
		}else if(tag == 2){
			(*dag->g).myvertex[tgt]->replica_location.push_back(dc_to_place);
		}else if(tag == 3){
			(*dag->g).myvertex[src]->replica_location.push_back(dc_to_place);
			(*dag->g).myvertex[tgt]->replica_location.push_back(dc_to_place);
		}else if(tag == -1){
			printf("no dc is chosen for placing edge %d -> %d\n",src,tgt);
			exit(1);
		}
	}
	return std::pair<int,int>(dc_to_place,tag);
}

std::pair<int,int> Optimizer::MinTimeAssign(int src, int tgt, std::vector<int> notmoveto, std::vector<int> loads){
	
	std::vector<int> src_replicas = (*dag->g).myvertex[src]->replica_location;
	std::vector<int> tgt_replicas = (*dag->g).myvertex[tgt]->replica_location;
	std::vector<int> intersection(src_replicas.size()+tgt_replicas.size());
	std::vector<int>::iterator it;
	std::sort(src_replicas.begin(),src_replicas.end());
	std::sort(tgt_replicas.begin(),tgt_replicas.end());
	it = std::set_intersection(src_replicas.begin(), src_replicas.end(), tgt_replicas.begin(), tgt_replicas.end(), intersection.begin());
	intersection.resize(it-intersection.begin()); 
	
	int dc_to_place = -1;
	int tag = -1; //
	float LARGE_FLOAT = 1e18;
	/* if(intersection.size() > 0){
		tag = 0;
		float lowest_time = LARGE_FLOAT;
		for(int i=0; i<intersection.size(); i++){
			if(std::find(notmoveto.begin(),notmoveto.end(),intersection[i]) == notmoveto.end()){
				float gathertime = 0;
				float timeup, timedown, gatherold;
				timeup = ;
				timedown = ;
				gathertime = timeup > timedown ? timeup : timedown;
				gathertime = gathertime > gatherold ? gathertime : gatherold;
				gathertime *= ();
			}
		}
	}else{
		
	} */
	return std::pair<int,int>(dc_to_place,tag);
}

std::pair<int,int> Optimizer::CostWeightedMinTimeAssign(int src, int tgt, std::vector<int> notmoveto, std::vector<int> loads){
	std::vector<int> src_replicas = (*dag->g).myvertex[src]->replica_location;
	std::vector<int> tgt_replicas = (*dag->g).myvertex[tgt]->replica_location;
	std::vector<int> intersection(src_replicas.size()+tgt_replicas.size());
	std::vector<int>::iterator it;
	std::sort(src_replicas.begin(),src_replicas.end());
	std::sort(tgt_replicas.begin(),tgt_replicas.end());
	it = std::set_intersection(src_replicas.begin(), src_replicas.end(), tgt_replicas.begin(), tgt_replicas.end(), intersection.begin());
	intersection.resize(it-intersection.begin()); 
	
	int dc_to_place = -1;
	int tag = -1; //
	float LARGE_FLOAT = 1e18;
	return std::pair<int,int>(dc_to_place,tag);
}
