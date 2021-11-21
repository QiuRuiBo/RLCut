#ifndef ENTITIES_H
#define ENTITIES_H

#include "Distributed_graph.h"

#endif

extern int NUM_DC;
extern float OnDemandLag;
extern ofstream fileResult;

enum Integer_VM{
	not_ready,
	ready,
	scheduled,
	running,
	failed
};

//consider only one type of VM
class VM{
public:
	float price;
	int DC_name;
	
	MyVertex* tk; //task running on the VM
	Integer_VM status;
	float turn_on; //turn on time
	float life_time;
};


class DataCenter{
public:
	int id;
	int size;
	Amazon_EC2_regions location;
	
	std::vector<VM*> vms; //the running vms in the current datacenter
	float data_size; //the total input data size of all vertices in this dc
	// std::vector<int> contained_vertices; //global id of vertex
	/**
	* cloud setting uses net_band
	* the network bandwidth between the current DC and other DCs, ordered by DC's ID
	*/
	// std::vector<float> net_band; 	
	// std::vector<float> net_latency; //the network latency between the current DC and other DCs
	float vm_price; //hourly price of instances, only consider m1.large instances
	/** hpc setting uses upload/download bandwidth*/
	float upload_band;
	float download_band;
	float upload_price;
	float download_price;

	float g_upload_data = 0.0;
	float g_dnload_data = 0.0;
	float a_dnload_data = 0.0;
	float a_upload_data = 0.0;

	DataCenter(){ 
		// net_band = std::vector<float>(num_regions, 3.0); 
		// net_band[location_id] = 100;
		// net_latency = std::vector<float>(num_regions, 0.001);
		// net_latency[location_id] = 0;
		upload_band = 100;
		download_band = 500;
		data_size = 0;	
		upload_price = 0.02;
		download_price = 0;
	}
	DataCenter(Amazon_EC2_regions l){
		location = l;
		data_size = 0;
		upload_band = 100;
		download_band = 500;
		if (l == useast){
			vm_price = 0.175;
			upload_price = 0.02;
		}
		else if (l == uswest_northcalifornia){
			vm_price = 0.19;
			upload_price = 0.02;
		}
		else if (l == uswest_oregon){
			vm_price = 0.175;
			upload_price = 0.02;
		}
		//else if (l == ap_seoul){
		//	vm_price = 1000;//do not support m1.large type
		//	net_price = 0.08;
		//}
		else if (l == ap_singapore){
			vm_price = 0.233;
			upload_price = 0.09;
		}
		else if (l == ap_sydney){
			vm_price = 0.233;
			upload_price = 0.14;
		}
		else if (l == ap_tokyo){
			vm_price = 0.243;
			upload_price = 0.09;
		}
		//else if (l == eu_frankfurt){
		//	vm_price = 1000;//do not support m1.large type
		//	net_price = 0.02;
		//}
		else if (l == eu_ireland){
			vm_price = 0.19;
			upload_price = 0.02;
		}
		else if (l == saeast){
			vm_price = 0.233;
			upload_price = 0.16;
			}else if(l == east_asia){
			upload_price = 0.12;
			upload_band = 128;
			download_band = 165;
		}else if(l == jp_west){
			upload_price = 0.12;
			upload_band = 128;
			download_band = 165;
		}else if(l == west_eu){
			upload_price = 0.087;
			upload_band = 179;
			download_band = 295;
		}else if(l == east_us){
			upload_price = 0.087;
			upload_band = 126;
			download_band = 175;
		}
	}
};

