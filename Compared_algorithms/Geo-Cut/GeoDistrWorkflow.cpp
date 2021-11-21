// GeoDistrWorkflow.cpp : Defines the entry point for the console application.
//#include <windows.h>
//#include<unistd.h>
#include <time.h>
#include "Simulator.h"
#include <ctime>
#include <string>
#include <string.h>
#include <fstream>
#include <iostream>
#include <iomanip>
#include <unistd.h>
#include <time.h>
float OnDemandLag = 60; //seconds
//float budget;
//int Max_Iteration;//for graph algorithms to converge
int Max_Monte_Carlo; //for monte carlo simulation
int N_THREADS;
int numofdcs;
ofstream fileResult;

int main(int argc, char *argv[])
{
	std::vector<DataCenter *> DCs;
	time_t time1, time2;
	time_t tt = time(NULL);
	tm *t = localtime(&tt);
	time(&time1);
	clock_t start, finish;
	start = clock();
	string txtName = "txt/";
	for (int i = 0; i < 14; i++)
		txtName += argv[i];
	string txt = ".txt";
	txtName += txt;
	fileResult.open(txtName);
	fileResult << t->tm_year + 1900 << "-" << t->tm_mon + 1 << "-" << t->tm_mday << "  " << t->tm_hour << ":" << t->tm_min << ":" << t->tm_sec << "\n";

	if (strcmp(argv[9], "real") == 0)
	{				  //for Amazon EC2 experiments
		numofdcs = 8; // real cloud
		for (int i = 0; i < numofdcs; i++)
		{
			DataCenter *DC = new DataCenter(Amazon_EC2_regions(i));
			DC->id = i;
			DC->location = Amazon_EC2_regions(i);
			if (i == 0)
			{
				DC->download_band = 100; //usually it's 500
				DC->upload_band = 500;
			}
			else if (i == 5)
			{
				DC->download_band = 100; //usually it's 500
				DC->upload_band = 50;
			}
			DCs.push_back(DC);
		}
	}
	else if (strcmp(argv[9], "azure") == 0)
	{
		numofdcs = 4;
		for (int i = 0; i < numofdcs; i++)
		{
			DataCenter *DC = new DataCenter(Amazon_EC2_regions(20 + i));
			DC->id = i;
			DC->location = Amazon_EC2_regions(20 + i);
			DCs.push_back(DC);
		}
	}
	else if (strcmp(argv[9], "azure1") == 0)
	{
		numofdcs = 3;
		for (int i = 0; i < numofdcs; i++)
		{
			DataCenter *DC = new DataCenter(Amazon_EC2_regions(20 + i));
			DC->id = i;
			DC->location = Amazon_EC2_regions(20 + i);
			DCs.push_back(DC);
		}
	}
	else if (strcmp(argv[9], "test") == 0)
	{
		numofdcs = 4; // test the heterogeneities in prices and bandwidth
		for (int i = 0; i < numofdcs; i++)
		{
			DataCenter *DC = new DataCenter();
			DC->id = i;
			DC->location = Amazon_EC2_regions(i);
			DCs.push_back(DC);
		}
		if (strcmp(argv[11], "case1") == 0)
		{
			DCs[0]->upload_band *= 2.0;
			DCs[0]->download_band *= 2.0;
			DCs[0]->upload_price *= 2.0;
		}
		else if (strcmp(argv[11], "case2") == 0)
		{
			DCs[0]->upload_band *= 2.0;
			DCs[0]->download_band *= 2.0;
			DCs[0]->upload_price *= 0.5;
		}
		else if (strcmp(argv[11], "case3") == 0)
		{
			DCs[0]->upload_band *= 0.5;
			DCs[0]->download_band *= 0.5;
			DCs[0]->upload_price *= 2.0;
		}
		else if (strcmp(argv[11], "case4") == 0)
		{
			DCs[0]->upload_band *= 0.5;
			DCs[0]->download_band *= 0.5;
			DCs[0]->upload_price *= 0.5;
		}
	}
	else
	{
		numofdcs = 20; // simulation
		for (int i = 0; i < numofdcs; i++)
		{
			DataCenter *DC = new DataCenter(Amazon_EC2_regions(1));
			DC->id = i;
			DC->location = Amazon_EC2_regions(1);
			DCs.push_back(DC);
			//delete DC;
		}
		if (strcmp(argv[9], "homo") == 0)
		{
			//all dcs have the same bandwidth with us east
			//dc 10-19 have half of the upload price
			for (int i = 10; i < numofdcs; i++)
			{
				DCs[i]->upload_price *= 0.5;
				//DCs[i]->upload_band = DCs[i]->download_band;
			}
		}
		else if (strcmp(argv[9], "medium") == 0)
		{
			//dc 10-14 have half of the bandwidth
			//dc 10-19 have half of the upload price
			for (int i = 10; i < 15; i++)
			{
				DCs[i]->upload_band *= 0.5;
				DCs[i]->download_band *= 0.5;
			}
			for (int i = 10; i < numofdcs; i++)
			{
				DCs[i]->upload_price *= 0.5;
			}
		}
		else if (strcmp(argv[9], "high") == 0)
		{
			//dc 10-14 have the 1/4 of the bandwidth
			//dc 15-19 have the half of the bandwidth
			//dc 10-19 have half of the upload price
			for (int i = 10; i < numofdcs; i++)
			{
				DCs[i]->download_band *= 0.5;
				DCs[i]->upload_band *= 0.5;
				DCs[i]->upload_price *= 0.5;
			}
			for (int i = 10; i < 15; i++)
			{
				DCs[i]->download_band *= 0.5;
				DCs[i]->upload_band *= 0.5;
			}
		}
		else if (strcmp(argv[9], "top") == 0)
		{
			//dc 10-14 have the 1/8 of the bandwidth
			//dc 15-19 have the 1/4 of the bandwidth
			//dc 10-19 have half of the upload price
			for (int i = 10; i < numofdcs; i++)
			{
				DCs[i]->download_band *= 0.5;
				DCs[i]->upload_band *= 0.5;
				DCs[i]->upload_price *= 0.5;
			}
			for (int i = 10; i < 15; i++)
			{
				DCs[i]->download_band *= 0.25;
				DCs[i]->upload_band *= 0.25;
			}
		}
		else if (strcmp(argv[9], "phomo") == 0)
		{
			//dc 10-14 have 1/4 of the bandwidth
			//dc 15-19 have 1/2 of the bandwidth
			//all dcs have the same upload price
			for (int i = 10; i < numofdcs; i++)
			{
				DCs[i]->download_band *= 0.5;
				DCs[i]->upload_band *= 0.5;
			}
			for (int i = 10; i < 15; i++)
			{
				DCs[i]->download_band *= 0.5;
				DCs[i]->upload_band *= 0.5;
			}
		}
		else if (strcmp(argv[9], "phigh") == 0)
		{
			//dc 10-14 have 1/4 of the bandwidth and 1/4 of the upload price
			//dc 15-19 have 1/2 of the bandwidth and 1/2 of the upload price
			for (int i = 10; i < numofdcs; i++)
			{
				DCs[i]->download_band *= 0.5;
				DCs[i]->upload_band *= 0.5;
			}
			for (int i = 10; i < 15; i++)
			{
				DCs[i]->download_band *= 0.5;
				DCs[i]->upload_band *= 0.5;
			}
			for (int i = 10; i < 20; i++)
			{
				DCs[i]->upload_price *= 0.5;
			}
			for (int i = 10; i < 15; i++)
			{
				DCs[i]->upload_price *= 0.5;
			}
		}
		else if (strcmp(argv[9], "pmedium") == 0)
		{
			for (int i = 10; i < numofdcs; i++)
			{
				DCs[i]->download_band *= 0.5;
				DCs[i]->upload_band *= 0.5;
			}
			for (int i = 10; i < 15; i++)
			{
				DCs[i]->download_band *= 0.5;
				DCs[i]->upload_band *= 0.5;
			}
			for (int i = 10; i < 15; i++)
			{
				DCs[i]->upload_price *= 0.5;
			}
			for (int i = 15; i < 20; i++)
			{
				DCs[i]->upload_price *= 0.75;
			}
		}
		else if (strcmp(argv[9], "phighorignal") == 0)
		{
			//dc 10-14 have 1/4 of the bandwidth and 1/8 of the upload price
			//dc 15-19 have 1/2 of the bandwidth and 1/4 of the upload price
			for (int i = 10; i < numofdcs; i++)
			{
				DCs[i]->download_band *= 0.5;
				DCs[i]->upload_band *= 0.5;
			}
			for (int i = 10; i < 15; i++)
			{
				DCs[i]->download_band *= 0.5;
				DCs[i]->upload_band *= 0.5;
			}
			for (int i = 10; i < 20; i++)
			{
				DCs[i]->upload_price *= 0.25;
			}
			for (int i = 10; i < 15; i++)
			{
				DCs[i]->upload_price *= 0.25;
			}
		}
		else if (strcmp(argv[9], "budgetacc") == 0)
		{
			for (int i = 10; i < 20; i++)
			{
				DCs[i]->download_band *= 0.5;
				DCs[i]->upload_band *= 0.5;
				DCs[i]->upload_price *= 0.5;
			}
			for (int i = 15; i < 20; i++)
			{
				DCs[i]->download_band *= 0.5;
				DCs[i]->upload_band *= 0.5;
				DCs[i]->upload_price *= 0.5;
			}
		}
		else if (strcmp(argv[9], "budgetdis") == 0)
		{
			for (int i = 10; i < 20; i++)
			{
				DCs[i]->download_band *= 0.5;
				DCs[i]->upload_band *= 0.5;
				DCs[i]->upload_price *= 2;
			}
			for (int i = 15; i < 20; i++)
			{
				DCs[i]->download_band *= 0.5;
				DCs[i]->upload_band *= 0.5;
				DCs[i]->upload_price *= 2;
			}
		}
	}

	int degree = atoi(argv[3]); //graph size for synthetic
	distributed_graph *dag = new distributed_graph();

	if (strcmp(argv[1], "livejournal") == 0)
	{
		dag->load_live_journal_graph();
	}
	else if (strcmp(argv[1], "twitter") == 0)
	{
		dag->load_twitter_graph();
	}
	else if (strcmp(argv[1], "orkut") == 0)
	{
		dag->load_orkut_graph();
	}
	else if (strcmp(argv[1], "uk") == 0)
	{
		dag->load_uk_graph();
	}
	else if (strcmp(argv[1], "it") == 0)
	{
		dag->load_it_graph();
	}
	else if (strcmp(argv[1], "file") == 0)
	{
		string filename;
		if (degree == 10000)
		{
			filename = "powerlaw-10000-2.1";
		}
		else if (degree = 100)
		{
			filename = "powerlaw-100-2.1";
		}
		else if (degree == 50000)
		{
			filename = "powerlaw-50000-2.1";
		}
		else if (degree == 100000)
		{
			filename = "powerlaw-100000-2.1";
		}
		else if (degree == 1000000)
		{
			filename = "powerlaw-1000000-2.1";
		}
		dag->load_synthetic_file(filename.c_str());
	}
	else if (strcmp(argv[1], "facebook") == 0)
	{
		dag->load_facebook_graph();
	}
	else if (strcmp(argv[1], "googleweb") == 0)
	{
		dag->load_googleweb_graph();
	}
	else if (strcmp(argv[1], "wiki") == 0)
	{
		dag->load_wiki_graph();
	}
	else if (strcmp(argv[1], "roadca") == 0)
	{
		dag->load_roadnet_graph();
	}
	else if (strcmp(argv[1], "p2p") == 0)
	{
		dag->load_p2p_graph();
	}
	else if (strcmp(argv[1], "test") == 0)
	{
		// dag->test_graph();
	}
	dag->vectorSort(); //sort  vector
	// dag->mutexlocks = std::vector<LockV>(dag->num_vertices,LockV());
	// dag->mutexlocks = std::vector<std::mutex>(dag->num_vertices);
	int Max_Iteration = atoi(argv[4]); //if==0, converge with threshold
	Max_Monte_Carlo = atoi(argv[5]);
	float budget = atof(argv[6]) / 1;
	N_THREADS = atoi(argv[7]);

	BaseApp *myapp;
	if (strcmp(argv[2], "pagerank") == 0)
	{
		myapp = new PageRank();
		myapp->mytype = pagerank;
		for (int v = 0; v < dag->num_vertices; v++)
		{
			(*dag->g).myvertex[v]->data = new PageRankVertexData();
			(*dag->g).myvertex[v]->accum = new PageRankVertexData();
		}
	}
	else if (strcmp(argv[2], "sssp") == 0)
	{
		myapp = new SSSP();
		myapp->mytype = sssp;
		for (int v = 0; v < dag->num_vertices; v++)
		{
			(*dag->g).myvertex[v]->data = new SSSPVertexData();
			(*dag->g).myvertex[v]->accum = new SSSPVertexData();
			dynamic_cast<SSSPVertexData *>((*dag->g).myvertex[v]->data)->routed_nodes.push_back(v);
		}

		/*the source node to find the path*/
		if (dag->graph_type == test)
		{
			myapp->sources.push_back(0);
		}
		else
		{
			/*select the one with the largest degree as source*/
			int degree = 0, sourceid = 0;
			for (int i = 0; i < dag->num_vertices; i++)
			{
				int curr_degree = dag->get_in_nbrs(i).size() + dag->get_out_nbrs(i).size();
				if (curr_degree > degree)
				{
					degree = curr_degree;
					sourceid = i;
				}
			}
			myapp->sources.push_back(sourceid);
		}
		for (int i = 0; i < myapp->sources.size(); i++)
		{
			dynamic_cast<SSSPVertexData *>((*dag->g).myvertex[myapp->sources[i]]->data)->dist = 0;
		}
	}
	else if (strcmp(argv[2], "subgraph") == 0)
	{
		myapp = new SubgraphIsom();
		myapp->mytype = subgraph;
		for (int v = 0; v < dag->num_vertices; v++)
		{
			(*dag->g).myvertex[v]->data = new SubgraphVertexData();
			(*dag->g).myvertex[v]->accum = new SubgraphVertexData();
		}
		/* the pattern to be matched, no selfcycle
		*  without loss of generosity, we consider structural match 
		*  criteria: if vg matches vp, 
		*			 the adjacent edges of vg must be the superset of vp¡¯s adjacent edges
		*/
		myapp->pattern_graph = new Graph();
		int num_vertex = 3;
		for (int i = 0; i < num_vertex; i++)
		{
			MyVertex *v = new MyVertex();
			v->vertex_id = i; //starting from 0
			//add_vertex(*v, *myapp->pattern_graph);  //use push_back
			myapp->pattern_graph->myvertex.push_back(v);
		}

		if (num_vertex == 2)
		{
			//pipeline (pattern size = 2)
			//add_edge(0, 1, 10, *myapp->pattern_graph);
		}
		else if (num_vertex == 3)
		{
			//triangle (pattern size = 3)
			//add_edge(0, 1, 10, *myapp->pattern_graph);
			myapp->pattern_graph->myvertex[0]->out_neighbour.push_back(1);
			myapp->pattern_graph->myvertex[1]->in_neighbour.push_back(0);
			//add_edge(0, 2, 10, *myapp->pattern_graph);
			myapp->pattern_graph->myvertex[0]->out_neighbour.push_back(2);
			myapp->pattern_graph->myvertex[2]->in_neighbour.push_back(0);
		}
		else if (num_vertex == 4)
		{
			//square (pattern size = 4)
			//add_edge(0, 1, 10, *myapp->pattern_graph);
			//add_edge(0, 2, 10, *myapp->pattern_graph);
			//add_edge(1, 3, 10, *myapp->pattern_graph);
		}
	}
	float max_price_dc = DCs[0]->upload_price;
	int max_price_dc_index = 0;
	for(int i = 1; i < DCs.size(); i++)
		if(max_price_dc < DCs[i]->upload_price)
			max_price_dc = DCs[i]->upload_price, max_price_dc_index = i;
	
	double total_move_cost = 0;
	for(int i = 0; i < (*dag->g).myvertex.size(); i++)
	if((*dag->g).myvertex[i]->location_id != max_price_dc_index)
		total_move_cost += (*dag->g).myvertex[i]->data->input_size * DCs[(*dag->g).myvertex[i]->location_id]->upload_price;

	myapp->budget = budget * total_move_cost / 1000;
	cout << "budget = " << budget << " total_move_cost = " << total_move_cost << "  budget_ = " << myapp->budget << endl;
	// myapp->budget = budget;
	myapp->ITERATIONS = Max_Iteration;
	myapp->global_graph = dag;

	//apply the replication plan to the vertices
	Optimizer *optimizer = new Optimizer();
	optimizer->myapp = myapp;
	optimizer->dag = dag;
	optimizer->DCs = DCs;
	EngineType type = synchronous;
	GraphEngine *engine = new GraphEngine(type, DCs.size());
	engine->myapp = myapp;
	engine->myopt = optimizer;
	engine->DCs = DCs;

	//streaming policy
	Stream_Policy p;
	if (strcmp(argv[10], "cost") == 0)
	{
		p = Stream_Policy(0);
	}
	else if (strcmp(argv[10], "time") == 0)
	{
		p = Stream_Policy(1);
	}
	else if (strcmp(argv[10], "balanced") == 0)
	{
		p = Stream_Policy(2);
	}
	else if (strcmp(argv[10], "wan") == 0)
	{
		p = Stream_Policy(3);
	}

	// std::clock_t starttime, endtime;
	time_t starttime, endtime;
	float timeelapsed;

	if (strcmp(argv[8], "random") == 0)
	{
		//printf("testing random edge distribution\n");
		fileResult << "testing random edge distriution\n"
				   << "\n";
		time(&starttime);
		cout << starttime << endl;
		optimizer->RandomReplication();
		time(&endtime);
		cout << endtime << endl;
		timeelapsed = difftime(endtime, starttime);
		printf("optimization overhead for random replication is %f seconds\n", timeelapsed);
		fileResult << "optimization overhead for random replication is " << timeelapsed << " seconds\n";
		engine->Simulate();
	}
	else if (strcmp(argv[8], "greedy") == 0)
	{
		/*load balancing vertex-cut (PowerGraph)*/
		//printf("testing load balancing vertex-cut\n");
		fileResult << "testing load balancing vertex-cut\n";
		time(&starttime);
		cout << starttime << endl;
		optimizer->GreedyReplication();
		time(&endtime);
		cout << endtime << endl;
		timeelapsed = difftime(endtime, starttime);
		printf("optimization overhead for load balancing vertex-cut is %f seconds\n", timeelapsed);
		fileResult << "optimization overhead for load balancing vertex-cut is " << timeelapsed << " seconds\n";
		engine->Simulate();
	}
	else if (strcmp(argv[8], "hcut") == 0)
	{
		/*heuristic hybrid cut (Powerlyra)*/
		//printf("testing hybrid cut\n");
		fileResult << "testing hybrid cut\n";
		time(&starttime);
		optimizer->HPartition();
		time(&endtime);
		timeelapsed = difftime(endtime, starttime);
		printf("optimization overhead for hybrid cut is %f seconds\n", timeelapsed);
		fileResult << "optimization overhead for hybrid cut is " << timeelapsed << " seconds\n";
		engine->Simulate();
	}
	else if (strcmp(argv[8], "streaming") == 0)
	{
		printf("testing with our method\n");
		fileResult << "testing with our method\n";
		//time(&starttime);
		optimizer->InputDataMovement();
		optimizer->StreamEdgeAssignment(p);
		time(&endtime);
		timeelapsed = difftime(endtime, starttime);
		printf("overhead of edge assignment is %f seconds\n", timeelapsed);
		fileResult << "overhead of edge assignment is " << timeelapsed << " seconds\n";
		//printf("simulation without partition placement\n");
		fileResult << "simulation without partition placement\n";
		engine->Simulate();
	}
	else if (strcmp(argv[8], "placement") == 0)
	{
		//printf("testing with our method\n");
		fileResult << "testign with our method\n";
		time(&starttime);
		//optimizer->InputDataMovement();
		optimizer->StreamEdgeAssignment(p);
		//optimizer->PartitionPlacement();
		optimizer->SampleBasedPartitionPlacement();
		time(&endtime);
		timeelapsed = difftime(endtime, starttime);
		printf("optimization overhead of partition placement is %f seconds\n", timeelapsed);
		fileResult << "optimizaiton overhead of partition placement is " << timeelapsed << " seconds\n";
		engine->Simulate();
	}
	else if (strcmp(argv[8], "migration") == 0)
	{
		fileResult << "testing with our method\n";
		time(&starttime);
		//optimizer->InputDataMovement();	  //nothing
		optimizer->StreamEdgeAssignment(p);
		optimizer->PartitionPlacement();
		optimizer->EdgeMigration(0);
		time(&endtime);
		timeelapsed = difftime(endtime, starttime);
		printf("optimization overhead including edge migration is %f seconds\n", timeelapsed);
		fileResult << "optimization overhead including edge migration is " << timeelapsed << " seconds\n";
		engine->Simulate();
	}
	else if (strcmp(argv[8], "costmigration") == 0)
	{
		for (int i = 0; i < DCs.size(); i++)
			cout << "DC " << i
				 << "'s uploadband = " << DCs[i]->upload_band
				 << ", downloadband = " << DCs[i]->download_band
				 << ", upload price = " << DCs[i]->upload_price
				 << ", download price = " << DCs[i]->download_price
				 << endl;
		time(&starttime);
		optimizer->StreamEdgeAssignment(p);
		cout << "Finished StreamEdgeAssignment." << endl;

		time(&endtime);
		timeelapsed = difftime(endtime, starttime);
		printf("StreamEdgeAssignment's overhead is %f seconds\n", timeelapsed);

		optimizer->SampleBasedPartitionPlacement();
		cout << "Finished SampleBasedPartitionPlacement." << endl;

		time(&endtime);
		timeelapsed = difftime(endtime, starttime);
		printf("SampleBasedPartitionPlacement's overhead is %f seconds\n", timeelapsed);

		optimizer->EdgeMigration(1);
		cout << "Finished EdgeMigration." << endl;
		time(&endtime);
		timeelapsed = difftime(endtime, starttime);
		printf("overhead is %f seconds\n", timeelapsed);
		fileResult << "optimization overhead  is " << timeelapsed << " seconds\n";
		engine->Simulate();
	}
	//std::cout<<"breakpoint1"<<std::endl;
	for (int i = 0; i < DCs.size(); ++i)
	{
		delete DCs[i];
	}
	//std::cout<<"breakpoint3"<<std::endl;

	fileResult.close();

	return 0;
}
