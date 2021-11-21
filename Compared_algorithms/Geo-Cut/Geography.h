#ifndef GEOGRAPHY_H
#define GEOGRAPHY_H

#include <string>
#include <iostream>
#include <fstream>
using namespace std;

#endif
//cluster different countries into a small number of cloud regions
extern ofstream fileResult;
enum Amazon_EC2_regions
{
	useast=0,
	uswest_oregon=1,
	uswest_northcalifornia=2,
	eu_ireland=3,
//	eu_frankfurt,
	ap_singapore=4,
	ap_tokyo=5,
//	ap_seoul,
	ap_sydney=6,
	saeast=7,
	sim1,
	sim2,
	sim3,
	sim4,
	sim5,
	sim6,
	sim7,
	sim8,
	sim9,
	sim10,
	sim11,
	sim12,
	//num_regions
	east_asia,
	jp_west,
	west_eu,
	east_us
};


inline Amazon_EC2_regions location_to_region(string location,string graph_indicator){
		
	Amazon_EC2_regions region;
	/**
	* for the live journal graph, there are in total 244 uniq countries
	* we have mapped the countries to their geographic regions 
	* there are in total 13 unique regions
	//Antarctic
	//Caribbean Islands
	//East Asia
	//Europe
	//Mesoamerica
	//North Africa
	//North America
	//North Asia
	//Oceania
	//South America
	//South & South East Asia
	//Sub-Saharan Africa
	//West & Central Asia
	*/
	if (graph_indicator.compare("livejournal") == 0){
		if (location.compare("Antarctic") == 0){
			region = ap_sydney;
		}
		else if (location.compare("Caribbean Islands") == 0){
			region = useast;
		}
		else if (location.compare("East Asia") == 0){
			region = ap_tokyo;
		}		
		else if (location.compare("Europe") == 0){
			region = eu_ireland;
		}
		else if (location.compare("Mesoamerica") == 0){
			region = uswest_northcalifornia;
		}
		else if (location.compare("North Africa") == 0){
			region = eu_ireland;
		}
		else if (location.compare("North America") == 0){
			region = uswest_oregon;
		}
		else if (location.compare("North Asia") == 0){
			region = ap_tokyo;
		}
		else if (location.compare("Oceania") == 0){
			region = ap_sydney;
		}
		else if (location.compare("South America") == 0){
			region = saeast;
		}
		else if (location.compare("South & South East Asia") == 0){
			region = ap_singapore;
		}
		else if (location.compare("Sub-Saharan Africa") == 0){
			region = saeast;
		}
		else if (location.compare("West & Central Asia") == 0){
			region = ap_tokyo;
		}
		else{
			//std::cout << "none recorded region: " << location << std::endl;
			fileResult << "none recorded region :" <<location << "\n";
			exit(1);
		}

		return region;
	}
	/**
	* for the twitter graph
	* it's more complicated as the locations are not standarized
	* so the first step is to standarize the location strings
	*/
	if (graph_indicator.compare("twitter") == 0){
		
		region = location_to_region(location, "livejournal");
		return region;
	}

	//printf("location to region transformation is customized for livejournal and twitter graphs only\n");
	fileResult << "location to region transformation is customized for livejournal and twitter graphs only\n";
	
	exit(1);
};