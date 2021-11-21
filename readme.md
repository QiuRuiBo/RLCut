# Start

```powershell
make
```

```powershell
./main [RLcut/Ginger/HashPL] [pagerank/sssp/subgraph] path_to_graph_file path_to_network_file  DC_num Budget THETA Threads path_to_output initial_samping_rate path_to_trained_file BSP Batchsize Overhead
```

For example:

```powershell
./main RLcut pagerank "/home/local_graph/livejournal" "network/amazon"  8 0.4 100 48 output/test 0.01 train/test 1 8 15
```

if you want to use the dynamic partitioning, please use "void RLcut_dynamic()" function in "lib.cpp" file and modify "void initialEdge()" to load subgraph.



