/*
 * distrograph.c Base implementation of Revolver/Spinner graph partitioning algorithms
 * (c) Mohammad H. Mofrad, 2017
 * (e) mohammad.hmofrad@pitt.edu
 */

#include "libgraph.h"
int main(int argc, char *argv[])
{
    //if(strcmp(argv[8],"t")==0){
    // printf("%s\n","yes");}
    int i = 0; // num_nodes/thread index
    //int j = 0; // mapped_nodes index
    //int k = 0; // num_partitions index
    printf("最新版\n");

    srand(time(NULL));
    struct timeval t1, t2;
    double elapsedTime;

    if (((argc != 14) && (argc != 16)) || !(!strcmp(argv[4], "spinner") || !strcmp(argv[4], "revolver") || !strcmp(argv[4], "random") || !strcmp(argv[4], "hash") || !strcmp(argv[4], "range") || !strcmp(argv[4], "pagerank")))
    {
        fprintf(stderr, "USAGE: %s -n <num_partitions> -a <revolver|spinner|random|hash|range|pagerank> -f [<edge_file_name> <comment_style> <delimiter> <columns>] -v [<vetex_file_name> <delimiter>] thb_k_rate output_file_name\n", argv[0]);
        for (i = 0; i < argc; i++)
            fprintf(stderr, "argv[%2d]: %s\n", i, argv[i]);

        exit(EXIT_FAILURE);
    }
    for (i = 0; i < argc; i++)
            fprintf(stderr, "argv[%2d]: %s\n", i, argv[i]);

    thb_k_rate = atof(argv[12]);
    char file_name[MAX_PATH_LEN];
    memset(file_name, '\0', sizeof(file_name));
    snprintf(file_name, MAX_PATH_LEN, "%s/%s", "outputs", argv[13]);

    output_file = fopen(file_name, "w");
    if (!output_file)
    {
        fprintf(stderr, "Error: outputfile() %s\n", file_name);
        exit(EXIT_FAILURE);
    }
    fprintf(output_file, "%s  %s  %s\n", argv[6], argv[12], argv[10]);

    // Allocate graph data structure
    graph = malloc(sizeof(struct Graph));
    if (!graph)
        exit(EXIT_FAILURE);
    memset(graph, 0, sizeof(struct Graph));

    char comment[strlen(argv[7])];
    memset(comment, '\0', strlen(argv[7]));
    memcpy(comment, argv[7], strlen(argv[7]));

    char delimiter[strlen(argv[8])];
    memset(delimiter, '\0', strlen(argv[8]));
    memcpy(delimiter, argv[8], strlen(argv[8]));

    int columns = atoi(argv[9]);
    (void)columns;

    /*char directed[strlen(argv[10])];
    memset(directed, '\0', strlen(argv[10]));
    memcpy(directed, argv[10], strlen(argv[10]));
    printf("%s\n",directed);*/
    /*

    int status = edge_open_file_mmapped(argv[6], argv[7], argv[8], atoi(argv[9]));
    if(status == EXIT_ERROR)
	{
		fprintf(stderr, "Error: edge_open_file_mmapped()\n");
        exit(EXIT_FAILURE);
	}   
*/
    /*    
    int status = para_edge_open_file(argv[6], argv[7], argv[8], atoi(argv[9]));
    if(status == EXIT_ERROR)
	{
		fprintf(stderr, "Error: para_edge_open_file()\n");
        exit(EXIT_FAILURE);
	}
 */

    // Read input file

    int status = edge_open_file(argv[6], argv[7], argv[8], atoi(argv[9]));
    if (status == EXIT_ERROR)
    {
        fprintf(stderr, "Error: open_edge_file()\n");
        exit(EXIT_FAILURE);
    }

    // Refine node indicces
    // status = vertex_map();

    // if (status == EXIT_ERROR)
    // {
    //     fprintf(stderr, "Error: vertex_map()\n");
    //     exit(EXIT_FAILURE);
    // }
    
    //print_graph();
    //exit(0);
    //init_adjacent(argv[6]);
    //init_network("Network.txt");

    double dataunit = 0.000008;

    // if (!strcmp(argv[10], "pagerank"))
    // {
    //     dataunit = 0.000008;
    //     // calculate_time_cost("true", dataunit);
    // }
    // else if (!strcmp(argv[10], "subgraph"))
    // {
    //     dataunit = 0.001;
    //     // calculate_time_cost("false", dataunit);
    // }
    // else if (!strcmp(argv[10], "sssp"))
    // {
    //     dataunit = 0.000004;
    //     // calculate_time_cost("true", dataunit);
    // }

    if (!strcmp(argv[4], "pagerank"))
    {
        printf("Sort the vertex file before calling open_vertex_file(%s)\n", argv[11]);
        printf("    sort -k 1 -n  %s\n", argv[11]);
        status = vertex_open_file(argv[11], argv[12]);
        if (status == EXIT_ERROR)
        {
            fprintf(stderr, "Error: open_file()\n");
            exit(EXIT_FAILURE);
        }
    }

    graph->algorithm = malloc(sizeof(struct Algorithm));
    if (!graph->algorithm)
        exit(EXIT_FAILURE);
    memset(graph->algorithm, 0, sizeof(struct Algorithm));
    struct Algorithm *algorithm = graph->algorithm;
    algorithm->num_partitions = atoi(argv[2]);
    memset(algorithm->name, 0, sizeof(algorithm->name));
    strcpy(algorithm->name, argv[4]);
    memset(algorithm->dir_name, 0, sizeof(algorithm->dir_name));
    strcpy(algorithm->dir_name, dirname(argv[0]));
    memset(algorithm->base_name, 0, sizeof(algorithm->base_name));
    strcpy(algorithm->base_name, basename(argv[6]));

    //printf("%s  %s\n",algorithm->dir_name,algorithm->base_name);
    //agetchar();
    algorithm->l = 1;     // lambda
    algorithm->c = 1;  // load imbalance
    algorithm->o = 0.001; // omega
    algorithm->e = 5;     // epsilon
    //printOUtandIn();
    //init_adjacent(argv[6]);

    init_network(argv[11]);
    //print_edges_per_vertex();
    //exit(0);
    printf("123\n");
    // LA alpha and beta parameters
    algorithm->alpha = 1; // LA reward signal
    algorithm->beta = .1; // LA penalty signal

    if (strcmp(argv[4], "pagerank"))
    {
        printf("456");
        status = algorithm_initialization(argv[10]); 
        if (status == EXIT_ERROR)
        {
            fprintf(stderr, "Error: algorithm_initialization()\n");
            exit(EXIT_FAILURE);
        }
    }
    
    // return 0;

    int k1;
    unsigned int k1_num;
    int k2;
    unsigned int k2_num;
    multiprocessing_pool(graph->num_nodes, MAX_THREADS_NUM, &k1, &k1_num, &k2, &k2_num);

    pthread_t tid[MAX_THREADS_NUM];
    memset(tid, 0, sizeof(tid));
    struct pthread_args_struct *args = NULL;

    int iteration = 0;
    int ep = algorithm->e;
    float score_old = .0;

    

    // fprintf(output_file, "Before train:\n");

    

    gettimeofday(&t1, NULL);
    while (iteration < MAX_ITER)
    {
        iteration++;
        printf("iteration: %d\n", iteration);
        sentinel = 0;
        score_old = score;
        score = .0;
        pthread_barrier_init(&barrier, NULL, MAX_THREADS_NUM);
        for (i = 0; i < MAX_THREADS_NUM; i++)
        {
            args = malloc(sizeof(struct pthread_args_struct) * MAX_THREADS_NUM);
            if (!args)
                return (EXIT_FAILURE);
            memset(args, 0, sizeof(struct pthread_args_struct));
            args->iteration = iteration;
            args->index = i;

            if (i < k1)
            {
                args->low = i * k1_num;
                args->high = ((i + 1) * k1_num) - 1;
            }
            else if ((i >= k1) && (i < k1 + k2))
            {
                args->low = (k1 * k1_num) + ((i - k1) * k2_num);
                args->high = (k1 * k1_num) + (((i + 1) - k1) * k2_num) - 1;
            }

            if (!strcmp(algorithm->name, "revolver"))
                status = pthread_create(&tid[i], NULL, (void *)revolver, (void *)args);
            else if (!strcmp(algorithm->name, "spinner"))
                status = pthread_create(&tid[i], NULL, (void *)spinner, (void *)args);
            else if (!strcmp(algorithm->name, "random"))
                status = pthread_create(&tid[i], NULL, (void *)random_partitioning, (void *)args);
            else if (!strcmp(algorithm->name, "hash"))
                status = pthread_create(&tid[i], NULL, (void *)hash_partitioning, (void *)args);
            else if (!strcmp(algorithm->name, "range"))
                status = pthread_create(&tid[i], NULL, (void *)range_partitioning, (void *)args);
            else if (!strcmp(algorithm->name, "pagerank"))
                status = pthread_create(&tid[i], NULL, (void *)pagerank, (void *)args);

            if (status != EXIT_SUCCESS)
            {
                fprintf(stderr, "Error: pthread_create()\n");
                exit(EXIT_FAILURE);
            }
        }

        for (i = 0; i < MAX_THREADS_NUM; i++)
        {
            pthread_join(tid[i], NULL);
        }

        /*    printf("-------------------------------------------------------------------------\n");
            int w=0;
            for(w=0;w<algorithm->num_partitions-1;w++)
            printf("%f ",graph->nodes[1].signal_[w]);
            printf("%f\n",graph->nodes[1].signal_[w]);
            printf("-------------------------------------------------------------------------\n");*/
        
        status = computation_is_halted(score, score_old, &ep);
        if (status == 0)
        {
            printf("Computation has been converged.\n");
            break;
        }
        if ((!strcmp(argv[4], "pagerank")) && iteration == 3)
        {
            printf("Pagerank is done\n");
            break;
        }
        //getchar();
    }
    gettimeofday(&t2, NULL);
    /*
    long long j = 0;
    for(i = 0; i < graph->num_nodes; i++){
        j = graph->mapped_nodes[i];
        printf("%lli %d\n", j, graph->nodes[j].label);
    }*/

    // status = write_partition();
    // if (status == EXIT_ERROR)
    // {
    //     fprintf(stderr, "Error: write_partition()\n");
    //     exit(EXIT_FAILURE);
    // }

    /*
    status = write_partition_e(argv[6], comment, delimiter, columns);
    if(status == EXIT_ERROR)
	{
		fprintf(stderr, "Error: write_partition_e()\n");
        exit(EXIT_FAILURE);
	}
	printf("[x] %s (%d)\n", algorithm->name, status);
*/
    //print_graph();
    
    
    // fprintf(output_file, "\n\nTrain finished:\n");
    if (!strcmp(argv[10], "pagerank"))
    {
        dataunit = 0.000008;
        calculate_time_cost("true", dataunit);
    }
    else if (!strcmp(argv[10], "subgraph"))
    {
        dataunit = 0.001;
        calculate_time_cost("false", dataunit);
    }
    else if (!strcmp(argv[10], "sssp"))
    {
        dataunit = 0.000004;
        calculate_time_cost("true", dataunit);
    }
    // calculate_time_cost("true", dataunit);

    for(i = 0; i < graph->num_nodes; i++)
        graph->nodes[graph->mapped_nodes[i]].ori_label = graph->nodes[graph->mapped_nodes[i]].label;

    //calculate_time_cost(directed,dataunit);
    //free_all();

    // compute and print the elapsed time in milliseconds and seconds

    elapsedTime = (t2.tv_sec - t1.tv_sec) * 1000;    // sec to ms
    elapsedTime += (t2.tv_usec - t1.tv_usec) / 1000; // us to ms
    printf("%.0f milliseconds | %.2lf seconds\n", elapsedTime, elapsedTime / 1000);
    
    
    fprintf(output_file, "\t%f\n", elapsedTime / 1000);
    //calculate_time_cost();
    int t = graph->ignore_edge_size;
    struct Integer *source = &graph->ignore_edge_source;
    struct Integer *dest = &graph->ignore_edge_dest;

    printf("Begin loading edge...\n");
    thb_k_rate = thb_k_rate * 100 / 30;
    while (t--)
    {


        source = source->next;
        dest = dest->next;

        if(1. * rand() / RAND_MAX > thb_k_rate)
            continue;

        int S = source->v;
        int D = dest->v;

        if (!graph->nodes[S].ohead)
        {
            graph->nodes[S].ohead = malloc_Integer(D);
            graph->nodes[S].otail = graph->nodes[S].ohead;
        }
        else
        {
            graph->nodes[S].otail->next = malloc_Integer(D);
            graph->nodes[S].otail = graph->nodes[S].otail->next;
        }

        if (!graph->nodes[D].ihead)
        {
            graph->nodes[D].ihead = malloc_Integer(S);
            graph->nodes[D].itail = graph->nodes[D].ihead;
        }
        else
        {
            graph->nodes[D].itail->next = malloc_Integer(S);
            graph->nodes[D].itail = graph->nodes[D].itail->next;
        }

        graph->nodes[S].degree++;
        graph->nodes[D].idegree++;

        graph->num_alloced_edges++;

        
    }

    printf("#num_alloced_nodes: %lli\n", graph->num_alloced_nodes);
    printf("#num_nodes:         %lli\n", graph->num_nodes);
    printf("#num_edges:         %lli\n", graph->num_edges);
    printf("#num_alloced_edges: %lli\n", graph->num_alloced_edges);

    
    // fprintf(output_file, "\n\nLoad edges finished:\n");
    // fprintf(output_file, "Train finished:\n");
    if (!strcmp(argv[10], "pagerank"))
    {
        dataunit = 0.000008;
        calculate_time_cost("true", dataunit);
    }
    else if (!strcmp(argv[10], "subgraph"))
    {
        dataunit = 0.001;
        calculate_time_cost("false", dataunit);
    }
    else if (!strcmp(argv[10], "sssp"))
    {
        dataunit = 0.000004;
        calculate_time_cost("true", dataunit);
    }
    // calculate_time_cost("true", dataunit);
    fprintf(output_file, "\n");

    printf("************Begin retraing*************\n");
    // algorithm->l = 1;     // lambda
    // algorithm->c = 1;  // load imbalance
    // algorithm->o = 0.001; // omega
    // algorithm->e = 5;     // epsilon
    
    // ep = algorithm->e;
    score_old = .0;
    iteration = 0;


    
    gettimeofday(&t1, NULL);
    while (iteration < MAX_ITER)
    {
        iteration++;
        printf("iteration: %d\n", iteration);
        sentinel = 0;
        score_old = score;
        score = .0;
        pthread_barrier_init(&barrier, NULL, MAX_THREADS_NUM);
        for (i = 0; i < MAX_THREADS_NUM; i++)
        {
            args = malloc(sizeof(struct pthread_args_struct) * MAX_THREADS_NUM);
            if (!args)
                return (EXIT_FAILURE);
            memset(args, 0, sizeof(struct pthread_args_struct));
            args->iteration = iteration;
            args->index = i;

            if (i < k1)
            {
                args->low = i * k1_num;
                args->high = ((i + 1) * k1_num) - 1;
            }
            else if ((i >= k1) && (i < k1 + k2))
            {
                args->low = (k1 * k1_num) + ((i - k1) * k2_num);
                args->high = (k1 * k1_num) + (((i + 1) - k1) * k2_num) - 1;
            }

            if (!strcmp(algorithm->name, "revolver"))
                status = pthread_create(&tid[i], NULL, (void *)revolver, (void *)args);
            else if (!strcmp(algorithm->name, "spinner"))
                status = pthread_create(&tid[i], NULL, (void *)spinner, (void *)args);
            else if (!strcmp(algorithm->name, "random"))
                status = pthread_create(&tid[i], NULL, (void *)random_partitioning, (void *)args);
            else if (!strcmp(algorithm->name, "hash"))
                status = pthread_create(&tid[i], NULL, (void *)hash_partitioning, (void *)args);
            else if (!strcmp(algorithm->name, "range"))
                status = pthread_create(&tid[i], NULL, (void *)range_partitioning, (void *)args);
            else if (!strcmp(algorithm->name, "pagerank"))
                status = pthread_create(&tid[i], NULL, (void *)pagerank, (void *)args);

            if (status != EXIT_SUCCESS)
            {
                fprintf(stderr, "Error: pthread_create()\n");
                exit(EXIT_FAILURE);
            }
        }

        for (i = 0; i < MAX_THREADS_NUM; i++)
        {
            pthread_join(tid[i], NULL);
        }

        /*    printf("-------------------------------------------------------------------------\n");
            int w=0;
            for(w=0;w<algorithm->num_partitions-1;w++)
            printf("%f ",graph->nodes[1].signal_[w]);
            printf("%f\n",graph->nodes[1].signal_[w]);
            printf("-------------------------------------------------------------------------\n");*/

        status = computation_is_halted(score, score_old, &ep);
        if (status == 0)
        {
            printf("Computation has been converged.\n");
            break;
        }
        if ((!strcmp(argv[4], "pagerank")) && iteration == 3)
        {
            printf("Pagerank is done\n");
            break;
        }
        //getchar();
    }
    gettimeofday(&t2, NULL);
    elapsedTime = (t2.tv_sec - t1.tv_sec) * 1000;    // sec to ms
    elapsedTime += (t2.tv_usec - t1.tv_usec) / 1000; // us to ms

    
    // fprintf(output_file, "\n\nTrain finished:\n");
    
    if (!strcmp(argv[10], "pagerank"))
    {
        dataunit = 0.000008;
        calculate_time_cost("true", dataunit);
    }
    else if (!strcmp(argv[10], "subgraph"))
    {
        dataunit = 0.001;
        calculate_time_cost("false", dataunit);
    }
    else if (!strcmp(argv[10], "sssp"))
    {
        dataunit = 0.000004;
        calculate_time_cost("true", dataunit);
    }
    // calculate_time_cost("true", dataunit);
    

    printf("************Retrain finished*************\n");
    printf("%.0f milliseconds | %.2lf seconds\n", elapsedTime, elapsedTime / 1000);

    fprintf(output_file, "\t%lf\n", elapsedTime / 1000);
    fclose(output_file);
    return (EXIT_SUCCESS);
}
