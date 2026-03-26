const int num_iter_per_proc = 10 * 1000 * 1000;


int main() {
  MPI_Init(NULL, NULL);
    
  int rank;
  int num_procs;
  int sum;
  
  MPI_Comm_rank(MPI_COMM_WORLD, &rank);
  MPI_Comm_size(MPI_COMM_WORLD, &num_procs);


  srand(time(NULL) + rank); // Init random number generator; Adding the rank ensures that each process starts with a different random seed, producing independent random number sequences for parallel execution.

  double x, y;
  int count = 0;

  for (int i = 0; i < num_iter_per_proc; i++){
    x = ((double) rand()) / RAND_MAX; // RAND_MAX is a pre-defined macro in C (stdlib.h)
    y = ((double) rand()) / RAND_MAX;
    if ((x*x)+(y*y) <= 1.0){
      count++;
    }
  }

  int *array_result = NULL;
  if(rank == 0){
    array_result = (int *) malloc(sizeof(int)*num_procs);
  }
  
  MPI_Gather(&count, 1, MPI_INT, array_result, 1, MPI_INT, 0, MPI_COMM_WORLD);
  
  if (rank == 0) {
    sum = 0;
    for(int i=0; i<num_procs; i++){
        sum+= array_result[i];
    }
    double pi = (4.0*sum) / (num_iter_per_proc*num_procs);
    printf("Pi = %f\n", pi);

    free(array_result);
  }
    
  MPI_Finalize();
  return 0;
}
