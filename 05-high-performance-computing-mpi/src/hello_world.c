#include <mpi.h>
#include <stdio.h>

/*
  Notice that everything between init and finalize is executed in parallel on each processor we start the program with.
*/

int main(int argc, char** argv) {
  // Init the MPI environment
  MPI_Init(NULL, NULL);

  // Get the number of processes
  int world_size;
  MPI_Comm_size(MPI_COMM_WORLD, &world_size); //get number of processor within this communicator: MPI_COMM_WORLD is the comunicator that includes all the processes we start the program with

  // Get the rank of the process
  int world_rank;
  MPI_Comm_rank(MPI_COMM_WORLD, &world_rank);

  // Get the name of the processor
  char processor_name[MPI_MAX_PROCESSOR_NAME];
  int name_len;
  MPI_Get_processor_name(processor_name, &name_len);

  // Print off a hello world message
  printf("Hello world from processor %s (rank %d out of %d)\n", processor_name, world_rank, world_size);

  // Finalize the MPI environment
  MPI_Finalize();
}


/*
  In the directory: you@you:~/your_path/NSDS_MPI_tutorial 
          mpicc src/hello_world.c -o hello_world      // compile
          mpirun hello_world                          // run: by default it creates one process for each core available on your machine
                                                            // the order in which the processor are executed is not guaranteed.

*/
