#include <stdio.h>
#include <stdlib.h>
#include <time.h>
#include <mpi.h>

// Creates an array of random numbers
int *create_random_array(int num_elements, int max_value) {
  int *arr = (int *) malloc(sizeof(int)*num_elements);
  for (int i=0; i<num_elements; i++) {
    arr[i] = (rand() % max_value);
  }
  return arr;
}

// Computes the average value
float compute_average(int *array, int num_elements) {
  int sum = 0;
  for (int i=0; i<num_elements; i++) {
    sum += array[i];
  }
  return ((float) sum) / num_elements;
}

// Computes final average value
float compute_final_average(float *array, int num_elements) {
  float sum = 0.0f;
  for (int i=0; i<num_elements; i++) {
    sum += array[i];
  }
  return  sum/num_elements;
}

int main(int argc, char** argv) {
  // Number of elements for each processor
  int num_elements_per_proc = 1000;
  if (argc > 1) {
    num_elements_per_proc = atoi(argv[1]);
  }

  // Init random number generator
  srand(time(NULL));

  MPI_Init(NULL, NULL);

  int my_rank, world_size; 
  MPI_Comm_rank(MPI_COMM_WORLD, &my_rank);
  MPI_Comm_size(MPI_COMM_WORLD, &world_size);

  // Process 0 creates the array
  int *global_arr = NULL;
  if (my_rank == 0) {
    global_arr = create_random_array(num_elements_per_proc * world_size, 100);
  }

  // For each process, create a receive buffer
  int *local_arr = (int *) malloc(sizeof(int) * num_elements_per_proc);

  // Scatter the random numbers from process 0 to all processes: global_arr is splitted and send to receivers
  MPI_Scatter(global_arr, num_elements_per_proc, MPI_INT, local_arr, num_elements_per_proc, MPI_INT, 0, MPI_COMM_WORLD);

  // Compute the average of local array
  float local_average = compute_average(local_arr, num_elements_per_proc);

  // Gather all partial results in process 0
  float *gather_buffer = NULL;
  if (my_rank == 0) {
    gather_buffer = (float *) malloc(sizeof(float) * world_size); // P0 allocates a gather_buffer large enough to hold one float from every process.
  }
  MPI_Gather(&local_average, 1, MPI_FLOAT, gather_buffer, 1, MPI_FLOAT, 0, MPI_COMM_WORLD); // All processes send their single local_average value to the root.

  // Compute the final average in process 0
  if (my_rank == 0) {
    float result = compute_final_average(gather_buffer, world_size);
    printf("The average is %f\n", result);

    // Sequential code to check correctness
    float sequential_result = compute_average(global_arr, num_elements_per_proc * world_size);
    printf("The average (sequential computation) is %f\n", sequential_result);
  }

  // Clean up
  if (my_rank == 0) {
    free(global_arr);
    free(gather_buffer);
  }
  free(local_arr);

  MPI_Barrier(MPI_COMM_WORLD);
  MPI_Finalize();
}

/*
- MPI_Scatter: is a collective operation. The data is distributed from the root buffer (global_arr) to the receive buffer (local_arr) of every process in the communicator,
   including the root itself.
   P0 keeps the first chunk of the global_arr in its own local_arr while P1 gets the second chunk, P2 the third chunk and so on.
   Notice that the first 3 parameters refers to the sender; The other parameters refer to all processes P0 included.
    P0 uses the global_arr argument as its send buffer and local_arr argument as its receive buffer while all the other processes ignore arguments related to
     the sending (send buffer (global_arr) and the send count (num_elements_per_proc).)
   Indeed MPI standard allows the send arguments (global_arr and the send count) to be set to a null pointer on all non-root processes. Therefore, setting global_arr = NULL
    for all non-root processes is correct and standard practice, as they only need a valid local_arr to receive the data.

- MPI_Gather: ollective operation where all processes participate, and the root is both the collector and a contributor.
    -- All processes (P0, P1, P2, ...) send their local_average to the root process (P0) and the root process (P0) places its own local_average into the first slot of its gather_buffer.


*/