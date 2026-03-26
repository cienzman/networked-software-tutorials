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

  // Scatter the random numbers from process 0 to all processes
  MPI_Scatter(global_arr, num_elements_per_proc, MPI_INT, local_arr, num_elements_per_proc, MPI_INT, 0, MPI_COMM_WORLD);

  // Compute the average of local array
  float local_average = compute_average(local_arr, num_elements_per_proc);

  // Declare a variable on P0 to hold the final sum of averages
  
  float global_sum_of_averages;


  // Reduce (sum) all local_average values into global_sum_of_averages on P0
  MPI_Reduce(&local_average, &global_sum_of_averages, 1, MPI_FLOAT, MPI_SUM, 0, MPI_COMM_WORLD);
  
  // Compute the final average in process 0
  if (my_rank == 0) {
    // Final result is the sum of partial averages divided by the number of processes (world_size)
    float result = global_sum_of_averages / world_size;
    printf("The average is %f\n", result);
    
    // Sequential code to check correctness
    float sequential_result = compute_average(global_arr, num_elements_per_proc * world_size);
    printf("The average (sequential computation) is %f\n", sequential_result);
  }


  // Clean up
  if (my_rank == 0) {
    free(global_arr);
  }
  free(local_arr);

  MPI_Barrier(MPI_COMM_WORLD);
  MPI_Finalize();
}

/*
    The difference with respect to the original version (average.c) is:
        - MPI_Gather (Original approach): Collects all individual values into an array on P0, and P0 performs the final calculation manually.
        - MPI_Reduce (Alternative approach): Performs the aggregation (the sum) in a distributed manner as the values are collected, and only the single final summed value is returned to P0.
             This is often more efficient for large numbers of processes.
*/