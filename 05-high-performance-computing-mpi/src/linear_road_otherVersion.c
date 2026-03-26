#include <mpi.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

/* - Notice that this implementations is not equivalent to that of linear_road.c since in the following program we are assuming that each process manage every segments of the street:
        -- here every process manage their own cars in each segments from 0 to num_segments-1
        -- so for every process some cars enters in the street
        -- for every process some cars left the street
        -- for every process some cars move from a generic segment i to the next i+1
    - On the other hand linear_road.c is implemented such that the first process manage the first segments_per_proc segments, the second process manage the segments from segments_per_proc
      to segments_per_proc+segments_per_proc-1 and so on...

*/


// Set DEBUG 1 if you want car movement to be deterministic
#define DEBUG 0


const int num_segments = 256;


const int num_iterations = 1000;
const int count_every = 10;


const double alpha = 0.5;
const int max_in_per_sec = 10;


// Returns the number of car that enter the first segment at a given iteration.
int create_random_input() {
#if DEBUG
  return 1;
#else
  return rand() % max_in_per_sec;
#endif
}


// Returns 1 if a car needs to move to the next segment at a given iteration, 0 otherwise.
int move_next_segment() {
#if DEBUG
  return 1;
#else
  return rand() < alpha ? 1 : 0;
#endif
}


int main(int argc, char** argv) { 
  MPI_Init(NULL, NULL);


  int rank;
  int num_procs;
  MPI_Comm_rank(MPI_COMM_WORLD, &rank);
  MPI_Comm_size(MPI_COMM_WORLD, &num_procs);
  srand(time(NULL) + rank);
  
  // define and init/allocate variables
  int *my_car = (int *) malloc(sizeof(int) * num_segments);
  memset( (void *) my_car, 0, num_segments * sizeof(int));

  int *new_car = (int *) malloc(sizeof(int) * num_segments);
  memset( (void *) new_car, 0, num_segments * sizeof(int));

  // Simulate for num_iterations iterations
  for (int it = 0; it < num_iterations; ++it) {

    // Move cars across segments (only from the second it since in the first it there are no cars)
    if(it > 0) {
      for(int k = 0; k < num_segments; k++){
        new_car[k] = my_car[k];
      }
      // iterate over all the segments
      for(int i = 0; i < num_segments; i++){
        //iterate over all the cars in a segment
        for(int j = 0; j < my_car[i]; j++){
          int move = move_next_segment(); 
          if(move){ 
            new_car[i]--; //this car is moving away from segment i
            if(i != num_segments - 1){ 
              new_car[i+1]++; //this car is enetering segment i+1 (only if the next segment actually exists)
            }

          }

        }
        
      }
      // my_car = new_car
      for(int i = 0; i < num_segments; i++){
        my_car[i] = new_car[i];
      }

    }

    // New cars may enter in the first segment
    int entering_cars = create_random_input();
    my_car[0] += entering_cars;
    //printf("Process: %d, Iteration: %d, my_car[0]: %d\n",rank, it, my_car[0]);



    // When needed, compute the overall sum
    if (it%count_every == 0) {
      int global_sum = 0;
      int process_sum = 0;
      for(int i = 0; i < num_segments; i++){ //  compute global sum
        process_sum += my_car[i];
      }
      //printf("Process: %d, Iteration: %d, process_sum: %d\n",rank, it, process_sum);
      MPI_Reduce(&process_sum, &global_sum, 1, MPI_INT, MPI_SUM, 0, MPI_COMM_WORLD);

      if (rank == 0) {
	      printf("Iteration: %d, sum: %d\n", it, global_sum);
      }
    }
    
    MPI_Barrier(MPI_COMM_WORLD);
  }

  // deallocate dynamic variables
  free(my_car);
  free(new_car);
  MPI_Barrier(MPI_COMM_WORLD);
  MPI_Finalize();
}
