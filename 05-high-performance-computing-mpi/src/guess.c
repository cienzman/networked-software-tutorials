#include <mpi.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <math.h>
#include <limits.h>

#define DEBUG 0

int rank;
int num_procs;

const int num_rounds = 5;
const int min_num = 1;
const int max_num = 1000;

// The leader for the current round 
int leader = 0;

// Per-process selection buffer (one int per process) 
int *selected_numbers = NULL;

// Leaderboard maintained on rank 0: wins per process 
int *leaderboard = NULL;

// Select a random number between min_num and max_num (inclusive) 
int select_number() {
    return min_num + rand() % (max_num - min_num + 1);
}

/* Compute the next leader using selected_numbers[] collected on the current leader.
   Tie-breaking: if a tie occurs (equal distance) the current nobody wins. */
int compute_next_leader(int num_to_guess, int leader) {
#if DEBUG
    printf("Number to guess: %d (on leader %d)\n", num_to_guess, leader);
    for (int i = 0; i < num_procs; ++i) {
        printf("P%d selected %d\n", i, selected_numbers[i]);
    }
#endif

    int closest = max_num;
    int next_leader = leader;

    for (int i = 0; i < num_procs; ++i) {
        int diff = abs(num_to_guess - selected_numbers[i]);
        if (diff < closest) {
            closest = diff;
            next_leader = i;
        } else if (diff == closest) {
            // keep current leader in case of tie
            next_leader = leader;
        }
    }
    return next_leader;
}

void update_leaderboard(int next_leader, int old_leader, int round) {
    if(next_leader != old_leader){
        leaderboard[next_leader]++;
    }
    printf("\n* Round %d *\n", round);
    for (int i = 0; i < num_procs; ++i) {
        printf(" process %d has won %d rounds\n", i, leaderboard[i]);
    }
}

// Allocate dynamic variables
void allocate_vars() {
  selected_numbers = (int *) malloc(num_procs * sizeof(int));
  if (rank == 0) {
    leaderboard = (int *) malloc(num_procs * sizeof(int));
    memset((void *) leaderboard, 0, num_procs * sizeof(int));
  }
}

// Deallocate dynamic variables
void free_vars() {
  free(selected_numbers);
  if (rank == 0) {
    free(leaderboard);
  }
}


//// MAIN ///////
int main(int argc, char **argv) {
    MPI_Init(NULL, NULL);
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    MPI_Comm_size(MPI_COMM_WORLD, &num_procs);

    srand(time(NULL) + rank);

    allocate_vars();

    for (int round = 0; round < num_rounds; ++round) {
        int attempt = select_number();

        // collect attempts at the current leader 
        MPI_Gather(&attempt, 1, MPI_INT, selected_numbers, 1, MPI_INT, leader, MPI_COMM_WORLD);

        int next_leader;
        if (rank == leader) {
            int num_to_guess = select_number();
            next_leader = compute_next_leader(num_to_guess, leader);
        }

        // broadcast next_leader from the leader to all ranks
        MPI_Bcast(&next_leader, 1, MPI_INT, leader, MPI_COMM_WORLD);

        if (rank == 0) {
            update_leaderboard(next_leader, leader, round);
        }

        leader = next_leader;
    }

    MPI_Barrier(MPI_COMM_WORLD);
    free_vars();
    MPI_Finalize();
}
