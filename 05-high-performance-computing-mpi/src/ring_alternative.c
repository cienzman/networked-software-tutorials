#include <mpi.h>
#include <stdio.h>
#include <stdlib.h>

// Send a message in a ring: every process sends its rank to the next process in the ring and receives from the previous one the previous one's kank.
int main(int argc, char *argv[]) {
    
    const int total_ring_iterations = 1; // Total number of times the token should pass through the whole ring (P0 -> P1 -> ... -> P0)
    int numtasks, rank, next, prev, token_value, tag1=1;
    MPI_Request reqs[2];
    MPI_Status stats[2];

    MPI_Init(&argc, &argv);
    MPI_Comm_size(MPI_COMM_WORLD, &numtasks);
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);

    prev = rank - 1;
    next = rank + 1;

    if (rank == 0) prev = numtasks - 1;
    if (rank == (numtasks - 1)) next = 0;
    
    MPI_Irecv(&token_value, 1, MPI_INT, prev, tag1, MPI_COMM_WORLD, &reqs[0]);
    MPI_Isend(&rank, 1, MPI_INT, next, tag1, MPI_COMM_WORLD, &reqs[1]);

    /* Do some work here */

    MPI_Waitall(2, reqs, stats);

    printf("Process %d sent msg with token_value = %d\n", rank, rank);
    printf("Process %d received msg with token_value = %d\n", rank, token_value);
    MPI_Finalize();
}