#include <mpi.h>
#include <stdio.h>

// This program avoids deadlocking by using the Sendrecv primitive
int main(int argc, char** argv) {
  MPI_Init(NULL, NULL);

  int my_rank;
  MPI_Comm_rank(MPI_COMM_WORLD, &my_rank);
  int other_rank = 1 - my_rank;

  int msg_to_send = 1;
  int msg_to_recv;

  /*
     Single and combined primitive specifically designed to perform a send and a receive operation atomically (as a single, non-divisible action).
     - &msg_to_send	&msg_to_recv : buffer; The memory address of the data to send/receive.
     - 1, 1 : The number of elements (one integer).
     - MPI_INT, MPI_INT : The type of data being sent/received.
     - other_rank, MPI_ANY_SOURCE : Where to send (the partner) / From where to receive (any process).
     - 0, MPI_ANY_TAG : The message identifier / Accept any tag.

     - Process 0 attempts to send its msg_to_send to Process 1 while simultaneously attempting to receive a message from Process 1 into its msg_to_recv variable. Process 1 does the exact opposite.
  */
  MPI_Sendrecv(&msg_to_send, 1, MPI_INT, other_rank, 0, &msg_to_recv, 1, MPI_INT, MPI_ANY_SOURCE, MPI_ANY_TAG, MPI_COMM_WORLD, MPI_STATUS_IGNORE);

  printf("Process %d received message %d\n", my_rank, msg_to_recv);

  /*
    MPI_Barrier: forces all participating processes (in this case, all processes in MPI_COMM_WORLD) to stop and wait for every other process to also reach that same point in the code.
  */
  MPI_Barrier(MPI_COMM_WORLD); 
  MPI_Finalize();
}