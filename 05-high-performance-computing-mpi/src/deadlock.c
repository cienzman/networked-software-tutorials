#include <mpi.h>
#include <stdio.h>

// Run with two processes.
// Process 0 sends an integer to process 1 and vice-versa.
// Try to run the system: what goes wrong?
int main(int argc, char** argv) {
  MPI_Init(NULL, NULL);

  int my_rank;
  MPI_Comm_rank(MPI_COMM_WORLD, &my_rank);
  int other_rank = 1 - my_rank;

  int msg_to_send = 1;
  int msg_to_recv;
  /*
  * MPI_Ssend it is blocking and synchronous: 
  * In synchronous mode, the sending process waits not just for the message to be buffered,
  *     but for an acknowledgment from the receiver that the message has been fully received;
  *     it means that the other process should receive the message before the first process can continue.
  * - Process 0 calls MPI_Ssend and blocks, waiting for Process 1 to call MPI_Recv.
  * - Process 1 calls MPI_Ssend and blocks, waiting for Process 0 to call MPI_Recv.
  * So what happens is that both process 1 and process 0 are blocked on the MPI_Ssend and they cannot post the MPI_Recv.
  * The programm will not terminate because they are waiting each other.
  */
  MPI_Ssend(&msg_to_send, 1, MPI_INT, other_rank, 0, MPI_COMM_WORLD); //
  MPI_Recv(&msg_to_recv, 1, MPI_INT, other_rank, MPI_ANY_TAG, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
  
  MPI_Finalize();
}
