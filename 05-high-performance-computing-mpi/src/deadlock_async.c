#include <mpi.h>
#include <stdio.h>

// Run with two processes.
// Process 0 sends an integer to process 1 and vice-versa.
int main(int argc, char** argv) {
  MPI_Init(NULL, NULL);

  int my_rank;
  MPI_Comm_rank(MPI_COMM_WORLD, &my_rank);
  int other_rank = 1 - my_rank;

  int msg_to_send = 1;
  int msg_to_recv;

  MPI_Send(&msg_to_send, 1, MPI_INT, other_rank, 0, MPI_COMM_WORLD); 
  MPI_Recv(&msg_to_recv, 1, MPI_INT, other_rank, MPI_ANY_TAG, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
  
  MPI_Finalize();
}

/*
    The behavior of this program, which uses MPI_Send followed by MPI_Recv in a two-process exchange, is not guaranteed to result in a deadlock
     but it is highly risky and generally considered unsafe for this pattern. It exhibits undefined behavior and will likely fail (deadlock or hang)
      in many real-world MPI implementations.

    MPI_Send is the standard mode blocking send.
    The MPI standard allows the implementation to decide how an MPI_Send operates based on the size of the message and system resources:

    - If the message is small (usually a few kilobytes, depending on the implementation), the MPI library might internally buffer the message.
        -- Process 0 calls MPI_Send. The message is copied into an internal buffer, and MPI_Send returns immediately to Process 0.
        -- Process 1 calls MPI_Send. The message is copied into an internal buffer, and MPI_Send returns immediately to Process 1.
        -- Both processes then proceed to their respective MPI_Recv calls and communication is successful, and the program terminates correctly.
    
    - If the message is large or if the MPI implementation is configured to be more conservative, the library might not buffer the message.
         Instead, it might transition to a synchronous handshake mechanism, similar to MPI_Ssend.
         -- Process 0 calls MPI_Send and blocks, waiting for Process 1 to post a matching MPI_Recv. 
         -- Process 1 calls MPI_Send and blocks, waiting for Process 0 to post a matching MPI_Recv.
         -- Since bothh processes are blocked on their sends, neither can reach the MPI_Recv call --> deadlock
*/