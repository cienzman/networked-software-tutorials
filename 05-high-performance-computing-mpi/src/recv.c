#include <mpi.h>
#include <stdio.h>

/*
  Let's analyze how MPI behaves when we post a blocking receive.

*/

// Every process p>0 waits for an interger from process p=0 that never arrives.
// Try to check the use of resources.
int main(int argc, char** argv) {
  MPI_Init(NULL, NULL);

  int my_rank;
  MPI_Comm_rank(MPI_COMM_WORLD, &my_rank);

  /*
  * Since the process 0 is doing nothing, the following process we'll wait forever.
  */
  if (my_rank > 0) {
    int buf;
    /*
    * -  blocking receive only returns when the data has arrived and is ready for use by the program
    *     -- the caller is blocked until we received the message.
    * - 1: we are waiting for one message.
    * - MPI_INT: the message we are waiting for is of type Integer.
    * - 0: the message comes from process 0.
    * - MPI_ANY_TAG: any tag is ok.
    * - MPI_COMM_WORLD: we are considering the world communicator.
    * - MPI_STATUS_IGNORE: we are ignoring the status of MPI_Recv.
    */
    MPI_Recv(&buf, 1, MPI_INT, 0, MPI_ANY_TAG, MPI_COMM_WORLD, MPI_STATUS_IGNORE); 
  }

  MPI_Finalize();
}
/*
NSDS_MPI_tutorial$ mpicc ./src/recv.c -o recv
NSDS_MPI_tutorial$ mpirun recv

nothing happens. Moreover if we open another terminal and we run the command:
  top 
  we can observe the resources: processes are occupied 100% of cpu.
  Indeed MPI tries to maximize the use of resources and does not want to wait for communication:
     so the MPI_Recv is implemented as busy waits: the process continuosly ask for bytes to the network



*/
