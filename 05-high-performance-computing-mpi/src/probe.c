#include <mpi.h>
#include <stdio.h>
#include <stdlib.h>

// The receiver does not know in advance the number of messages it will receive (and the source sending them).
// It dynamically gets the number of messages using a probe.
int main(int argc, char** argv) {
  MPI_Init(NULL, NULL);

  int my_rank;
  MPI_Comm_rank(MPI_COMM_WORLD, &my_rank);
  int other_rank = 1 - my_rank;

  // Sender
  if (my_rank == 0) {
    int num_msgs = 10;
    int *msgs = (int *) malloc(sizeof(int) * num_msgs); // dinamic array: allocating 10 integers
    for (int i=0; i<num_msgs; i++) { //initialize the array
      msgs[i] = i;
    }
    MPI_Send(msgs, num_msgs, MPI_INT, other_rank, 0, MPI_COMM_WORLD); //blocking send.
    free(msgs); //deallocate the memory of msgs.
  }
  // Receiver
  else {
    // Probe is a blocking operation that checks the number of messages in the receiver buffer
    MPI_Status status;
    /*
      - MPI_ANY_SOURCE: we are accepting messages from any source.
      - MPI_ANY_TAG: we are accepting any tag.
      - MPI_COMM_WORLD: we are considering the world communicator.
      - &status: when there will be a message available MPI_Probe returns and status will contain information about the incoming message (source, tag, etc.)
    */
    MPI_Probe(MPI_ANY_SOURCE, MPI_ANY_TAG, MPI_COMM_WORLD, &status);

    int num_msgs;
    MPI_Get_count(&status, MPI_INT, &num_msgs); // get the number of incoming messages of type MPI_INT --> result in num_msgs
    int source = status.MPI_SOURCE;
    int tag = status.MPI_TAG;
    printf("Process %d received %d messages from source %d with tag %d\n", my_rank, num_msgs, source, tag);

    int *msgs = (int *) malloc(sizeof(int) * num_msgs); // at this point we know how large is the message: how many integers we will receive: allocate the memory
    MPI_Recv(msgs, num_msgs, MPI_INT, source, tag, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
    for (int i=0; i<num_msgs; i++) {
      printf("%d\t", msgs[i]);
    }
    printf("\n");
    free(msgs);
  }
  
  MPI_Finalize();
}