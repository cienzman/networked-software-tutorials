#include <stdio.h>
#include <stdlib.h>
#include <time.h>
#include <mpi.h>

/*
    This C program uses the Monte Carlo method and Message Passing Interface (MPI) to estimate the value of pi in parallel.
    The core idea is to simulate random points in a square and count how many fall inside the inscribed quarter-circle.
    The ratio of the counts gives an estimate of pi.

    1: Consider a square with corners at $(0, 0)$ and $(1, 1)$. Its area is $1 \times 1 = 1$.
    2: Inscribe a quarter-circle in this square, centered at $(0, 0)$, with a radius $R=1$. The area of this quarter-circle is $\frac{1}{4}\pi R^2 = \frac{\pi}{4}$.
    3: If you randomly drop a large number of points ($N$) into the square, the ratio of the points that fall inside the quarter-circle ($C$) to the total number of points ($N$) is approximately equal to the ratio of their areas:


*/


const int num_iter_per_proc = 10 * 1000 * 1000;


int main() {
  MPI_Init(NULL, NULL);
    
  int rank;
  int num_procs;
  int sum;
  
  MPI_Comm_rank(MPI_COMM_WORLD, &rank);
  MPI_Comm_size(MPI_COMM_WORLD, &num_procs);


  srand(time(NULL) + rank); // Init random number generator; Adding the rank ensures that each process starts with a different random seed, producing independent random number sequences for parallel execution.

  double x, y;
  int count = 0;

  for (int i = 0; i < num_iter_per_proc; i++){
    x = ((double) rand()) / RAND_MAX; // RAND_MAX is a pre-defined macro in C (stdlib.h)
    y = ((double) rand()) / RAND_MAX;
    if ((x*x)+(y*y) <= 1.0){
      count++;
    }
  }

  MPI_Reduce(&count, &sum, 1, MPI_INT, MPI_SUM, 0, MPI_COMM_WORLD);
  
  if (rank == 0) {
    double pi = (4.0*sum) / (num_iter_per_proc*num_procs);
    printf("Pi = %f\n", pi);
  }
    
  MPI_Finalize();
  return 0;
}
