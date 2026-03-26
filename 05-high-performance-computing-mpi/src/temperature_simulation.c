#include <mpi.h>
#include <stdio.h>
#include <stdlib.h>
#include <math.h>
#include <string.h>


const double L = 100.0;                 // Length of the 1d domain
const int n = 1000;                     // Total number of points
const int iterations_per_round = 1000;  // Number of iterations for each round of simulation
const double allowed_diff = 0.001;      // Stopping condition: maximum allowed difference between values
const int default_tag = 0;

double initial_condition(double x, double L) {
    return fabs(x - L / 2);
}

void send_and_receive(int rank, int size, double *my_points, int local_n,
    double *before_first, double *after_last) {
    MPI_Status status;

    // Initialize halos with something meaningful for boundary processes
    *before_first = 0.0;
    *after_last = 0.0;

    // If there is a left neighbor: send my first to left neighbor and receive its last into before_first
    if (rank > 0) {
    MPI_Sendrecv(&my_points[0], 1, MPI_DOUBLE, rank - 1, default_tag,
        before_first, 1, MPI_DOUBLE, rank - 1, default_tag,
        MPI_COMM_WORLD, &status);
    }

    // If there is a right neighbor: send my last to right neighbor and receive its first into after_last
    if (rank < size - 1) {
    MPI_Sendrecv(&my_points[local_n - 1], 1, MPI_DOUBLE, rank + 1, default_tag,
        after_last, 1, MPI_DOUBLE, rank + 1, default_tag,
        MPI_COMM_WORLD, &status);
    }
}

int main(int argc, char **argv) {
    MPI_Init(&argc, &argv);


    int rank, size;
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    MPI_Comm_size(MPI_COMM_WORLD, &size);

    // Variables declaration and initialization
    int local_n = n / size;
    double dx = L / (n - 1);
    double *my_points = malloc(sizeof(double) * local_n);
    double *next_points = malloc(sizeof(double) * local_n);
    double before_first = 0.0, after_last = 0.0;

    // Set initial conditions
    for(int i = 0; i < local_n; i++) {
        double x = (rank * (local_n) + i) * dx;
        my_points[i] = initial_condition(x, L);
    }
    
    int round = 0;
    while (1) {
        // Perform one round of iterations 
        round++;
        for (int t = 0; t < iterations_per_round; t++) {
            // Exchange values between processes
            send_and_receive(rank, size, my_points, local_n, &before_first, &after_last);

            // Update Rules
            for(int p = 0; p < local_n; p++){

                // global first point t[0]
                if(rank * (local_n) + p == 0){ 
                    next_points[p] = ( my_points[p] + my_points[p+1] ) / 2.0;
                }
                // global lasr point t[n-1]
                else if(rank * (local_n) + p == n - 1){ 
                    next_points[p] = ( my_points[p] + my_points[p-1] ) / 2.0;
                }
                // global intermediate points
                else { 
                    // interior points: consider local neighbors where possible
                    double left_val, right_val;

                    if (p == 0) {
                        // left neighbor is in previous rank -> use before_first
                        left_val = before_first;
                    } else {
                        left_val = my_points[p - 1];
                    }

                    if (p == local_n - 1) {
                        // right neighbor is in next rank -> use after_last
                        right_val = after_last;
                    } else {
                        right_val = my_points[p + 1];
                    }

                    next_points[p] = (left_val + my_points[p] + right_val) / 3.0;
                }
                
            }
            // Swap buffers (copy next into my_points)
            double *tmp = my_points;
            my_points = next_points;
            next_points = tmp;
        }
        
        // Compute local minimum and maximum
        double local_min = my_points[0] ;
        double local_max = my_points[0];
        for (int p = 1; p < local_n; ++p) {
            if (my_points[p] < local_min) local_min = my_points[p];
            if (my_points[p] > local_max) local_max = my_points[p];
        }
        // Compute global minimum and maximum
        double global_min, global_max, max_diff;
        MPI_Allreduce(&local_min, &global_min, 1, MPI_DOUBLE, MPI_MIN, MPI_COMM_WORLD);
        MPI_Allreduce(&local_max, &global_max, 1, MPI_DOUBLE, MPI_MAX, MPI_COMM_WORLD);
        
        
        max_diff = global_max - global_min;
        if (rank == 0) {
            printf("Round: %d\tMin: %.5f\tMax: %.5f\tDiff: %.5f\n", round, global_min, global_max, max_diff);
        }
        // Implement stopping conditions (break)
        if(max_diff < allowed_diff ){
            break;
        }
    }

    // Deallocation
    free(my_points);
    free(next_points);


    MPI_Finalize();
    return 0;

}
