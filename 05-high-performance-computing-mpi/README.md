# High-Performance Computing with MPI

## Overview
This module explores **High-Performance Computing (HPC)** using the **Message Passing Interface (MPI)** in C. It covers fundamental distributed memory programming concepts, ranging from basic point-to-point communication to advanced parallel algorithms and deadlock avoidance strategies.

## ⚙️ Prerequisites & Setup

To compile and run these MPI C programs, you need an MPI toolchain such as **Open MPI**.

### 1. Install Open MPI
* **Linux (Ubuntu/Debian):** Install via the package manager:
    ```bash
    sudo apt install libopenmpi-dev openmpi-bin
    ```
* **macOS:** Install via [Homebrew](https://brew.sh):
    ```bash
    brew install open-mpi
    ```
* **Windows:** It is highly recommended to use **WSL** (Windows Subsystem for Linux) and follow the Linux instructions.

### 2. Project Setup & Compilation
1. **Clone the Repository:**
    ```bash
    git clone [https://github.com/cienzman/networked-software-tutorials.git](https://github.com/cienzman/networked-software-tutorials.git)
    ```
2. **Compile an Example:** Use the `mpicc` wrapper:
    ```bash
    mpicc ./src/hello_world.c -o hello_world
    ```
3. **Run with Multiple Processes:** Use `mpirun` (e.g., for 4 processes):
    ```bash
    mpirun -np 4 ./hello_world
    ```

---

## Code Structure & Concepts

The examples are categorized by the architectural patterns and communication strategies they demonstrate.

### 1. Basic Communication Patterns
* **`ping_pong.c`**: A simple example of `MPI_Send` and `MPI_Recv` between two processes. It highlights how alternating roles prevents deadlocks.
* **`ring.c`**: Implements a logical ring topology where a message (number of hops) is incremented and passed sequentially until it returns to the root process.

### 2. Communication Safety & Deadlock Avoidance
These files analyze common pitfalls in parallel programming and how to resolve them.
* **`deadlock.c`**: Demonstrates a classic deadlock where both processes call the blocking synchronous `MPI_Ssend` simultaneously and wait indefinitely.
* **`deadlock_async.c`**: Explores the risks of standard `MPI_Send`. It explains that while small messages might be buffered, large ones can trigger synchronous behavior and lead to deadlocks.
* **`deadlock_avoid_isend.c`**: Resolves deadlocks using **Non-blocking** communication (`MPI_Isend`). It uses `MPI_Wait` to ensure the communication is complete before finalizing.
* **`deadlock_avoid_sendrecv.c`**: Uses the atomic `MPI_Sendrecv` primitive to combine sending and receiving into a single, safe operation.

### 3. Parallel Algorithms & Patterns
* **`monte_carlo.c`**: Parallel estimation of $\pi$ using random point simulation. It utilizes independent random seeds based on process rank and aggregates results with `MPI_Reduce`.
* **`filter.c`**: Implements a **Master-Worker** pattern where worker processes filter local data and send variable-sized arrays back to the master. It uses `MPI_Bcast` for parameters and `MPI_Probe` with `MPI_Get_count` to handle dynamic message sizes.
* **`linear_road.c`**: Simulates car movement across segments. It demonstrates neighbor communication and periodic global synchronization via `MPI_Reduce`, `MPI_Barrier`, and `MPI_Send`.

### 4. Advanced Scientific Simulation
* **`temperature_simulation.c`**: A 1D heat distribution simulation using domain decomposition.
    * **Halo Exchange:** Uses `MPI_Sendrecv` to swap boundary "ghost cells" between neighboring ranks.
    * **Convergence:** Employs `MPI_Allreduce` to determine global min/max temperature differences to decide when to stop.

---

## How to Run

1. **Navigate** to the MPI directory.
2. **Compile** the specific source file:
    ```bash
    mpicc monte_carlo.c -o monte_carlo
    ```
3. **Execute** across multiple cores (e.g., 2 or 4):
    ```bash
    mpirun -np 4 ./monte_carlo
    ```

**Testing for Deadlocks:**
* Running `deadlock` with `mpirun -np 2` will cause the program to hang.
* Running `deadlock_avoid_isend` or `deadlock_avoid_sendrecv` will complete successfully.