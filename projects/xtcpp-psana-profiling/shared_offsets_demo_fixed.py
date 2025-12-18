#!/usr/bin/env python3
"""
shared_offsets_demo_fixed.py
Run with: mpirun -n <num_ranks> --hostfile myhost.txt python shared_offsets_demo_fixed.py

This version properly handles MPI shared memory across multiple nodes.
Pattern adapted from lcls2 commit 53c80f09299e8bad3abeab08926695a4b04bc7f4:
1. Create per-node communicators by hostname
2. Create node-leader communicator (rank 0 on each node)
3. EB broadcasts to node leaders
4. Each node leader creates shared memory and populates it
5. Other ranks on the node map to the shared memory
"""

import numpy as np
from mpi4py import MPI

comm = MPI.COMM_WORLD
rank = comm.Get_rank()
size = comm.Get_size()

# Example configuration
batch_size = 10  # number of offsets in this marching batch

def setup_node_comms(comm_world):
    """
    Split ranks into per-node and node-leader communicators.
    Returns: (node_comm, is_leader, leader_comm)
    """
    hostname = MPI.Get_processor_name()
    hostnames = comm_world.allgather(hostname)

    # Assign a color to each unique hostname
    color_map = {}
    colors = []
    for idx, host in enumerate(hostnames):
        if host not in color_map:
            color_map[host] = len(color_map)
        colors.append(color_map[host])

    node_color = colors[comm_world.Get_rank()]
    node_comm = comm_world.Split(node_color, comm_world.Get_rank())
    node_rank = node_comm.Get_rank()
    is_leader = node_rank == 0

    # Create leader communicator (only rank 0 on each node)
    leader_color = 0 if is_leader else MPI.UNDEFINED
    leader_comm = comm_world.Split(leader_color, comm_world.Get_rank())

    return node_comm, is_leader, leader_comm

def distribute_offsets_across_nodes(comm_world, node_comm, is_leader, leader_comm, offsets_src, n_entries):
    """
    Distribute offsets from rank 0 to all ranks using shared memory within nodes.

    Pattern from lcls2:
    1. Rank 0 has the data
    2. Broadcast to node leaders via leader_comm
    3. Each node leader populates shared memory
    4. Other ranks on the node map to shared memory
    """
    entry_dtype = np.int64
    bytes_per_entry = entry_dtype().nbytes
    total_bytes = bytes_per_entry * n_entries

    leader_buffer = None

    # Step 1: Broadcast from rank 0 to all node leaders
    if leader_comm != MPI.COMM_NULL:
        if is_leader and comm_world.Get_rank() == 0:
            # Rank 0 is a leader with source data
            leader_buffer = offsets_src.copy()
        elif is_leader:
            # Other node leaders receive
            leader_buffer = np.empty(n_entries, dtype=entry_dtype)

        if is_leader:
            leader_comm.Bcast(leader_buffer, root=0)

    # Step 2: Each node leader creates shared memory and populates it
    win = MPI.Win.Allocate_shared(total_bytes if is_leader else 0,
                                   bytes_per_entry,
                                   comm=node_comm)

    # Step 3: All ranks query the shared memory base pointer from node leader (rank 0)
    buf_ptr, itemsize = win.Shared_query(0)
    shared_array = np.ndarray(buffer=buf_ptr, dtype=entry_dtype, shape=(n_entries,))

    # Step 4: Node leader populates the shared memory
    if is_leader:
        if leader_buffer is None:
            raise RuntimeError("Leader buffer missing during shared memory population")
        shared_array[:] = leader_buffer

    # Step 5: Synchronize within node so all ranks see the data
    node_comm.Barrier()

    return win, shared_array

def eb_fill_offsets(offset_buf):
    """Only EB rank (global rank 0) fills offsets."""
    for i in range(len(offset_buf)):
        offset_buf[i] = 1000 + i * 50  # replace with real offsets

def bd_march(offset_buf, comm_world, node_comm):
    """
    BD ranks march through offsets using atomic operations.
    Use MPI.Win.Allocate for the counter on all ranks.
    """
    n_entries = offset_buf.shape[0]

    # Use Win.Allocate for the counter window - all ranks allocate
    counter_win = MPI.Win.Allocate(8, 8, comm=comm_world)  # 8 bytes for int64
    counter_buf = np.ndarray(1, dtype=np.int64, buffer=counter_win.tomemory())

    # Initialize counter on rank 0
    if comm_world.Get_rank() == 0:
        counter_buf[0] = 0

    comm_world.Barrier()  # Ensure counter is initialized

    claimed = np.zeros(1, dtype=np.int64)
    one = np.array(1, dtype=np.int64)

    while True:
        # Atomic fetch-and-add on rank 0's counter
        counter_win.Lock(rank=0)
        counter_win.Fetch_and_op(one, claimed, target_rank=0, op=MPI.SUM)
        counter_win.Unlock(0)

        idx = int(claimed[0])
        if idx >= n_entries:
            break

        # Synchronize access to shared buffer within the node
        node_comm.Barrier()
        offset = offset_buf[idx]

        print(f"[Rank {comm_world.Get_rank()}] marching index {idx} offset {offset}")
        # ... do pread using offset here ...

    counter_win.Free()

# Main execution
print(f"[Rank {rank}] Starting on host {MPI.Get_processor_name()}")

# Step 1: Setup node communicators
node_comm, is_leader, leader_comm = setup_node_comms(comm)
print(f"[Rank {rank}] Node setup: is_leader={is_leader}, node_rank={node_comm.Get_rank()}")

# Step 2: EB rank (rank 0) fills the offsets
if rank == 0:
    offsets_src = np.empty(batch_size, dtype=np.int64)
    eb_fill_offsets(offsets_src)
    print(f"[Rank {rank}] Filled offsets: {offsets_src}")
else:
    offsets_src = None

# Step 3: Distribute offsets to all ranks using the pattern from lcls2
win, offsets = distribute_offsets_across_nodes(comm, node_comm, is_leader, leader_comm,
                                                 offsets_src, batch_size)

# Verify all ranks have the data
print(f"[Rank {rank}] Received offsets: {offsets}")

# Global barrier to ensure all ranks have the offsets
comm.Barrier()

# BD ranks march through the offsets
if rank != 0:
    bd_march(offsets, comm, node_comm)

# Cleanup
win.Free()
comm.Barrier()

if rank == 0:
    print("All BD ranks finished marching")
