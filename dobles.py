from mpi4py import MPI

comm = MPI.COMM_WORLD
rank = comm.Get_rank()

if rank == 0:
    data = []
    size = comm.Get_size()

    comm.isend(rank, dest=0)
    r = comm.recv(source=0)
    data.append(2 * r)

    for i in range(1, size):
        comm.send(i, dest=i)

    for i in range(1, size):
        rank_leaf = comm.recv(source=i)
        data.append(rank_leaf)

    print(data)
else:
    rank_node = comm.recv(source=0)
    comm.send(2 * rank_node, dest=0, tag=rank_node)
