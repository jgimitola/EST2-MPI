import sys
from math import sqrt, floor
from time import time

from mpi4py import MPI

comm = MPI.COMM_WORLD
rank = comm.Get_rank()
workers = comm.Get_size()


# Devuelve el número de primos en un rango.
def primes_in_range(start, stop):
    primes = 0
    for n in range(start, stop + 1):
        divisores = 2
        for i in range(2, floor(sqrt(n)) + 1):
            if n % i == 0:
                divisores += 1
        if divisores == 2 and n != 1:
            primes += 1
    return primes


# Devuelve una lista de los bloques.
def blocks_for_(k):
    blocks = []
    blocks_count = k // 100
    for i in range(1, blocks_count + 1):
        blocks.append(((i - 1) * 100 + 1, i * 100))

    if k % 100 != 0:
        blocks.append((blocks_count * 100 + 1, k))
    return blocks


k = int(sys.argv[1])

primes = 0
if rank == 0:
    print(f"& Vamos a verificar hasta {k} con n = {workers}")

    start_time = time()
    blocks = blocks_for_(k)  # Generamos los bloques para k

    fsend = 0  # Debemos asegurarnos que entremos al while fsend veces para esperar la respuesta.
    for i in range(1, workers):
        if len(blocks) > 0:
            block = blocks.pop(0)
            comm.send(block, dest=i)
            fsend += 1
        else:
            break

    c = 0
    env = 0
    while c < fsend or env != 0:  # Entramos al menos fsend veces o mientras esperemos respuesta.
        c += 1
        info = comm.recv(source=MPI.ANY_SOURCE)
        primes += info['primes']

        if c > fsend:
            env -= 1

        if len(blocks) > 0:
            env += 1  # Cada vez que enviemos debemos esperar por la respuesta.
            block = blocks.pop(0)
            comm.send(block, dest=info['source'])

    for i in range(1, workers):
        comm.send("STOP", dest=i)

    comm.barrier()
    print(f'& La cantidad de numeros primos es: {primes} \n& El procesamiento tardo: {time() - start_time} segundos')
else:
    atendido = 0
    while True:
        primes = 0
        block = comm.recv(source=0)

        if block == "STOP":
            break
        else:
            atendido += 1

        primes += primes_in_range(block[0], block[1])
        info = {
            "primes": primes,
            "source": rank
        }
        comm.send(info, dest=0)

    comm.barrier()
    print(f"## P{rank} proceso {atendido} paquetes")
