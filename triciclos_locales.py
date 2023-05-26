
# Escribe un programa paralelo que calcule los 3-ciclos grafos de manera local denidos 
# como lista de aristas en varios ficheros.

import sys
from pyspark import SparkContext
sc = SparkContext()

# Arista recibe una linea y devuelve la arista en orden alfabético.
# En caso de que el vértice de entrada y salida sea el mismo no lo guarda.
def arista_linea(linea):
    vertice = linea.strip().split(',')
    v1 = vertice[0]
    v2 = vertice[1]
    V1 = max(v1,v2)
    V2 = min(v1,v2)
    if V1 != V2:
        return(V1,V2)

# Esta función recibe una tupla (nodo,lista de adyacencia) y se devuelve la 
# lista según indica la pista dos de las instrucciones.
def conexiones(tupla):
    sol = []
    for i in range(len(tupla[1])):
        sol.append(((tupla[0],tupla[1][i]),'exists'))
        for j in range(i+1,len(tupla[1])):
            nodo1 = tupla[1][i]
            nodo2 = tupla [1][j]
            minimo = min(nodo1,nodo2)
            maximo = min(nodo1,nodo2)
            sol.append(((minimo,maximo),('pending',tupla[0])))
    return sol

#Creación de los grupos de tres elementos    
def agrupar(tupla):
    sol = []
    for i in tupla[1]:
        if i != 'exists':
            sol.append((i[1],tupla[0][0], tupla[0][1]))
    return sol

def condicion(tupla):
    return (len(tupla[1])>= 2 and 'exists' in tupla[1])

def main(sc, ficheros):
    rdd = sc.parallelize([])
    for fichero in ficheros:
        file_rdd = sc.textFile(fichero).map(lambda x : arista_linea(x,fichero)).\
            filter(lambda x: x is not None).distinct()
        rdd = rdd.union(file_rdd).distinct()
    linked = rdd.groupByKey().mapValues(list).flatMap(conexiones)
    triciclos = linked.groupByKey().mapValues(list).filter(condicion).flatMap(agrupar)
    print(triciclos.collect())
    return triciclos.collect()

if __name__ == "__main__":
    ficheros = []
    if len(sys.argv) <= 2:
        print(f"Uso: python3 {0} <file>")
    else:
        for i in range(len(sys.argv)):
            if i != 0:
                ficheros.append(sys.argv[i])
        main(sc,ficheros)
