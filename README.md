# CME-323-project

Implements Minimum Spanning Trees on Apache Spark. Here we assume that the set of nodes is small enough to fit in the memory of a single machine. This is not a bad assumption considering that about 1 million nodes can fit in the memory of a standard laptop. However, the edges are stored as an RDD.

The algorithm is based on a distributed version of Kruskal's algorithm using the Disjoint Set data structure.
