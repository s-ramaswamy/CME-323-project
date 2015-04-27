import util._

def Kruskal(V : Array[Int], E : Array[((Int,Int), Int)]): Array[((Int,Int), Int)] = {

    //val V = Array(1, 2, 3, 4, 5, 6, 7)
    //val E = Array(((1,4),5), ((3,5),5), ((4,6),6), ((1,2),7), ((2,5),7), ((2,3),8), ((5,6),8), ((2,4),9), ((5,7),9), ((6,7),11), ((4,5),15))

    //val V = Array(1,2,3,4,5,6)
    //val E = Array(((1,2),3), ((1,3),5), ((2,3),2), ((4,5),3), ((4,6),5), ((5,6),2))

    val Esorted = E.sortBy(_._2)

    var tree = new Array[((Int, Int), Int)](0)

    var x = new DisjointSet[Int]

    for(v <- V)
        x.add(v)


        for(e <- Esorted)
        {
            val u = e._1._1
            val v = e._1._2

            if(x.find(u) != x.find(v))
            {
                tree = tree :+ e
                x.union(u,v)
            }
        }

        for(e <- tree)
        {
            val u = e._1._1
            val v = e._1._2
            val w = e._2

            println("{(" + u + ", " + v + "), " + w + "}")
        }

        return tree
}
