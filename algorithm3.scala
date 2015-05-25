:load ./DisjointSet.scala
import scala.collection.mutable.Map
import scala.collection.mutable.HashMap
import scala.collection.mutable.ArrayBuffer
import scala.math
import scala.util.control._
def find(x: Int, parent: HashMap[Int,Int]): Int = {
    if(parent(x) == x) x
    else{
        parent += (x -> find(parent(x), parent))
        parent(x)
    }
}
def mapfunction(x: Iterable[((Int,Int),Int)]): Array[((Int,Int), Int)] = {
    var y = new Array[((Int,Int), Int)](x.size)
    x.copyToArray(y)
    return y
}

def findminimum(elist: Array[((Int,Int),Int)], xbroad: scala.collection.immutable.HashMap[Int,Int]): Map[Int,((Int,Int),Int)] = {
    var y:Map[Int,((Int,Int),Int)]  = Map()
    for(e <- elist)
    {
        val u = e._1._1
        val v = e._1._2
        val wt = e._2
        val compU = xbroad(u)
        val compV = xbroad(v)
        if(compU != compV)
        {
            if(!y.contains(compU))
                y += (compU -> e)

            if(!y.contains(compV))
                y += (compV -> e)

            if(y(compU)._2 > wt)
                y(compU) = e
                        
            if(y(compV)._2 > wt)
                y(compV) = e
        }
    }
    return y
}

def minfunction(x: ((Int,Int), Int), y: ((Int,Int), Int)): ((Int, Int), Int) = {
    if(x._2 > y._2 && y._2 != -1)
        return y
    return x
}    

def reduceMaps(p: Map[Int,((Int,Int),Int)], q: Map[Int,((Int,Int),Int)]): Map[Int,((Int,Int),Int)] = {
    var z = p ++ q.map{case (k,v) => k -> minfunction(v,  p.getOrElse(k, ((0,0),-1)))}
    return z
}

//val V = Array(1, 2, 3, 4, 5, 6, 7)
//val E = Array(((1,4),5), ((3,5),5), ((4,6),6), ((1,2),7), ((2,5),7), ((2,3),8), ((5,6),8), ((2,4),9), ((5,7),9), ((6,7),11), ((4,5),15))

//val distE = sc.parallelize(E)
val V = 0 to 1696415  toArray
val distK = sc.textFile("as-skitter.txt")
val distE = distK.map(s => ((s.split("\\s+")(0).toInt,s.split("\\s+")(1).toInt),(1).toInt))
var A = new DisjointSet[Int]
for(i <- V){
    A.add(i)
}
var distEgrouped = distE.map(x => (scala.util.Random.nextInt(4), x)).groupByKey
var MST:Set[((Int,Int),Int)] = Set()
val niter =(math.log(V.size)/math.log(2)).ceil toInt
println(niter)
var edgelistsize = 1
for(m <- 1 to niter if edgelistsize != 0){
        var B:scala.collection.immutable.HashMap[Int,Int] = scala.collection.immutable.HashMap()
        for(i <- V){
            B += (i -> A.find(i))
        }

        var xbroad = sc.broadcast(B)
        var edgelist = distEgrouped.map(a => findminimum(mapfunction(a._2), xbroad.value)).reduce((a,b) => reduceMaps(a,b))
        edgelistsize = edgelist.size
        for((key,value) <- edgelist){
            A.union(value._1._1, value._1._2)
            MST += value
        }
        println(MST.size)
    }
