:load ./KSM.scala
val E = Array(((1,4),5), ((3,5),5), ((4,6),6), ((1,2),7), ((2,5),7), ((2,3),8), ((5,6),8), ((2,4),9), ((5,7),9), ((6,7),11), ((4,5),15))//val distE = sc.parallelize(E)
val V = Array(1,2,3,4,5,6,7)

val distK = sc.textFile("facebook_combined.txt")
val distE = distK.map(s => ((s.split("\\s+")(0).toInt,s.split("\\s+")(1).toInt),(1).toInt))
val distV = sc.parallelize(0 to 4039)
//val distE = sc.parallelize(E)
//val distV = sc.parallelize(V)
def mapIterableToArray(x : Iterable[Int]): Array[Int] = {
    var y = new Array[Int](x.size)
    x.copyToArray(y)
    return y
}

val mappedV = distV.map((scala.util.Random.nextInt(10),_)).groupByKey()
val mappedVArray = mappedV.map(x => (x._1, mapIterableToArray(x._2)))
def CartesianProduct(xs: Array[Int], i : Int) : Array[((Int,Int),Int)] = {
    println("Cartesian Product")
    //var A = new Array[((Int,Int),Int)](0)
    for(x <- xs; y <- xs)
         yield((x,y),1)
}
def mapfunction(x: Iterable[((Int,Int),Int)]): Array[((Int,Int), Int)] = {
	var y = new Array[((Int,Int), Int)](x.size)
	x.copyToArray(y)
	return Kruskal(y)
}


val first = mappedVArray.cartesian(mappedVArray).filter{case (a,b) => a._1 < b._1}.map(x => x._1._2 ++ x._2._2).flatMap(x => CartesianProduct(x,scala.util.Random.nextInt(45))).join(distE).map(x => (x._2._1, ((x._1._1, x._1._2), x._2._2))).groupByKey().flatMap(x=> mapfunction(x._2))

