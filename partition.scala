import math._
:load ./KSM.scala

//val e1 = Array(((1,4),5), ((3,5),5), ((4,6),6), ((1,2),7), ((2,5),7), ((2,3),8), ((5,6),8), ((2,4),9), ((5,7),9), ((6,7),11), ((4,5),15))

//val e = sc.parallelize(e1)
//val V = Array(1,2,3,4,5,6,7,8)

//val e = Array(((1,4),5), ((3,5),5), ((4,6),6), ((1,2),7), ((2,5),7), ((2,3),8), ((5,6),8), ((2,4),9), ((5,7),9), ((6,7),11), ((4,5),15))

//val V = Array(1,2,3,4,5,6,7)


val distK = sc.textFile("roadnet.txt")
val e = distK.map(s => ((s.split("\\s+")(0).toInt,s.split("\\s+")(1).toInt),(1).toInt))
val V = 0 to 2088092  toArray

val Vpart = V.map((_,scala.util.Random.nextInt(3)))
val V1 = sc.parallelize(Vpart)

def partition(edge: ((Int,Int),Int), v: org.apache.spark.broadcast.Broadcast[Array[(Int,Int)]], n: Int): Array [((Int,Int),((Int,Int),Int))] = {
	var A = new Array[((Int,Int),((Int,Int),Int))](1)
	if(v.value(edge._1._1)._2 != v.value(edge._1._2)._2){
		if(v.value(edge._1._1)._2 < v.value(edge._1._2)._2){
			A(0) = ((v.value(edge._1._1)._2,v.value(edge._1._2)._2),edge)
		}
		else{
			A(0) = ((v.value(edge._1._2)._2,v.value(edge._1._1)._2),edge)
		}
		return A
	}
	else{
		var vlist = 0 to n-1 toArray;
		for{a <- vlist
		if a != v.value(edge._1._1)._2
		} yield {
		for(a <- vlist) yield {
			((min(v.value(edge._1._1)._2,a),max(v.value(edge._1._1)._2,a)),edge);
		}
	}
}
val Vbroad = sc.broadcast(Vpart)
//val temp = e.flatMap(x => partition(x,Vbroad,3))
//val t = sc.parallelize(temp)
val t = e.flatMap(x => partition(x, Vbroad, 3))
def mapfunction(x: Iterable[((Int,Int),Int)]): Array[((Int,Int), Int)] = {
	var y = new Array[((Int,Int), Int)](x.size)
	x.copyToArray(y)
	return y
}
val temp = t.groupByKey
val temp0 = temp.map(x => mapfunction(x._2))
val temp1 = temp0.flatMap(x => Kruskal(x))
val edge_partition = temp1.distinct
	return Kruskal(y)
}
val edge_partition = t.groupByKey.map(x => mapfunction(x._2)).flatMap(x => Kruskal(x)).distinct


