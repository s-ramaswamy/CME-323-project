import math._

val e = Array(((1,4),5), ((3,5),5), ((4,6),6), ((1,2),7), ((2,5),7), ((2,3),8), ((5,6),8), ((2,4),9), ((5,7),9), ((6,7),11), ((4,5),15))

val V = Array(1,2,3,4,5,6,7)

val Vpart = V.map((_,scala.util.Random.nextInt(3)))
val V1 = sc.parallelize(Vpart)

def partition(edge: ((Int,Int),Int), v: Array[(Int,Int)], n: Int): Array [((Int,Int),((Int,Int),Int))] = {
	var A = new Array[((Int,Int),((Int,Int),Int))](1)
	if(v(edge._1._1 - 1)._2 != v(edge._1._2 - 1)._2){
		if(v(edge._1._1 - 1)._2 < v(edge._1._2 - 1)._2){
			A(0) = ((v(edge._1._1 - 1)._2,v(edge._1._2 - 1)._2),edge)
		}
		else{
			A(0) = ((v(edge._1._2 - 1)._2,v(edge._1._1 - 1)._2),edge)
		}
		return A
	}
	else{
		var a = 0
		var small = 0
		var big = 0
		//var T = new Array[((Int,Int),((Int,Int),Int))](n)
		var vlist = 1 to n toArray
		for(a <- vlist){
			small = min(v(edge._1._1 - 1)._2,a-1)
			big = max(v(edge._1._1 - 1)._2,a-1)
			yield((small,big),edge)
			//T(a-1) = ((small,big),edge)
		}
		return T
	}
}

val temp = e.flatMap(x => partition(x,V1))
val t = sc.parallelize(temp)
val edge_partition = t.groupByKey.collect()

