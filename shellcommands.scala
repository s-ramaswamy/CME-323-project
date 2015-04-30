:load ./KSM.scala

val distK = sc.textFile("roadnet.txt")
val distE = distK.map(s => ((s.split("\\s+")(0).toInt,s.split("\\s+")(1).toInt),(1).toInt))
//val E = Array(((1,4),5), ((3,5),5), ((4,6),6), ((1,2),7), ((2,5),7), ((2,3),8), ((5,6),8), ((2,4),9), ((5,7),9), ((6,7),11), ((4,5),15))
//val distE = sc.parallelize(E)

def mapfunction(x: Iterable[((Int,Int),Int)]): Array[((Int,Int), Int)] = {
	var y = new Array[((Int,Int), Int)](x.size)
	x.copyToArray(y)
	return Kruskal(y)
}

var distF = distE

for(a <- 1 to 5)
	distF = distF.map((scala.util.Random.nextInt(4),_)).groupByKey().flatMap(x => mapfunction(x._2))
