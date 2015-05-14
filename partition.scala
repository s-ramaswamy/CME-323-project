def partition(edge: ((Int,Int),Int), v: Array[(Int,Int)]): Array [((Int,Int),((Int,Int),Int))] = {
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
		var T = new Array[((Int,Int),((Int,Int),Int))](3)
		for(a <- 1 to 3){
			small = min(v(edge._1._1 - 1)._2,a-1)
			big = max(v(edge._1._1 - 1)._2,a-1)
			T(a-1) = ((small,big),edge)
		}
		return T
	}
}

