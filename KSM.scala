import scala.collection.mutable.HashMap
import scala.collection.mutable.ArrayBuffer

class DisjointSet[Element] {
  
  private val parent = new HashMap[Element, Element]().withDefaultValue((-1).asInstanceOf[Element])
  private val rank = new HashMap[Element, Int]
  
  /* number of Elements in the Data structure */
  def size = parent.size
  
  /* Add an Element to the collection */
  def +=(x: Element) = add(x)
  def ++(x: Element) = add(x)
  
  def add(x: Element) {
    parent += (x -> x) 
    rank += (x -> 0)
  }
  
  /* Union of two Sets which contain x and y */
  def union(x: Element, y: Element) {
    val s = find(x)
    val t = find(y)
    if(s == t) return
    if(rank(s) > rank(t)) parent += (t -> s)
    else{
      if (rank(s) == rank(t)) rank(t) += 1 
      parent += (s -> t)
    }
  }
  
  /* Find the set/root of the tree containing given Element x */
  def find(x: Element): Element = {
    if(parent(x) == x) x
    else{
      parent += (x -> find(parent(x)))
      parent(x)
    }
  }
  
  /* check the connectivity between two Elements */
  def isConnected(x: Element, y:Element): Boolean = find(x) == find(y) 
  
  /* toString method */
  override def toString: String = parent.toString
}

def Kruskal(E : Array[((Int,Int), Int)]): Array[((Int,Int), Int)] = {

//val V = Array(1, 2, 3, 4, 5, 6, 7)
//val E = Array(((1,4),5), ((3,5),5), ((4,6),6), ((1,2),7), ((2,5),7), ((2,3),8), ((5,6),8), ((2,4),9), ((5,7),9), ((6,7),11), ((4,5),15))

//val V = Array(1,2,3,4,5,6)
//val E = Array(((1,2),3), ((1,3),5), ((2,3),2), ((4,5),3), ((4,6),5), ((5,6),2))

val Esorted = E.sortBy(_._2)

var tree = new Array[((Int, Int), Int)](0)

var x = new DisjointSet[Int]

//for(v <- V)
//	x.add(v)


for(e <- Esorted)
{
	val u = e._1._1
	val v = e._1._2

	if(x.find(u) == -1)
		x.add(u)
	if(x.find(v) == -1)
		x.add(v)

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

	//println("{(" + u + ", " + v + "), " + w + "}")
}

return tree
}
