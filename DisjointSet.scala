package util

import scala.collection.mutable.HashMap
import scala.collection.mutable.ArrayBuffer

class DisjointSet[Element] extends Serializable{
  
  val parent = new HashMap[Element, Element]
  val rank = new HashMap[Element, Int]
  
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

