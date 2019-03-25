import scala.collection.mutable

object testMap {

  def main(args: Array[String]): Unit = {


    var map=new mutable.HashMap[Int,Int]()
    map.+=((1,2))
    map.+=((3,6))
    println(map.getOrElse(2,5))
  }

}
