import org.apache.spark.{SparkConf, SparkContext}

object WordCount {

  def main(args: Array[String]): Unit = {

    //1.创建SparkConf并设置App名称
    val conf = new SparkConf().setAppName("WC")

    //2.创建SparkContext，该对象是提交Spark App的入口
    val sc = new SparkContext(conf)

    //3.使用sc创建RDD并执行相应的transformation和action
//    sc.textFile(args(0)).flatMap(_.split(" ")).map{
//      case _=>{(_,1)}
//    }
//    sc.textFile(args(0)).flatMap(_.split(" ")).map{
//      case x=>{(_,1)}
//    }
//    sc.textFile(args(0)).flatMap(_.split(" ")).map((_,1)).reduceByKey(_+_,1).sortBy{
//
//      case (x,y)=>y false
    }

  println(1L)

//  }

}
