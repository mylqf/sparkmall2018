import java.util.Random

object RandomNum{

  def apply(fromNum:Int,toNum: Int): Int = {

    fromNum+ new Random().nextInt(toNum-fromNum+1)

  }

  def multi(fromNum:Int,toNum: Int,amount:Int,delimeter:String,canRepeat:Boolean)={

    "1,2,3"
  }

}

class RandomNum {

}
