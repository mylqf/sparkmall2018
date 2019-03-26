
class Phone{

    public  synchronized void call(){

    }

    public synchronized void sendSMS(){

    }

}

public class TestThread {


    public static void main(String[] args) {

        final Phone phone1=new Phone();
       new Thread(new Runnable() {
           public void run() {
               phone1.call();
           }
       }, "a").start();

        new Thread(new Runnable() {
            public void run() {
                phone1.sendSMS();
            }
        }, "b").start();



    }


}
