import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

class Print{

    Lock lock=new ReentrantLock();
    Condition condition=lock.newCondition();
    Condition condition1=lock.newCondition();
    Condition condition2=lock.newCondition();



    public void print5() throws InterruptedException {
        lock.lock();
        try{
            for (int i = 0; i < 5; i++) {
                System.out.println(i);
                condition.await();
            }
            condition1.signal();
        }finally{
            lock.unlock();
        }

    }
    public void print10(){

        lock.lock();
        try{
            for (int i = 0; i < 10; i++) {
                System.out.println(i);
            }
        }finally{
            lock.unlock();
        }
    }
    public void print15(){
        lock.lock();
        try{
            for (int i = 0; i < 15; i++) {
                System.out.println(i);
            }
        }finally{
            lock.unlock();
        }
    }


}



public class TestThread2 {


    public static void main(String[] args) {


        final Print print=new Print();
        new Thread(new Runnable() {
            public void run() {
                try {
                    print.print5();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }).start();
        new Thread(new Runnable() {
            public void run() {
                print.print10();
            }
        }).start();
        new Thread(new Runnable() {
            public void run() {
                print.print15();
            }
        }).start();



    }

}
