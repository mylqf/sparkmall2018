package JUC;

import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

class Saler1 extends Thread{

    int num=30;
    Lock lock=new ReentrantLock();


    public void sale(){
        lock.lock();

        try {

            while (num>1){
                num--;
                System.out.println(Thread.currentThread().getName()+" "+num);
            }
        }finally {

            lock.unlock();
        }

    }

}

public class LockTest {


    public static void main(String[] args) {

        Saler1 saler1 = new Saler1();

        new Thread(()->{saler1.sale();},"a").start();
        new Thread(()->{saler1.sale();},"b").start();

    }

}
