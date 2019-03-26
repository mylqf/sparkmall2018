package JUC;

class Demo02 extends Thread{

    int num=0;

    public synchronized void increse() throws InterruptedException {

        if (num==0){
            num++;
            System.out.println(Thread.currentThread().getName()+" "+num);
            this.notifyAll();

        }else{
            this.wait();
        }
    }

    public  synchronized void decrese() throws InterruptedException {

        if (num==1){
            num--;
            System.out.println(Thread.currentThread().getName()+" "+num);
            this.notifyAll();
        }else{
            this.wait();
        }
    }



}

public class WaitNotyDemo {

    public static void main(String[] args) {

        Demo02 demo02=new Demo02();
        new Thread(()->{for(int i=0;i<=9;i++){
            try {
                Thread.sleep(200);
                demo02.increse();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }},"a").start();
        new Thread(()->{for(int i=0;i<=9;i++){
            try {
                Thread.sleep(200);
                demo02.decrese();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }},"b").start();
        new Thread(()->{for(int i=0;i<=9;i++){
            try {
                Thread.sleep(200);
                demo02.increse();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }},"c").start();
        new Thread(()->{for(int i=0;i<=9;i++){
            try {
                Thread.sleep(200);
                demo02.decrese();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }},"d").start();

    }

}
