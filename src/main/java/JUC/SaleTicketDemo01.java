package JUC;

import java.util.ArrayList;

public class SaleTicketDemo01  {



    public static void main(String[] args) {

        new Saler().start();
        new Saler().start();
        new Saler().start();

    }

}




class Saler extends Thread{
    //ArrayList

    int ticketNum=30;

    @Override
    public   void run() {
        while (ticketNum>=1) {
            ticketNum -= 1;
            System.out.println(Thread.currentThread().getName()+" "+"还剩" + ticketNum + "张票");

        }
    }
}
