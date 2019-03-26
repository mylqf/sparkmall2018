package JUC;

import java.util.ArrayList;
import java.util.HashSet;

public class SaleTicketDemo01  {

   static int ticketNum=30;

    public static void main(String[] args) {

//        new Saler().start();
//        new Saler().start();
//        new Saler().start();

        new Thread(()->{
            while (ticketNum>1) {
                ticketNum -= 1;
                System.out.println(Thread.currentThread().getName()+" "+"还剩" + ticketNum + "张票");

        }}, "a").start();

        new Thread(()->{
            while (ticketNum>1) {
                ticketNum -= 1;
                System.out.println(Thread.currentThread().getName()+" "+"还剩" + ticketNum + "张票");

            }}, "b").start();
    }



}




class Saler extends Thread{
    //ArrayList
    ///HashSet

//    int ticketNum=30;
//
//    @Override
//    public   void run() {
//        while (ticketNum>=1) {
//            ticketNum -= 1;
//            System.out.println(Thread.currentThread().getName()+" "+"还剩" + ticketNum + "张票");
//
//        }
//    }


}
