package JUC;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

public class TestCollection {



    public static void main(String[] args) {


       Set set= Collections.synchronizedSet(new HashSet<>());
       for(int i=0;i<10;i++){
           new Thread(()->{ set.add(UUID.randomUUID().toString().substring(0,8));
               System.out.println(set);
           }, String.valueOf(i)).start();
       }

       List list=Collections.synchronizedList(new ArrayList());
       for (int i=0;i<10;i++){

           new Thread(()->{list.add(UUID.randomUUID().toString().substring(0,8));
               System.out.println(list);}, String.valueOf(i)).start();

       }
       Map map=new ConcurrentHashMap();
       for(int i=0;i<10;i++){
           new Thread(()->{map.put(Thread.currentThread().getName(),UUID.randomUUID().toString().substring(0,8) );}, String.valueOf(i)).start();
       }

       Map map1=Collections.synchronizedMap(new HashMap<>());


    }
}
