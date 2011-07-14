import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import com.mongodb.Mongo;

public class MongoStress {
   
   public static Executor executor;
   public static AtomicBoolean runz = new AtomicBoolean(true);
   public static Mongo mongo;
   public static String dbName;
   public static String collectionName;
   
   public static AtomicInteger counter = new AtomicInteger();
   public static AtomicInteger total = new AtomicInteger();
   
   public static String[] entities = new String[1000];
   public static LinkedBlockingDeque<String> queue = new LinkedBlockingDeque<String>(10000);
   
   public static void main(String[] args) throws Exception {
      dbName = args[0];
      collectionName = args[1];
      mongo = new Mongo();
      int threads = 10;
      
      try {
         threads = Integer.parseInt(args[2]);
      } catch (Exception e) {
         System.out.println("defaulting to " + threads + " threads");
      }
      
      executor = Executors.newFixedThreadPool(threads);
      System.out.println("running " + threads + " threads");

      Map<String, String> ipToUUID = new HashMap<String, String>();
      for (int i = 0; i < entities.length; i++)
         entities[i] = UUID.randomUUID().toString();
      
      // start workers
      for (int i = 0; i < threads; i++) {
         executor.execute(new Runner());
      }

      BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
      String strLine;
      long lastTimestamp = System.currentTimeMillis();
      while ((strLine = br.readLine()) != null) {
         if (System.currentTimeMillis() - lastTimestamp > 1000) {
            System.out.println(counter.getAndSet(0) + " lines/s\t\tline: " + total.get());
            lastTimestamp = System.currentTimeMillis();
         }
         String ip = strLine.replace(".", "_");
         if (!ipToUUID.containsKey(ip))
            ipToUUID.put(ip, UUID.randomUUID().toString());

         queue.put(ipToUUID.get(ip));
      }
      
      runz.set(false);
   }

   private static class Runner implements Runnable {
      
      Random rnd = new Random();
      
      @Override
      public void run() {
         while (runz.get()) {
            try {
               String user = queue.take();
               
               DB db = mongo.getDB(dbName);
               DBCollection collection = db.getCollection(collectionName);
               
               DBObject query = new BasicDBObject("user", user);
               DBObject object = collection.findOne(query);
               // simulate doing stuff

               String entity = entities[rnd.nextInt(entities.length)];
               BasicDBObject op = new BasicDBObject();
               op.put("$push", new BasicDBObject("entities." + entity, System.currentTimeMillis()));
               collection.update(query, op, true, false);
               
               total.incrementAndGet();
               counter.incrementAndGet();
               
            } catch (InterruptedException e) {
               e.printStackTrace();
            }
         }
      }

   }
}
