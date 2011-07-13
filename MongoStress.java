import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.UUID;


import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import com.mongodb.Mongo;

public class MongoStress {
  public static void main(String[] args) throws Exception {
      Mongo mongo = new Mongo();
      DB db = mongo.getDB("stress");
      DBCollection collection = db.getCollection("test");

      Map<String, String> ipToUUID = new HashMap<String, String>();
      BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
      String[] entities = new String[1000];
      for (int i = 0; i < entities.length; i++)
         entities[i] = UUID.randomUUID().toString();

      Random rnd = new Random();
      int line = 0;
      int linePerSecond = 0;
      String strLine;
      long lastTimestamp = System.currentTimeMillis();
      while ((strLine = br.readLine()) != null) {
         if (System.currentTimeMillis() - lastTimestamp > 1000) {
            System.out.println(linePerSecond + " lines/s\t\tline: " + line);
            lastTimestamp = System.currentTimeMillis();
            linePerSecond = 0;
         }
         String ip = strLine.replace(".", "_");
         if (!ipToUUID.containsKey(ip))
            ipToUUID.put(ip, UUID.randomUUID().toString());

         DBObject query = new BasicDBObject("user", ipToUUID.get(ip));
         DBObject object = collection.findOne(query);
         // simulate doing stuff

         String entity = entities[rnd.nextInt(entities.length)];
         BasicDBObject op = new BasicDBObject();
         op.put("$push", new BasicDBObject("entities." + entity, new Date()));
         collection.update(query, op, true, false);
         line++;
         linePerSecond++;
      }
   }
}
