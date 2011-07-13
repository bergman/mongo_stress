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
      String strLine;
      while ((strLine = br.readLine()) != null) {
         if (line++ % 1000 == 0)
            System.out.println(line);

         String ip = strLine.replace(".", "_");
         if (!ipToUUID.containsKey(ip))
            ipToUUID.put(ip, UUID.randomUUID().toString());

         DBObject query = new BasicDBObject("user", ipToUUID.get(ip));
         DBObject object = collection.findOne(query);
         if (object == null) {
            object = new BasicDBObject("user", ipToUUID.get(ip));
            object.put("entities", new HashMap<String, List<Date>>());
         }

         String entity = entities[rnd.nextInt(entities.length)];
         Map<String, List<Date>> map = (Map<String, List<Date>>) object.get("entities");
         if (!map.containsKey(entity))
            map.put(entity, new ArrayList<Date>());
         map.get(entity).add(new Date());
         collection.update(query, object, true, false);
      }
   }
}
