/**
 * LevelDB client binding for YCSB.
 */

package site.yahoo.ycsb.db;

import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;
import com.mongodb.util.JSON;
import org.apache.http.HttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.http.util.EntityUtils;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import site.ycsb.*;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URLEncoder;
import java.text.MessageFormat;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.Vector;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * LevelDB client for YCSB framework.
 */
public class LevelDbClient extends DB {

  private static final String dbUrl = "http://localhost:8080";
  private static final String deleteUrl = dbUrl + "/del";
  private static final String insertUrl = dbUrl + "/put";
  private static final String readUrl = dbUrl + "/get";
  private static final JSONParser parser = new JSONParser();

  private static DefaultHttpClient httpClient;
  private static HttpPost httpPost;
  private static HttpResponse response;
  private static HttpGet httpGet;

  // having multiple tables in leveldb is a hack. must divide key
  // space into logical tables
  private static Map<String, Integer> tableKeyPrefix;
  private static final AtomicInteger prefix = new AtomicInteger(0);

  private static String getStringFromInputStream(InputStream is) {
    BufferedReader br = null;
    StringBuilder sb = new StringBuilder();
    String line;
    try {
      br = new BufferedReader(new InputStreamReader(is));
      while ((line = br.readLine()) != null) {
        sb.append(line);
      }
    } catch (IOException e) {
      e.printStackTrace();
    } finally {
      if (br != null) {
        try {
          br.close();
        } catch (IOException e) {
          e.printStackTrace();
        }
      }
    }
    return sb.toString();
  }

  /**
   * Initialize any state for this DB. Called once per DB instance; there is
   * one DB instance per client thread.
   */
  @Override
  public void init() throws DBException {
    httpClient = new DefaultHttpClient();
    tableKeyPrefix = new HashMap<String, Integer>();
  }

  /**
   * Cleanup any state for this DB. Called once per DB instance; there is one
   * DB instance per client thread.
   */
  @Override
  public void cleanup() throws DBException {
  }

  /**
   * Delete a record from the database.
   *
   * @param table The name of the table
   * @param key   The record key of the record to delete.
   * @return Zero on success, a non-zero error code on error. See this class's
   * description for a discussion of error codes.
   */
  @Override
  public Status delete(String table, String key) {
    try {
      httpPost = new HttpPost(MessageFormat.format("{0}?key={1}",
          deleteUrl, key));
      // System.out.println("# delete request - " +
      // httpPost.getRequestLine());
      response = httpClient.execute(httpPost);
      // System.out.println("# delete response - " +
      // getStringFromInputStream(response.getEntity().getContent()));
      EntityUtils.consume(response.getEntity());
      return response.getStatusLine().getStatusCode() == 200 ? Status.OK : Status.ERROR;
    } catch (Exception e) {
      System.err.println(e.toString());
      return Status.ERROR;
    }
  }

  /**
   * Insert a record in the database. Any field/value pairs in the specified
   * values HashMap will be written into the record with the specified record
   * key.
   *
   * @param table  The name of the table
   * @param key    The record key of the record to insert.
   * @param values A HashMap of field/value pairs to insert in the record
   * @return Zero on success, a non-zero error code on error. See this class's
   * description for a discussion of error codes.
   */
  @Override
  public Status insert(String table, String key,
                       Map<String, ByteIterator> values) {
    try {
      JSONObject jsonValues = new JSONObject();
      jsonValues.putAll(StringByteIterator.getStringMap(
          values));
      String urlStringValues = URLEncoder.encode(
          jsonValues.toJSONString(), "UTF-8");
      httpPost = new HttpPost(MessageFormat.format(
          "{0}?key={1}&value={2}", insertUrl, key, urlStringValues));
      // System.out.println("# insert request - " +
      // httpPost.getRequestLine());
      response = httpClient.execute(httpPost);
      // System.out.println("# insert response - " +
      // getStringFromInputStream(response.getEntity().getContent()));
      EntityUtils.consume(response.getEntity());
      return response.getStatusLine().getStatusCode() == 200 ? Status.OK : Status.ERROR;
    } catch (Exception e) {
      e.printStackTrace();
      return Status.ERROR;
    }
  }

  /**
   * Read a record from the database. Each field/value pair from the result
   * will be stored in a HashMap.
   *
   * @param table  The name of the table
   * @param key    The record key of the record to read.
   * @param fields The list of fields to read, or null for all of them
   * @param result A HashMap of field/value pairs for the result
   * @return Zero on success, a non-zero error code on error or "not found".
   */
  @Override
  public Status read(String table, String key, Set<String> fields,
                     Map<String, ByteIterator> result) {
    try {
      httpGet = new HttpGet(MessageFormat.format("{0}?key={1}", readUrl,
          key));
      // System.out.println("# read request - " +
      // httpGet.getRequestLine());
      response = httpClient.execute(httpGet);
      JSONObject jsonResponse = (JSONObject) parser.parse(EntityUtils
          .toString(response.getEntity()));
      // System.out.println("# read response - " + jsonResponse);
      // Use Mongo DBObject to encode back to ByteIterator
      DBObject bson = (DBObject) JSON.parse(jsonResponse.get("data")
          .toString());
      if (bson != null) {
        result.putAll(bson.toMap());
      }
      EntityUtils.consume(response.getEntity());
      return bson != null ? Status.OK : Status.ERROR;
    } catch (Exception e) {
      System.err.println(e.toString());
      return Status.ERROR;
    }
  }

  /**
   * Update a record in the database. Any field/value pairs in the specified
   * values HashMap will be written into the record with the specified record
   * key, overwriting any existing values with the same field name.
   *
   * @param table  The name of the table
   * @param key    The record key of the record to write.
   * @param values A HashMap of field/value pairs to update in the record
   * @return Zero on success, a non-zero error code on error. See this class's
   * description for a discussion of error codes.
   */
  @Override
  public Status update(String table, String key,
                       Map<String, ByteIterator> values) {
    try {
      httpGet = new HttpGet(MessageFormat.format("{0}?key={1}", readUrl,
          key));
      // System.out.println("# update read request - " +
      // httpGet.getRequestLine());
      response = httpClient.execute(httpGet);
      JSONObject jsonResponse = (JSONObject) parser.parse(EntityUtils
          .toString(response.getEntity()));
      // System.out.println("# update read response - " + jsonResponse);
      JSONObject existingValues = new JSONObject();
      // check if key exists in the db
      if (response.getStatusLine().getStatusCode() == 200) {
        existingValues = (JSONObject) parser.parse(jsonResponse.get(
            "data").toString());
      }
      existingValues.putAll(StringByteIterator.getStringMap(
          values));
      String urlStringValues = URLEncoder.encode(
          existingValues.toJSONString(), "UTF-8");
      httpPost = new HttpPost(MessageFormat.format(
          "{0}?key={1}&value={2}", insertUrl, key, urlStringValues));
      // System.out.println("# update insert request - " +
      // httpPost.getRequestLine());
      response = httpClient.execute(httpPost);
      // System.out.println("# update insert response - " +
      // getStringFromInputStream(response.getEntity().getContent()));
      EntityUtils.consume(response.getEntity());
      return response.getStatusLine().getStatusCode() == 200 ? Status.OK : Status.ERROR;
    } catch (Exception e) {
      System.err.println(e.toString());
      return Status.ERROR;
    }
  }

  /**
   * Perform a range scan for a set of records in the database. Each
   * field/value pair from the result will be stored in a HashMap.
   *
   * @param table       The name of the table
   * @param startkey    The record key of the first record to read.
   * @param recordcount The number of records to read
   * @param fields      The list of fields to read, or null for all of them
   * @param result      A Vector of HashMaps, where each HashMap is a set field/value
   *                    pairs for one record
   * @return Zero on success, a non-zero error code on error. See this class's
   * description for a discussion of error codes.
   */
  @Override
  public Status scan(String table, String startkey, int recordcount,
                     Set<String> fields, Vector<HashMap<String, ByteIterator>> result) {
    try {
      String scanUrl = dbUrl + "/fwmatch";
      httpGet = new HttpGet(MessageFormat.format("{0}?key={1}&limit={2}",
          scanUrl, startkey, recordcount));
      // System.out.println("# scan request - " +
      // httpGet.getRequestLine());
      response = httpClient.execute(httpGet);
      JSONObject jsonResponse = (JSONObject) parser.parse(EntityUtils
          .toString(response.getEntity()));
      // System.out.println("# scan response - " + jsonResponse);
      JSONArray scanEntries = (JSONArray) parser.parse(jsonResponse.get(
          "data").toString());
      for (Object e : scanEntries) {
        JSONObject entry = (JSONObject) e;
        DBObject value = (DBObject) JSON.parse(entry.get("value")
            .toString());
        DBObject bsonResult = new BasicDBObject();
        if (fields == null) {
          bsonResult = value;
        } else {
          for (String s : fields) {
            // get has same result for missing keys and values that
            // are null
            bsonResult.put(s, value.get(s));
          }
        }
        HashMap<String, ByteIterator> singleResult = new HashMap<String, ByteIterator>(bsonResult.toMap());
        result.addElement(singleResult);
      }
      EntityUtils.consume(response.getEntity());
      return response.getStatusLine().getStatusCode() == 200 ? Status.OK : Status.ERROR;
    } catch (Exception e) {
      System.err.println(e.toString());
      return Status.ERROR;
    }
  }
}
