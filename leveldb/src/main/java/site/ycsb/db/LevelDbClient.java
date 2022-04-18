/**
 * LevelDB client binding for YCSB.
 */

package site.ycsb.db;

import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.http.util.EntityUtils;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import site.ycsb.*;

import java.io.*;
import java.net.URLEncoder;
import java.text.MessageFormat;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

import static java.nio.charset.StandardCharsets.UTF_8;

/**
 * LevelDB client for YCSB framework.
 */
public class LevelDbClient extends DB {

  private String dbUrl = "http://localhost:";
  private String deleteUrl;
  private String insertUrl;
  private String readUrl;
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
    Properties pros = getProperties();
    String port = pros.getProperty("db.port", "8080");
    dbUrl = dbUrl + port;
    deleteUrl = dbUrl + "/del";
    insertUrl = dbUrl + "/put";
    readUrl = dbUrl + "/get";
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
      httpGet = new HttpGet(MessageFormat.format("{0}?key={1}",
          deleteUrl, key));
      response = httpClient.execute(httpGet);
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
      JSONObject jsonValues = new JSONObject();
      jsonValues.putAll(StringByteIterator.getStringMap(
          values));
    try {
      String urlStringValues = URLEncoder.encode(
          jsonValues.toJSONString(), "UTF-8");
      httpGet= new HttpGet(MessageFormat.format(
          "{0}?key={1}&value={2}", insertUrl, key, urlStringValues));

      response = httpClient.execute(httpGet);
      EntityUtils.consume(response.getEntity());
    } catch (Exception e) {
      e.printStackTrace();
      return Status.ERROR;
    }
    return response.getStatusLine().getStatusCode() == 200 ? Status.OK : Status.ERROR;
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
    httpGet = new HttpGet(MessageFormat.format("{0}?key={1}", readUrl,
        key));
    try {
      response = httpClient.execute(httpGet);
    } catch (Exception e) {
      System.out.println(e.getMessage());
      return Status.ERROR;
    }
      HttpEntity entity= response.getEntity();
      Status status;
      switch (response.getStatusLine().getStatusCode()) {
        case 200:
          JSONObject document;
          try {
            document = (JSONObject) parser.parse(EntityUtils
                .toString(response.getEntity()));
          } catch (Exception e) {
            System.err.println(e.getMessage());
            return Status.ERROR;
          }
          if (document != null) {
            fillMap(result, fields, document);
            status = Status.OK;
          } else {
            status = Status.UNEXPECTED_STATE;
          }
          break;
        case 404:
          status = Status.NOT_FOUND;
          break;
        default:
          status = Status.ERROR;
      }
      try {
      EntityUtils.consume(entity);
    } catch (Exception e) {
        System.err.println(e.getMessage());
        status = Status.ERROR;
      }
    return  status;
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
      final Map<String, ByteIterator> result = new HashMap<>();
      Status status = read(table, key, null, result);
      if (status != Status.OK) return status;
      result.putAll(values);
      return insert(table, key, result);
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

      final String scanUrl = dbUrl + "/scan";
      httpGet = new HttpGet(MessageFormat.format("{0}?key={1}&limit={2}",
          scanUrl, startkey, recordcount));
    JSONArray scanEntries;
    try {
      response = httpClient.execute(httpGet);

      scanEntries = (JSONArray) parser.parse(EntityUtils
          .toString(response.getEntity()));
    } catch (Exception e) {
      System.out.println(e.getMessage());
      return Status.ERROR;
    }
      for (Object e : scanEntries) {
        HashMap<String, ByteIterator> values = new HashMap<>();
        fillMap(values, fields, (Map)e);
        result.add(values);
      }
    try {
      EntityUtils.consume(response.getEntity());
    } catch (Exception e) {
      System.err.println(e.getMessage());
      return Status.ERROR;
    }
    return response.getStatusLine().getStatusCode() == 200 ? Status.OK : Status.ERROR;
  }

  void fillMap(Map<String, ByteIterator> resultMap, Set<String> fields, Map<String, String> obj) {
    for (Map.Entry<String, String> stringStringEntry : obj.entrySet()) {
      Map.Entry<String, String> entry = (Map.Entry) stringStringEntry;
      if (fields == null || fields.contains(entry.getKey())) {
        resultMap.put(entry.getKey(), new ByteArrayByteIterator(entry.getValue().getBytes(UTF_8)));
      }
    }
  }
}
