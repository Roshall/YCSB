package cn.neu.hotness.leveldbjni;

import net.jcip.annotations.GuardedBy;
import site.ycsb.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.stream.IntStream;

import static java.nio.charset.StandardCharsets.UTF_8;

public class LevelDBClient extends DB {

  private static final String PROPERTY_LEVELDB_DIR = "leveldb.dir";

  // 0 for original, 1 for modified hotDB
  private static final String PROPERTY_LEVELDB_TYPE = "leveldb.type";

  // default is original
  private static final String PROPERTY_LEVELDB_TYPE_DEFAULT = "0";

  private static final Logger LOGGER = LoggerFactory.getLogger(LevelDBClient.class);

  @GuardedBy("LevelDBClient.class") private static LevelDB levelDb = null;

  @Override
  public void init() throws DBException, ArithmeticException {
    if (levelDb == null) {
      Properties pros = getProperties();
      Path levelDbDir = Paths.get(pros.getProperty(PROPERTY_LEVELDB_DIR));
      LOGGER.info("LevelDB data dir: " + levelDbDir);
      String levelDBType = pros.getProperty(PROPERTY_LEVELDB_TYPE, PROPERTY_LEVELDB_TYPE_DEFAULT);
      int type = 0;
      try {
        type = Integer.parseInt(levelDBType);
      } catch (NumberFormatException e) {
        e.printStackTrace();
      }

      if (type != 0 && type != 1) {
        throw new ArithmeticException("DB type not valid. [0 for original, 0 for hotDB]");
      }
      try {
        levelDb = new LevelDB(levelDbDir.toFile(), type);
      } catch (final RuntimeException e) {
        throw new DBException(e);
      }
    }

  }

  @Override
  public Status read(final String table, final String key, final Set<String> fields, final Map<String, ByteIterator> result) {
    final byte[] values;
    values = levelDb.get( key.getBytes(UTF_8));
    if(values == null) {
      return Status.NOT_FOUND;
    }
    deserializeValues(values, fields, result);
    return Status.OK;
  }

  @Override
  public Status scan(String table, String startkey, int recordcount, Set<String> fields, Vector<HashMap<String, ByteIterator>> result) {
    var record = levelDb.scan(startkey.getBytes(UTF_8), recordcount);
    if (record == null) {
      return Status.NOT_FOUND;
    }
    var offsets = record.getOffsets();
    IntStream.range(0, offsets.length - 1).parallel().forEach(i -> deserializeValuesFromArray(record.getValues(), offsets[i], offsets[i + 1], fields, result));
    return Status.OK;
  }

  @Override
  public Status update(final String table, final String key, final Map<String, ByteIterator> values) {
    final Map<String, ByteIterator> result = new HashMap<>();
    read(table, key, null, result);
    // update
    result.putAll(values);

    try {
      // store
      levelDb.put(key.getBytes(UTF_8), serializeValues(result));
      return Status.OK;
    } catch (final IOException e) {
      LOGGER.error("serializeValues Exception thrown update to DB: ");
      return Status.ERROR;
    }
  }

  @Override
  public Status insert(String table, String key, Map<String, ByteIterator> values) {
    boolean ok;
    try {
      ok = levelDb.put(key.getBytes(UTF_8), serializeValues(values));
      return ok ? Status.OK : Status.ERROR;
    } catch (IOException e) {
      LOGGER.error("serializeValues Exception thrown writing to DB: ");
      return Status.ERROR;
    }
  }

  @Override
  public Status delete(String table, String key) {
    return levelDb.delete(key.getBytes(UTF_8)) ? Status.OK : Status.ERROR;
  }

  @Override
  public void cleanup() throws DBException {
    super.cleanup();
    levelDb.close();
  }


  private void deserializeValuesFromArray(final byte[] values, int offset, int end,
                                          final Set<String> fields,
                                 final Vector<HashMap<String, ByteIterator>> result) {
    final HashMap<String, ByteIterator> resultOne = new HashMap<>();
    deserializeValues(values, offset, end, fields, resultOne);
    result.add(resultOne);
  }

  private void deserializeValues(final byte[] values,  int offset, int end,
                                 final Set<String> fields,
                                 final Map<String, ByteIterator> result) {
    final ByteBuffer buf = ByteBuffer.allocate(4);

    while(offset < end) {
      buf.put(values, offset, 4);
      buf.flip();
      final int keyLen = buf.getInt();
      buf.clear();
      offset += 4;

      final String key = new String(values, offset, keyLen);
      offset += keyLen;

      buf.put(values, offset, 4);
      buf.flip();
      final int valueLen = buf.getInt();
      buf.clear();
      offset += 4;

      if(fields == null || fields.contains(key)) {
        result.put(key, new ByteArrayByteIterator(values, offset, valueLen));
      }

      offset += valueLen;
    }

  }
  private void deserializeValues(final byte[] values, final Set<String> fields,
                                 final Map<String, ByteIterator> result) {
    deserializeValues(values, 0, values.length, fields, result);

  }

  private byte[] serializeValues(final Map<String, ByteIterator> values) throws IOException {
    try(final ByteArrayOutputStream baos = new ByteArrayOutputStream()) {
      final ByteBuffer buf = ByteBuffer.allocate(4);

      for(final Map.Entry<String, ByteIterator> value : values.entrySet()) {
        final byte[] keyBytes = value.getKey().getBytes(UTF_8);
        final byte[] valueBytes = value.getValue().toArray();

        buf.putInt(keyBytes.length);
        baos.write(buf.array());
        baos.write(keyBytes);

        buf.clear();

        buf.putInt(valueBytes.length);
        baos.write(buf.array());
        baos.write(valueBytes);

        buf.clear();
      }
      return baos.toByteArray();
    }
  }
}
