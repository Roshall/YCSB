package cn.neu.hotness.leveldbjni;

import java.io.*;
import java.util.concurrent.atomic.AtomicBoolean;

record ResultPair(byte[] values, int[] offsets) {

  public byte[] getValues() {
      return values;
      }

  public int[] getOffsets() {
      return offsets;
      }
}

public class LevelDB implements Closeable {
  private static final String LIBNAME = "leveldbjni";

  static {
    try {
      System.loadLibrary(LIBNAME);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  private final long dbRef;
  private final AtomicBoolean dbClosed = new AtomicBoolean();

  /**
   * Opens or creates LevelDB using default options.
   *
   * @param file    Directory for the DB
   * @param type    0 for leveldb, 1 for hotDB
   */
  public LevelDB(File file, int type) {
    dbRef = open(file.getAbsolutePath(), type);
  }


  protected long open(String fileName, int type) {
    long dbRef = Open(fileName, type);
    if (dbRef == 0) {
      throw new RuntimeException();
    }
    return dbRef;
  }


  /**
   * Delete given key
   * @param key the key to delete
   * @return true if successful, false for error
   */
  public boolean delete(byte[] key) {
    return delete(dbRef, key);
  }

  /**
   * Get value for given key
   * @param key the key
   * @return value
   */
  public byte[] get(byte[] key) {
    return get(dbRef, key);
  }

  public ResultPair scan(byte[] key, int limiter) {
    return scan(dbRef, key, limiter);
  }

  /**
   * Put key-value pair into the database
   * @param key key
   * @param value value
   * @return true if successful
   */
  public boolean put(byte[] key, byte[] value) {
    return put(dbRef, key, value);
  }

  private native long Open(String fileName, int type);

  private native boolean put(long dbRef, byte[] key, byte[] value);

  private native byte[] get(long dbRef, byte[] key);

  private native boolean delete(long dbRef, byte[] key);

  private native ResultPair scan(long dbRef, byte[] key, int limiter);

  private native void close(long dbRef);

  /**
   * Flush and close database
   */
  @Override
  public void close() {
    if (dbClosed.compareAndSet(false, true)) {
      close(dbRef);
    }
  }
}
