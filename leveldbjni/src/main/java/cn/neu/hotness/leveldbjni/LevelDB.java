package cn.neu.hotness.leveldbjni;

import java.io.Closeable;
import java.io.File;
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
   * @param optDir  Directory for the option config
   */
  public LevelDB(File file, int type, File optDir) {
    dbRef = open(file.getAbsolutePath(), type, optDir.getAbsolutePath());
  }


  protected long open(String fileName, int type, String OptionName) {
    long dbRef = Open(fileName, type, OptionName);
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

  /**
   * get leveldb status
   * @return leveldb status string
   */
  public byte[] getLeveldbStatus() {
    return GetLeveldbStatus(dbRef);
  }

  private native long Open(String fileName, int type, String optDir);

  private native boolean put(long dbRef, byte[] key, byte[] value);

  private native byte[] get(long dbRef, byte[] key);

  private native boolean delete(long dbRef, byte[] key);

  private native ResultPair scan(long dbRef, byte[] key, int limiter);

  private native void close(long dbRef);

  private native byte[] GetLeveldbStatus(long dbRef);

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
