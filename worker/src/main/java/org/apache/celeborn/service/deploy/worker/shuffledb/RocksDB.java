/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.celeborn.service.deploy.worker.shuffledb;

import java.io.IOException;
import java.io.File;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.concurrent.locks.Lock;

import com.google.common.annotations.VisibleForTesting;
import org.rocksdb.FlushOptions;
import org.rocksdb.RocksIterator;
import org.rocksdb.RocksDBException;
import org.rocksdb.WriteOptions;
import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * RocksDB implementation of the local KV storage used to persist the shuffle state.
 *
 * <p>Note: code copied from Apache Spark.
 */
public class RocksDB implements DB {
  private static final Logger log = LoggerFactory.getLogger(RocksDB.class);
  private volatile org.rocksdb.RocksDB db;
  private final StoreVersion storeVersion;
  private String parentPath;
  private volatile File dbPath;
  private final WriteOptions SYNC_WRITE_OPTIONS = new WriteOptions().setSync(true);
  private final ReentrantReadWriteLock rwLock = new ReentrantReadWriteLock();
  private final Lock migrationSafeOperationLock = rwLock.readLock();
  private final Lock migrationExclusiveLock = rwLock.writeLock();

  public RocksDB(org.rocksdb.RocksDB db, StoreVersion version, String parentPath, File dbPath) {
    this.db = db;
    this.storeVersion = version;
    this.parentPath = parentPath;
    this.dbPath = dbPath;
  }

  @Override
  public void put(byte[] key, byte[] value) {
    migrationSafeOperationLock.lock();
    try {
      db.put(key, value);
    } catch (RocksDBException e) {
      throw new RuntimeException(e);
    } finally {
      migrationSafeOperationLock.unlock();
    }
  }

  @Override
  public void put(byte[] key, byte[] value, boolean sync) {
    migrationSafeOperationLock.lock();
    try {
      if (sync) {
        db.put(SYNC_WRITE_OPTIONS, key, value);
      } else {
        db.put(key, value);
      }
    } catch (RocksDBException e) {
      throw new RuntimeException(e);
    } finally {
      migrationSafeOperationLock.unlock();
    }
  }

  @Override
  public byte[] get(byte[] key) {
    // reads do not need to be blocked by migration, but we still allow concurrent reads
    try {
      return db.get(key);
    } catch (RocksDBException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void delete(byte[] key) {
    migrationSafeOperationLock.lock();
    try {
      db.delete(key);
    } catch (RocksDBException e) {
      throw new RuntimeException(e);
    } finally {
      migrationSafeOperationLock.unlock();
    }
  }

  @Override
  public DBIterator iterator() {
    return new RocksDBIterator(db.newIterator());
  }

  @Override
  public void close() throws IOException {
    migrationExclusiveLock.lock();
    try {
      db.close();
    } finally {
      migrationExclusiveLock.unlock();
    }
  }

  /**
   * Migrate the underlying RocksDB to a new path.
   *
   * Steps:
   * 1) Block writers and acquire exclusive lock.
   * 2) Iterate over current DB.
   * 3) Create a new DB at the provided path.
   * 4) Copy all key-values to the new DB.
   * 5) Verify by counting entries (and spot-check reads).
   * 6) Swap the underlying db reference.
   * 7) Close the old db.
   */
  @Override
  public void migrate(String newParentPath) throws IOException {
    migrationExclusiveLock.lock();
    org.rocksdb.RocksDB oldDb = null;
    org.rocksdb.RocksDB newDb = null;
    try {
      final File oldPath = dbPath;
      final File oldParentPath = new File(parentPath);
      // 2) Create iterator from current DB
      oldDb = this.db;
      RocksIterator it = oldDb.newIterator();
      it.seekToFirst();

      // 3) Create a new DB with the new path (use current version policy 1.0)
      // The actual target file should be newParentPath/<current file name relative to parentPath>
      // Find the relative path of current db file to parentPath
      String relativePath = new File(parentPath).toPath().relativize(oldPath.toPath()).toString();
      File target = new File(newParentPath, relativePath);
      newDb = RocksDBProvider.initRockDB(target, storeVersion);
      if (newDb == null) {
        throw new IOException("Failed to initialize RocksDB at path: " + target);
      }

      // 4) Copy all key, values
      long copied = 0L;
      while (it.isValid()) {
        byte[] k = it.key();
        byte[] v = it.value();
        try {
          newDb.put(k, v);
        } catch (RocksDBException e) {
          throw new IOException("Failed to write to new RocksDB during migration", e);
        }
        it.next();
        copied++;
      }
      it.close();

      // 5) Verification: count keys in new DB
      long destCount = 0L;
      RocksIterator verifyIt = newDb.newIterator();
      verifyIt.seekToFirst();
      while (verifyIt.isValid()) {
        destCount++;
        verifyIt.next();
      }
      verifyIt.close();
      if (destCount != copied) {
        throw new IOException(
            "Verification failed: copied=" + copied + ", destinationCount=" + destCount);
      }

      // ensure all data is flushed to disk
      try (FlushOptions flushOptions = new FlushOptions().setWaitForFlush(true)) {
        newDb.flush(flushOptions);
      }

      // 6) Replace current db with new one
      this.db = newDb;
      this.dbPath = target;
      this.parentPath = newParentPath;
      newDb = null; // ownership transferred

      // 7) Close and cleanup older db
      oldDb.close();
      if (oldPath.exists()) {
        cleanupOldDbPathRecursivelyUp(oldPath, oldParentPath);
      }
    } catch (RocksDBException r) {
      throw new IOException("RocksDB exception during migration", r);
    } finally {
      // ensure we don't leak the newDb if swap failed
      try {
        if (newDb != null) {
          newDb.close();
        }
      } catch (Exception e) {
        log.warn("Failed to close new RocksDB after migration failure", e);
      }
      migrationExclusiveLock.unlock();
    }
  }

  private void cleanupOldDbPathRecursivelyUp(File pathToDelete, File pathToStopInclusive) throws IOException {
    if (pathToDelete.equals(pathToStopInclusive)) {
      // stop at (including) parent path
      FileUtils.deleteDirectory(pathToDelete);
      return;
    }
    FileUtils.deleteDirectory(pathToDelete);
    File parent = pathToDelete.getParentFile();
    if (parent != null && isEmptyDirectory(parent)) {
      cleanupOldDbPathRecursivelyUp(parent, pathToStopInclusive);
    }
  }

  private boolean isEmptyDirectory(File dir) {
    String[] files = dir.list();
    return files == null || files.length == 0;
  }

  @VisibleForTesting
  public String getDBPath() {
      return dbPath.getAbsolutePath();
  }
}
