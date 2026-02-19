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

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import com.google.common.annotations.VisibleForTesting;
import org.apache.celeborn.common.CelebornConf;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Function1;

/** Note: code copied from Apache Spark. */
public class DBProvider {
  private static final Logger logger = LoggerFactory.getLogger(DBProvider.class);
  private static final List<DB> dbInstances = new ArrayList<>();

  public static Pair<DB, File> initDBWithFallbackChecks(CelebornConf conf,
                                                 DBBackend dbBackend, String fileName, StoreVersion version) throws IOException {
    DB db = null;
    File dbFile = null;
    List<String> fallbackPaths = new ArrayList<>();
    fallbackPaths.add(conf.workerGracefulShutdownRecoverPath());
    conf.workerGracefulShutdownRecoverPathFallbacks()
            .foreach(new Function1<String, Object>() {
              @Override
              public Object apply(String v1) {
                return fallbackPaths.add(v1);
              }
            });

    File candidateFile;
    for (String path : fallbackPaths) {
      candidateFile = new File(path, fileName);
      try {
        DB tempDb = initDB(dbBackend, path, candidateFile, version, false);
        if (tempDb != null) {
          db = tempDb;
          dbFile = candidateFile;
          logger.info("Successfully initialized DB at fallback path: {}", path);
          break;
        }
      } catch (Exception e) {
          logger.warn("Failed to initialize DB at fallback path: {}", path, e);
      }
    }

    if (db == null) {
      logger.info("Failed to initialize DB at all fallback paths. Attempting to initialize at primary path.");
      dbFile = new File(conf.workerGracefulShutdownRecoverPath(), fileName);
      db = initDB(dbBackend, conf.workerGracefulShutdownRecoverPath(), dbFile, version, true);
    }

    return Pair.of(db, dbFile);
  }

  @VisibleForTesting
  public static DB initDB(DBBackend dbBackend, String baseParentPath,
                                    File dbFile, StoreVersion version) throws IOException {
    return initDB(dbBackend, baseParentPath, dbFile, version, true);
  }

  private static DB initDB(DBBackend dbBackend, String baseParentPath,
                          File dbFile, StoreVersion version, boolean createIfMissing)
      throws IOException {
    if (dbFile != null) {
      switch (dbBackend) {
        case LEVELDB:
          org.iq80.leveldb.DB levelDB = LevelDBProvider.initLevelDB(dbFile, version);
          logger.warn("The LEVELDB is deprecated. Please use ROCKSDB instead.");
          DB db = levelDB != null ? new LevelDB(levelDB) : null;
          if (db != null) {
            dbInstances.add(db);
          }
          return db;
        case ROCKSDB:
          org.rocksdb.RocksDB rocksDB = RocksDBProvider.initRockDB(dbFile, version, createIfMissing);
          DB rocksDBInstance = rocksDB != null
                  ? new org.apache.celeborn.service.deploy.worker.shuffledb.RocksDB(
                  rocksDB, version, baseParentPath, dbFile)
                  : null;
          if (rocksDBInstance != null) {
            dbInstances.add(rocksDBInstance);
          }

          return rocksDBInstance;
        default:
          throw new IllegalArgumentException("Unsupported DBBackend: " + dbBackend);
      }
    }
    return null;
  }

  public static void migrateAllDBs(String newParentPath) throws IOException {
    File newParentDir = new File(newParentPath);
    if (!newParentDir.exists()) {
        if (!newParentDir.mkdirs()) {
            throw new IOException("Failed to create directory for DB migration: " + newParentPath);
        }
    }
    for (DB db : dbInstances) {
      db.migrate(newParentPath);
    }
  }
}
