package org.apache.celeborn.service.deploy.worker.shuffledb

import org.apache.celeborn.common.CelebornConf
import org.apache.celeborn.common.CelebornConf.{WORKER_GRACEFUL_SHUTDOWN_ENABLED, WORKER_GRACEFUL_SHUTDOWN_RECOVER_PATH, WORKER_GRACEFUL_SHUTDOWN_RECOVER_PATH_FALLBACKS}
import org.scalatest.funsuite.AnyFunSuite

import java.io.File
import java.util.UUID

class DBSuiteFallbacksTest extends AnyFunSuite  {
  test("Test DBProvider fallback") {
    val conf = new CelebornConf()

    val dbDir = new File(System.getProperty("java.io.tmpdir"), "rocksdb_migration_test_original" + UUID.randomUUID)
    dbDir.mkdirs()
    val fallbackPath = new File(System.getProperty("java.io.tmpdir"), "rocksdb_migration_test_migrated" + UUID.randomUUID)
    fallbackPath.mkdirs()

    conf.set(WORKER_GRACEFUL_SHUTDOWN_RECOVER_PATH, dbDir.getPath)
    conf.set(WORKER_GRACEFUL_SHUTDOWN_RECOVER_PATH_FALLBACKS, Seq(fallbackPath.getPath))

    val db = DBProvider.initDBWithFallbackChecks(conf, DBBackend.ROCKSDB, "my_new_rocksdb", new StoreVersion(1, 0))
    val rocksDb = db.getLeft
    val path = db.getRight

    // Must be created at the original path, not the fallback path
    assert(path.getParent.equals(dbDir.getPath))

    // write some data to the db
    rocksDb.put("key1".getBytes, "value1".getBytes)
    rocksDb.put("key2".getBytes, "value2".getBytes)
    rocksDb.put("key3".getBytes, "value3".getBytes)

    // Migrate all
    DBProvider.migrateAllDBs(fallbackPath.getPath)

    // Close and initialize again
    rocksDb.close()

    val db2 = DBProvider.initDBWithFallbackChecks(conf, DBBackend.ROCKSDB, "my_new_rocksdb", new StoreVersion(1, 0))
    val rocksDb2 = db2.getLeft
    val path2 = db2.getRight

    // Must be created at the new fallback path, not the original path
    assert(path2.getParent.equals(fallbackPath.getPath))

    // Ensure all data is migrated correctly
    assert(new String(rocksDb2.get("key1".getBytes)) == "value1")
    assert(new String(rocksDb2.get("key2".getBytes)) == "value2")
    assert(new String(rocksDb2.get("key3".getBytes)) == "value3")
  }
}
