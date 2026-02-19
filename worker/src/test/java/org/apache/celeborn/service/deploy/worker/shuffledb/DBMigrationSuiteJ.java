package org.apache.celeborn.service.deploy.worker.shuffledb;

import org.apache.celeborn.common.util.JavaUtils;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.UUID;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class DBMigrationSuiteJ {
    @Test
    public void testMigrateAllDBs() throws IOException {
        File dbDir = new File(System.getProperty("java.io.tmpdir"), "rocksdb_migration_test_original" + UUID.randomUUID());
        File newParentPath = new File(System.getProperty("java.io.tmpdir"), "rocksdb_migration_test_migrated" + UUID.randomUUID());
        try {
            StoreVersion v1 = new StoreVersion(1, 0);
            DB db = DBProvider.initDB(DBBackend.ROCKSDB, dbDir.toPath().toString(), new File(dbDir, "my_new_rocks_db"), v1);
            // Add some KV pairs to make sure the db is created and can be migrated.
            db.put("key1".getBytes(), "value1".getBytes());
            db.put("key2".getBytes(), "value2".getBytes());
            db.put("key3".getBytes(), "value3".getBytes());

            DBProvider.migrateAllDBs(newParentPath.getPath());
            assertTrue(db instanceof RocksDB);
            RocksDB rocksDB = (RocksDB) db;
            assertTrue(rocksDB.getDBPath().startsWith(newParentPath.getPath()));

            // Validate the data is correctly migrated by reading the values back.
            assertArrayEquals("value1".getBytes(), rocksDB.get("key1".getBytes()));
            assertArrayEquals("value2".getBytes(), rocksDB.get("key2".getBytes()));
            assertArrayEquals("value3".getBytes(), rocksDB.get("key3".getBytes()));

            // Validate older path deleted
            assertFalse(rocksDB.getDBPath().startsWith(dbDir.getPath()));

            // Validate original parent path is deleted
            assertFalse(dbDir.exists());
        } finally {
            JavaUtils.deleteRecursively(dbDir);
            JavaUtils.deleteRecursively(newParentPath);
        }
    }
}
