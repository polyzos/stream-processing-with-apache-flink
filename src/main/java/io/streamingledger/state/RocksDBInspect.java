package io.streamingledger.state;

import org.rocksdb.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class RocksDBInspect {
    private static final Logger logger
            = LoggerFactory.getLogger(RocksDBInspect.class);

    public static void main(String[] args) {
        RocksDB.loadLibrary();

        var cfOptions = new ColumnFamilyOptions()
                .optimizeUniversalStyleCompaction();

        var cfDescriptors = Arrays.asList(
                new ColumnFamilyDescriptor(RocksDB.DEFAULT_COLUMN_FAMILY, cfOptions),
                new ColumnFamilyDescriptor("transactionState".getBytes(), cfOptions),
                new ColumnFamilyDescriptor("customerState".getBytes(), cfOptions),
                new ColumnFamilyDescriptor("_timer_state/event_user-timers".getBytes(), cfOptions),
                new ColumnFamilyDescriptor("_timer_state/processing_user-timers".getBytes(), cfOptions)
        );
        System.out.println(System.getProperty("user.dir"));
        File[] files = new File(System.getProperty("user.dir") + "/logs/flink/tm1/state/rocksdb/")
                .listFiles();

        assert files != null;
        Arrays.stream(files).forEach(dir -> {
            // a list which will hold the handles for the column families once the db is opened
            List<ColumnFamilyHandle> columnFamilyHandleList = new ArrayList<>();
            var dbPath = dir + "/db/";
            var options = new DBOptions()
                    .setCreateIfMissing(true)
                    .setCreateMissingColumnFamilies(true);

            try {
                RocksDB db = RocksDB
                        .open(options, dbPath, cfDescriptors, columnFamilyHandleList);
                logger.info("Processing state of operator: {}", Arrays.stream(dbPath.split("/")).filter(s -> s.startsWith("job")));

                columnFamilyHandleList.forEach(columnFamilyHandle -> {
                    var count = 0;
                    var iterator = db.newIterator(columnFamilyHandle);
                    iterator.seekToFirst();
                    while (iterator.isValid()) {
                        count += 1;
                        iterator.next();
                    }
                    try {
                        var name = new String(columnFamilyHandle.getName());
                        logger.info("\tColumn Family '{}' has {} entries.", name, count);
                    } catch (RocksDBException e) {
                        throw new RuntimeException(e);
                    }

                });
            } catch (RocksDBException e) {
                throw new RuntimeException(e);
            }
        });
    }
}
