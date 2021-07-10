package com.asinking.connector.clickhouse.table.internal.partitioner;

import java.util.concurrent.ThreadLocalRandom;
import org.apache.flink.table.data.RowData;

public class ShufflePartitioner implements ClickHousePartitioner {
    private static final long serialVersionUID = 1L;

    @Override
    public int select(RowData record, int numShards) {
        return ThreadLocalRandom.current().nextInt(numShards);
    }
}
