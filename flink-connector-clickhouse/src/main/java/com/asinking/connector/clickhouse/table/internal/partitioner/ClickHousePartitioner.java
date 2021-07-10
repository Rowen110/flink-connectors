package com.asinking.connector.clickhouse.table.internal.partitioner;


import java.io.Serializable;
import java.util.List;
import org.apache.flink.table.data.RowData;

public interface ClickHousePartitioner extends Serializable {
    public static final String BALANCED = "balanced";

    public static final String SHUFFLE = "shuffle";

    public static final String HASH = "hash";

    int select(RowData paramRowData, int paramInt);

    static ClickHousePartitioner createBalanced() {
        return new BalancedPartitioner();
    }

    static ClickHousePartitioner createShuffle() {
        return new ShufflePartitioner();
    }

    static ClickHousePartitioner createHash(List<RowData.FieldGetter> getter) {
        return new HashPartitioner(getter);
    }
}