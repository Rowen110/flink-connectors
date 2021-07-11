package com.asinking.connector.clickhouse.table.internal.partitioner;


import org.apache.flink.table.data.RowData;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

public class HashPartitioner implements ClickHousePartitioner {

    private final List<RowData.FieldGetter> getterList;

    public HashPartitioner(List<RowData.FieldGetter> getterList) {
        this.getterList = getterList;
    }

    @Override
    public int select(RowData record, int numShards) {
        List<Object> key = new ArrayList<>();
        for (RowData.FieldGetter getter : this.getterList) {
            key.add(getter.getFieldOrNull(record));
        }
        return (Objects.hashCode(key) % numShards + numShards) % numShards;
    }
}
