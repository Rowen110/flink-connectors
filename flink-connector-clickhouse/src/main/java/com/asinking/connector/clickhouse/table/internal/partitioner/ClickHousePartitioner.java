package com.asinking.connector.clickhouse.table.internal.partitioner;


import java.io.Serializable;
import java.util.List;
import org.apache.flink.table.data.RowData;

public interface ClickHousePartitioner extends Serializable {
  String BALANCED = "balanced";

  String SHUFFLE = "shuffle";

  String HASH = "hash";

  static ClickHousePartitioner createBalanced() {
    return new BalancedPartitioner();
  }

  static ClickHousePartitioner createShuffle() {
    return new ShufflePartitioner();
  }

  static ClickHousePartitioner createHash(List<RowData.FieldGetter> getter) {
    return new HashPartitioner(getter);
  }

  int select(RowData paramRowData, int paramInt);
}