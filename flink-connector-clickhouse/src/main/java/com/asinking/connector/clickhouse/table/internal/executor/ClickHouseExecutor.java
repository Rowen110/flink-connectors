package com.asinking.connector.clickhouse.table.internal.executor;

import com.asinking.connector.clickhouse.table.internal.ClickHouseStatementFactory;
import com.asinking.connector.clickhouse.table.internal.converter.ClickHouseRowConverter;
import com.asinking.connector.clickhouse.table.internal.options.ClickHouseOptions;
import java.io.IOException;
import java.io.Serializable;
import java.sql.SQLException;
import java.util.Optional;
import org.apache.flink.shaded.guava18.com.google.common.util.concurrent.AbstractExecutionThreadService;
import org.apache.flink.table.data.RowData;
import ru.yandex.clickhouse.ClickHouseConnection;

public interface ClickHouseExecutor extends Serializable {
  static ClickHouseUpsertExecutor createUpsertExecutor(String tableName, String[] fieldNames, String[] keyFields,
                                                       ClickHouseRowConverter converter, ClickHouseOptions options) {
    String insertSql = ClickHouseStatementFactory.getInsertIntoStatement(tableName, fieldNames);
    String updateSql =
        ClickHouseStatementFactory.getUpdateStatement(tableName, fieldNames, keyFields, Optional.empty());
    String deleteSql = ClickHouseStatementFactory.getDeleteStatement(tableName, keyFields, Optional.empty());
    return new ClickHouseUpsertExecutor(insertSql, updateSql, deleteSql, converter, options);
  }

  static ClickHouseCollapsingExecutor createCollapsingExecutor(String tableName, String[] fieldNames,
                                                               ClickHouseRowConverter converter,
                                                               ClickHouseOptions options) {
    String sql =
        ClickHouseStatementFactory.getCollapsingInsert(tableName, fieldNames, options.getTableCollapsingField());
    return new ClickHouseCollapsingExecutor(sql, converter, options);
  }

  static ClickHouseReplacingExecutor createReplacingExecutor(String tableName, String[] fieldNames,
                                                             ClickHouseRowConverter converter,
                                                             ClickHouseOptions options) {
    String sql =
        ClickHouseStatementFactory.getReplacingInsert(tableName, fieldNames, options.getTableReplacingField());
    return new ClickHouseReplacingExecutor(sql, converter, options);
  }

  static ClickHouseBatchExecutor createBatchExecutor(String tableName, String[] fieldNames,
                                                     ClickHouseRowConverter converter, ClickHouseOptions options) {
    String sql = ClickHouseStatementFactory.getInsertIntoStatement(tableName, fieldNames);
    return new ClickHouseBatchExecutor(sql, converter, options.getFlushInterval(), options.getMaxRetries());
  }

  void prepareStatement(ClickHouseConnection clickHouseConnection) throws SQLException;

  void addBatch(RowData rowData);

  void executeBatch() throws IOException;

  void closeStatement() throws SQLException;

  AbstractExecutionThreadService getExecuteService();
}
