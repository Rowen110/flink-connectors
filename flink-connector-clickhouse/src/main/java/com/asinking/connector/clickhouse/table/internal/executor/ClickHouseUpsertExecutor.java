package com.asinking.connector.clickhouse.table.internal.executor;

import com.asinking.connector.clickhouse.table.internal.converter.ClickHouseRowConverter;
import com.asinking.connector.clickhouse.table.internal.options.ClickHouseOptions;
import java.io.IOException;
import java.sql.SQLException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.apache.flink.shaded.guava18.com.google.common.util.concurrent.AbstractExecutionThreadService;
import org.apache.flink.table.data.RowData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.yandex.clickhouse.ClickHouseConnection;
import ru.yandex.clickhouse.ClickHousePreparedStatement;
import ru.yandex.clickhouse.ClickHouseStatement;

public class ClickHouseUpsertExecutor implements ClickHouseExecutor {
  private static final long serialVersionUID = 1L;

  private static final Logger LOG = LoggerFactory.getLogger(ClickHouseUpsertExecutor.class);
  private final String insertSql;
  private final String updateSql;
  private final String deleteSql;
  private final ClickHouseRowConverter converter;
  private final List<RowData> insertBatch;
  private final List<RowData> updateBatch;
  private final List<RowData> deleteBatch;
  private final Duration flushInterval;
  private final int maxRetries;
  private transient ClickHousePreparedStatement insertStmt;
  private transient ClickHousePreparedStatement updateStmt;
  private transient ClickHousePreparedStatement deleteStmt;
  private transient ExecuteBatchService service;

  public ClickHouseUpsertExecutor(String insertSql, String updateSql, String deleteSql,
                                  ClickHouseRowConverter converter, ClickHouseOptions options) {
    this.insertSql = insertSql;
    this.updateSql = updateSql;
    this.deleteSql = deleteSql;
    this.converter = converter;
    this.flushInterval = options.getFlushInterval();
    this.maxRetries = options.getMaxRetries();
    this.insertBatch = new ArrayList<>();
    this.updateBatch = new ArrayList<>();
    this.deleteBatch = new ArrayList<>();
  }

  @Override
  public void prepareStatement(ClickHouseConnection connection) throws SQLException {
    this.insertStmt = (ClickHousePreparedStatement) connection.prepareStatement(this.insertSql);
    this.updateStmt = (ClickHousePreparedStatement) connection.prepareStatement(this.updateSql);
    this.deleteStmt = (ClickHousePreparedStatement) connection.prepareStatement(this.deleteSql);
    this.service = new ExecuteBatchService();
    this.service.startAsync();
  }

  @Override
  public synchronized void addBatch(RowData record) {
    switch (record.getRowKind()) {
      case INSERT:
        this.insertBatch.add(record);
        return;
      case UPDATE_AFTER:
        this.updateBatch.add(record);
        return;
      case DELETE:
        this.deleteBatch.add(record);
        return;
      case UPDATE_BEFORE:
        return;
    }
    throw new UnsupportedOperationException(
        String.format(
            "Unknown row kind, the supported row kinds is: INSERT, UPDATE_BEFORE, UPDATE_AFTER, DELETE, but get: %s.",
            record.getRowKind()));
  }

  @Override
  public synchronized void executeBatch() throws IOException {
    if (this.service.isRunning()) {
      notifyAll();
    } else {
      throw new IOException("executor unexpectedly terminated", this.service.failureCause());
    }
  }

  @Override
  public void closeStatement() throws SQLException {
    if (this.service != null) {
      this.service.stopAsync().awaitTerminated();
    } else {
      LOG.warn("executor closed before initialized");
    }
    for (ClickHouseStatement stmt : Arrays.asList(
        this.insertStmt, this.updateStmt, this.deleteStmt)) {
        if (stmt != null) {
            stmt.close();
        }
    }
  }

  @Override
  public AbstractExecutionThreadService getExecuteService() {
    return this.service;
  }

  private class ExecuteBatchService extends AbstractExecutionThreadService {
    private ExecuteBatchService() {
    }

    @Override
    protected void run() throws Exception {
      while (isRunning()) {
        synchronized (ClickHouseUpsertExecutor.this) {
          ClickHouseUpsertExecutor.this.wait(ClickHouseUpsertExecutor.this
              .flushInterval.toMillis());
          processBatch(ClickHouseUpsertExecutor.this.insertStmt, ClickHouseUpsertExecutor.this
              .insertBatch);
          processBatch(ClickHouseUpsertExecutor.this.updateStmt, ClickHouseUpsertExecutor.this
              .updateBatch);
          processBatch(ClickHouseUpsertExecutor.this.deleteStmt, ClickHouseUpsertExecutor.this
              .deleteBatch);
        }
      }
    }

    private void processBatch(ClickHousePreparedStatement stmt, List<RowData> batch) throws SQLException, IOException {
      if (!batch.isEmpty()) {
        for (RowData r : batch) {
          ClickHouseUpsertExecutor.this.converter.toClickHouse(r, stmt);
          stmt.addBatch();
        }
        attemptExecuteBatch(stmt, batch);
      }
    }

    private void attemptExecuteBatch(ClickHousePreparedStatement stmt, List<RowData> batch) throws IOException {
      for (int i = 1; i <= ClickHouseUpsertExecutor.this.maxRetries; i++) {
        try {
          stmt.executeBatch();
          batch.clear();
          break;
        } catch (SQLException e) {
          ClickHouseUpsertExecutor.LOG.error("ClickHouse executeBatch error, retry times = {}", Integer.valueOf(i), e);
            if (i >= ClickHouseUpsertExecutor.this.maxRetries) {
                throw new IOException(e);
            }
          try {
            Thread.sleep((1000 * i));
          } catch (InterruptedException ex) {
            Thread.currentThread().interrupt();
            throw new IOException("unable to flush; interrupted while doing another attempt", e);
          }
        }
      }
    }
  }
}
