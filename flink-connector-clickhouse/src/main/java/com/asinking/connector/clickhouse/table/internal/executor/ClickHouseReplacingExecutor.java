package com.asinking.connector.clickhouse.table.internal.executor;

import com.asinking.connector.clickhouse.table.internal.converter.ClickHouseRowConverter;
import com.asinking.connector.clickhouse.table.internal.options.ClickHouseOptions;
import java.io.IOException;
import java.sql.SQLException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import org.apache.flink.shaded.guava18.com.google.common.util.concurrent.AbstractExecutionThreadService;
import org.apache.flink.table.data.RowData;
import org.apache.flink.types.RowKind;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.yandex.clickhouse.ClickHouseConnection;
import ru.yandex.clickhouse.ClickHousePreparedStatement;

public class ClickHouseReplacingExecutor implements ClickHouseExecutor {
  private static final long serialVersionUID = 1L;

  private static final Logger LOG = LoggerFactory.getLogger(ClickHouseReplacingExecutor.class);
  private final String sql;
  private final ClickHouseRowConverter converter;
  private final Duration flushInterval;
  private final int maxRetries;
  private final List<RowData> batch;
  private transient ClickHousePreparedStatement stmt;
  private transient ExecuteBatchService service;

  public ClickHouseReplacingExecutor(String sql, ClickHouseRowConverter converter, ClickHouseOptions options) {
    this.sql = sql;
    this.converter = converter;
    this.flushInterval = options.getFlushInterval();
    this.maxRetries = options.getMaxRetries();
    this.batch = new ArrayList<>();
  }

  @Override
  public void prepareStatement(ClickHouseConnection connection) throws SQLException {
    this.stmt = (ClickHousePreparedStatement) connection.prepareStatement(this.sql);
    this.service = new ExecuteBatchService();
    this.service.startAsync();
  }

  @Override
  public synchronized void addBatch(RowData record) {
    checkRowKing(record.getRowKind());
    this.batch.add(record);
  }

  private void checkRowKing(RowKind rowKind) {
    switch (rowKind) {
      case INSERT:
      case UPDATE_AFTER:
      case DELETE:
      case UPDATE_BEFORE:
        return;
      default:
        throw new UnsupportedOperationException(
            String.format(
                "Unknown row kind, the supported row kinds is: INSERT, UPDATE_BEFORE, UPDATE_AFTER, DELETE, but get: %s.",
                rowKind));
    }
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
    if (this.stmt != null) {
      this.stmt.close();
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
        synchronized (ClickHouseReplacingExecutor.this) {
          ClickHouseReplacingExecutor.this.wait(ClickHouseReplacingExecutor.this
              .flushInterval.toMillis());
          if (!ClickHouseReplacingExecutor.this.batch.isEmpty()) {
            ClickHouseReplacingExecutor.this.stmt.clearBatch();
            for (RowData record : ClickHouseReplacingExecutor.this.batch) {
              ClickHouseReplacingExecutor.this.converter.toClickHouse(record, ClickHouseReplacingExecutor.this.stmt);
              ClickHouseReplacingExecutor.this.stmt.addBatch();
            }
            attemptExecuteBatch(ClickHouseReplacingExecutor.this.stmt, ClickHouseReplacingExecutor.this.batch);
          }
        }
      }
    }

    private void attemptExecuteBatch(ClickHousePreparedStatement stmt, List<RowData> batch) throws IOException {
      for (int i = 1; i <= ClickHouseReplacingExecutor.this.maxRetries; i++) {
        try {
          stmt.executeBatch();
          batch.clear();
          break;
        } catch (SQLException e) {
          ClickHouseReplacingExecutor.LOG
              .error("ClickHouse executeBatch error, retry times = {}", Integer.valueOf(i), e);
          if (i >= ClickHouseReplacingExecutor.this.maxRetries) {
            throw new IOException(e);
          }
          try {
            Thread.sleep((1000L * i));
          } catch (InterruptedException ex) {
            Thread.currentThread().interrupt();
            throw new IOException("unable to flush; interrupted while doing another attempt", e);
          }
        }
      }
    }
  }
}