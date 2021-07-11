package com.asinking.connector.clickhouse.table.internal.executor;

import org.apache.flink.table.data.RowData;
import org.apache.flink.types.RowKind;

import org.apache.flink.shaded.guava18.com.google.common.util.concurrent.AbstractExecutionThreadService;

import com.asinking.connector.clickhouse.table.internal.converter.ClickHouseRowConverter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.yandex.clickhouse.ClickHouseConnection;
import ru.yandex.clickhouse.ClickHousePreparedStatement;

import java.io.IOException;
import java.sql.SQLException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

public class ClickHouseBatchExecutor implements ClickHouseExecutor {

  private static final long serialVersionUID = 1L;

  private static final Logger LOG = LoggerFactory.getLogger(ClickHouseBatchExecutor.class);
  private final String sql;
  private final ClickHouseRowConverter converter;
  private final List<RowData> batch;
  private final Duration flushInterval;
  private final int maxRetries;
  private transient ClickHousePreparedStatement stmt;
  private transient ExecuteBatchService service;

  public ClickHouseBatchExecutor(String sql, ClickHouseRowConverter converter, Duration flushInterval, int maxRetries) {
    this.sql = sql;
    this.converter = converter;
    this.flushInterval = flushInterval;
    this.maxRetries = maxRetries;
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
    if (record.getRowKind() != RowKind.DELETE && record.getRowKind() != RowKind.UPDATE_BEFORE) {
      this.batch.add(record);
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
      this.stmt = null;
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
        synchronized (ClickHouseBatchExecutor.this) {
          ClickHouseBatchExecutor.this.wait(ClickHouseBatchExecutor.this.flushInterval.toMillis());
          if (!ClickHouseBatchExecutor.this.batch.isEmpty()) {
            for (RowData r : ClickHouseBatchExecutor.this.batch) {
              ClickHouseBatchExecutor.LOG.info("try to trans row {}", r);
              ClickHouseBatchExecutor.this.converter.toClickHouse(r, ClickHouseBatchExecutor.this.stmt);
              ClickHouseBatchExecutor.this.stmt.addBatch();
            }
            attemptExecuteBatch();
          }
        }
      }
    }

    private void attemptExecuteBatch() throws IOException {
      for (int i = 1; i <= ClickHouseBatchExecutor.this.maxRetries; i++) {
        try {
          ClickHouseBatchExecutor.this.stmt.executeBatch();
          ClickHouseBatchExecutor.this.batch.clear();
          break;
        } catch (SQLException e) {
          ClickHouseBatchExecutor.LOG.error("ClickHouse executeBatch error, retry times = {}", i, e);
          if (i >= ClickHouseBatchExecutor.this.maxRetries) {

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
