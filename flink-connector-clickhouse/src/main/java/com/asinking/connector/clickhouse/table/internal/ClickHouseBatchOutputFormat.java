package com.asinking.connector.clickhouse.table.internal;

import com.asinking.connector.clickhouse.table.internal.connection.ClickHouseConnectionProvider;
import com.asinking.connector.clickhouse.table.internal.executor.ClickHouseExecutor;
import com.asinking.connector.clickhouse.table.internal.options.ClickHouseOptions;
import java.io.IOException;
import java.sql.SQLException;
import javax.annotation.Nonnull;
import org.apache.flink.table.data.RowData;
import org.apache.flink.util.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.yandex.clickhouse.ClickHouseConnection;

public class ClickHouseBatchOutputFormat extends AbstractClickHouseOutputFormat {
  private static final long serialVersionUID = 1L;
  private static final Logger LOG = LoggerFactory.getLogger(ClickHouseBatchOutputFormat.class);
  private final ClickHouseConnectionProvider connectionProvider;
  private final ClickHouseExecutor executor;
  private final ClickHouseOptions options;
  private transient ClickHouseConnection connection;
  private transient int batchCount = 0;
  private transient boolean closed = false;

  protected ClickHouseBatchOutputFormat(@Nonnull ClickHouseConnectionProvider connectionProvider,
                                        @Nonnull ExecutorFactory executorFactory, @Nonnull ClickHouseOptions options) {
    this.connectionProvider = Preconditions.checkNotNull(connectionProvider);
    this.executor = executorFactory.get();
    this.options = Preconditions.checkNotNull(options);
  }

  @Override
  public void open(int taskNumber, int numTasks) throws IOException {
    try {
      this.connection = this.connectionProvider.getConnection();
      LOG.info("clickhouse open");
      this.executor.prepareStatement(this.connection);
    } catch (Exception var4) {
      throw new IOException("unable to establish connection with ClickHouse", var4);
    }
  }

  @Override
  public void writeRecord(RowData record) throws IOException {
    this.addBatch(record);
    ++this.batchCount;
    if (this.batchCount >= this.options.getBatchSize()) {
      this.flush();
    }

  }

  private void addBatch(RowData record) {
    this.executor.addBatch(record);
  }

  @Override
  public void flush() throws IOException {
    this.executor.executeBatch();
    this.batchCount = 0;
  }

  @Override
  public void close() throws IOException {
    if (!this.closed) {
      this.closed = true;
      if (this.batchCount > 0) {
        try {
          this.flush();
        } catch (Exception var2) {
          LOG.warn("Writing records to ClickHouse failed.", var2);
        }
      }

      this.closeConnection();
    }

  }

  private void closeConnection() {
    if (this.connection != null) {
      try {
        this.executor.closeStatement();
        this.connectionProvider.closeConnections();
      } catch (SQLException var5) {
        LOG.warn("ClickHouse connection could not be closed: {}", var5.getMessage());
      } finally {
        this.connection = null;
      }
    }

  }
}
