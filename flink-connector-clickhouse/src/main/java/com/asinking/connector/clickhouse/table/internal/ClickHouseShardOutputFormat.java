package com.asinking.connector.clickhouse.table.internal;

import com.asinking.connector.clickhouse.table.internal.connection.ClickHouseConnectionProvider;
import com.asinking.connector.clickhouse.table.internal.executor.ClickHouseExecutor;
import com.asinking.connector.clickhouse.table.internal.options.ClickHouseOptions;
import com.asinking.connector.clickhouse.table.internal.partitioner.ClickHousePartitioner;
import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import javax.annotation.Nonnull;
import org.apache.flink.table.data.RowData;
import org.apache.flink.util.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.yandex.clickhouse.ClickHouseConnection;

public class ClickHouseShardOutputFormat extends AbstractClickHouseOutputFormat {
  private static final Logger LOG = LoggerFactory.getLogger(ClickHouseShardOutputFormat.class);
  private final ClickHouseConnectionProvider connectionProvider;
  private final ClickHousePartitioner partitioner;
  private final ClickHouseOptions options;
  private final ExecutorFactory executorFactory;
  private final List<ClickHouseExecutor> shardExecutors;
  private transient boolean closed = false;
  private transient ClickHouseConnection connection;
  private transient List<ClickHouseConnection> shardConnections;
  private transient int[] batchCounts;

  protected ClickHouseShardOutputFormat(@Nonnull ClickHouseConnectionProvider connectionProvider,
                                        @Nonnull ClickHousePartitioner partitioner,
                                        @Nonnull ExecutorFactory executorFactory, @Nonnull ClickHouseOptions options) {
    this.connectionProvider = Preconditions.checkNotNull(connectionProvider);
    this.partitioner = Preconditions.checkNotNull(partitioner);
    this.executorFactory = executorFactory;
    this.options = Preconditions.checkNotNull(options);
    this.shardExecutors = new ArrayList();
  }

  @Override
  public void open(int taskNumber, int numTasks) throws IOException {
    try {
      this.connection = this.connectionProvider.getConnection();
      this.establishShardConnections();
      this.initializeExecutors();
    } catch (Exception e) {
      throw new IOException("unable to establish connection to ClickHouse", e);
    }
  }

  private void establishShardConnections() throws IOException {
    try {
      this.shardConnections = connectionProvider.getShardConnections();
      this.batchCounts = new int[this.shardConnections.size()];
    } catch (SQLException e) {
      throw new IOException(e);
    }
  }

  private void initializeExecutors() throws SQLException {
    LOG.info("shardConnections size {}, begin to create executor", this.shardConnections.size());
    Iterator iterator = this.shardConnections.iterator();

    while (iterator.hasNext()) {
      ClickHouseConnection shardConnection = (ClickHouseConnection) iterator.next();
      ClickHouseExecutor clickHouseExecutor = this.executorFactory.get();
      clickHouseExecutor.prepareStatement(shardConnection);
      LOG.info("add executor {}", clickHouseExecutor);
      this.shardExecutors.add(clickHouseExecutor);
    }

  }

  @Override
  public void writeRecord(RowData record) throws IOException {
    int selected = this.partitioner.select(record, this.shardExecutors.size());
    this.shardExecutors.get(selected).addBatch(record);
    int counts = this.batchCounts[selected]++;
    if (this.batchCounts[selected] >= this.options.getBatchSize()) {
      this.flush(selected);
    }

  }

  @Override
  public void flush() throws IOException {
    for (int i = 0; i < this.shardExecutors.size(); ++i) {
      this.flush(i);
    }

  }

  public void flush(int index) throws IOException {
    this.shardExecutors.get(index).executeBatch();
    this.batchCounts[index] = 0;
  }

  @Override
  public void close() throws IOException {
    if (!this.closed) {
      this.closed = true;

      try {
        this.flush();
      } catch (Exception var2) {
        LOG.warn("Writing records to ClickHouse failed.", var2);
      }

      this.closeConnection();
    }

  }

  private void closeConnection() {
    try {
      Iterator iterator = this.shardExecutors.iterator();

      while (iterator.hasNext()) {
        ClickHouseExecutor shardExecutor = (ClickHouseExecutor) iterator.next();
        shardExecutor.closeStatement();
      }

      this.connectionProvider.closeConnections();
    } catch (SQLException var6) {
      LOG.warn("ClickHouse connection could not be closed: {}", var6.getMessage());
    } finally {
      this.connection = null;
    }

  }
}
