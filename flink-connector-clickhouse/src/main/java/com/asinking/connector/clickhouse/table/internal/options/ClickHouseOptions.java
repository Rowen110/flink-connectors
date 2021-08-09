package com.asinking.connector.clickhouse.table.internal.options;


import java.io.Serializable;
import java.time.Duration;
import java.util.Objects;
import java.util.Optional;
import javax.annotation.Nullable;

public class ClickHouseOptions implements Serializable {
  private static final long serialVersionUID = 1L;

  private final String url;

  @Nullable
  private final String username;

  @Nullable
  private final String password;

  private final String clusterName;

  private final String databaseName;

  private final String tableName;

  @Nullable
  private final String tableCollapsingField;

  @Nullable
  private final String tableReplacingField;

  private final int batchSize;

  private final Duration flushInterval;

  private final int maxRetries;

  private final boolean writeLocal;

  private final String writeLocalNodes;

  private final String partitionStrategy;

  private final String partitionKey;

  private final boolean ignoreDelete;

  private ClickHouseOptions(String url, String username, String password, String clusterName,
                            String databaseName,
                            String tableName, String tableCollapsingField, String tableReplacingField,
                            int batchSize, Duration flushInterval,
                            int maxRetires, boolean writeLocal, String writeLocalNodes, String partitionStrategy,
                            String partitionKey, boolean ignoreDelete) {
    this.url = url;
    this.username = username;
    this.password = password;
    this.clusterName = clusterName;
    this.databaseName = databaseName;
    this.tableName = tableName;
    this.tableCollapsingField = tableCollapsingField;
    this.tableReplacingField = tableReplacingField;
    this.batchSize = batchSize;
    this.flushInterval = flushInterval;
    this.maxRetries = maxRetires;
    this.writeLocal = writeLocal;
    this.writeLocalNodes = writeLocalNodes;
    this.partitionStrategy = partitionStrategy;
    this.partitionKey = partitionKey;
    this.ignoreDelete = ignoreDelete;
  }

  public String getUrl() {
    return this.url;
  }

  public Optional<String> getUsername() {
    return Optional.ofNullable(this.username);
  }

  public Optional<String> getPassword() {
    return Optional.ofNullable(this.password);
  }

  public String getClusterName() {
    return this.clusterName;
  }

  public String getDatabaseName() {
    return this.databaseName;
  }

  public String getTableName() {
    return this.tableName;
  }

  public Optional<String> getTableCollapsingField() {
    return Optional.ofNullable(this.tableCollapsingField);
  }

  public Optional<String> getTableReplacingField() {
    return Optional.ofNullable(this.tableReplacingField);
  }

  public int getBatchSize() {
    return this.batchSize;
  }

  public Duration getFlushInterval() {
    return this.flushInterval;
  }

  public int getMaxRetries() {
    return this.maxRetries;
  }

  public boolean getWriteLocal() {
    return this.writeLocal;
  }

  public String getWriteLocalNodes() {
    return this.writeLocalNodes;
  }

  public String getPartitionStrategy() {
    return this.partitionStrategy;
  }

  public String getPartitionKey() {
    return this.partitionKey;
  }

  public boolean getIgnoreDelete() {
    return this.ignoreDelete;
  }

  @Override
  public String toString() {
    return "ClickHouseOptions{url='" + this.url + '\'' + ", databaseName='" + this.databaseName + '\'' +
        ", tableName='" + this.tableName + '\'' + ", batchSize=" + this.batchSize + ", flushInterval=" +
        this.flushInterval + ", maxRetries=" + this.maxRetries + ", writeLocal=" + this.writeLocal +
        ", writeLocalNodes=" + this.writeLocalNodes + ", partitionStrategy='" + this.partitionStrategy + '\'' +
        ", partitionKey='" + this.partitionKey + '\'' + ", ignoreDelete=" + this.ignoreDelete + '}';
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    ClickHouseOptions options = (ClickHouseOptions) o;
    return (getBatchSize() == options.getBatchSize() &&
        getMaxRetries() == options.getMaxRetries() &&
        getWriteLocal() == options.getWriteLocal() &&
        getWriteLocalNodes().equals(options.getWriteLocalNodes()) &&
        getIgnoreDelete() == options.getIgnoreDelete() &&
        Objects.equals(getUrl(), options.getUrl()) &&
        Objects.equals(getUsername(), options.getUsername()) &&
        Objects.equals(getPassword(), options.getPassword()) &&
        Objects.equals(getDatabaseName(), options.getDatabaseName()) &&
        Objects.equals(getTableName(), options.getTableName()) &&
        Objects.equals(getFlushInterval(), options.getFlushInterval()) &&
        Objects.equals(getPartitionStrategy(), options.getPartitionStrategy()) &&
        Objects.equals(getPartitionKey(), options.getPartitionKey()));
  }

  @Override
  public int hashCode() {
    return
        Objects.hash(getUrl(), getUsername(), getPassword(), getDatabaseName(), getTableName(),
            Integer.valueOf(getBatchSize()), getFlushInterval(), Integer.valueOf(getMaxRetries()),
            Boolean.valueOf(getWriteLocal()),
            getWriteLocalNodes(),
            getPartitionStrategy(), getPartitionKey(), Boolean.valueOf(getIgnoreDelete()));
  }

  public static class Builder {
    private String url;

    private String username;

    private String password;

    private String clusterName;

    private String databaseName;

    private String tableName;

    private String tableCollapsingField;

    private String tableReplacingField;

    private int batchSize;

    private Duration flushInterval;

    private int maxRetries;

    private boolean writeLocal;

    private String writeLocalNodes;

    private String partitionStrategy;

    private String partitionKey;

    private boolean ignoreDelete;

    public Builder withUrl(String url) {
      this.url = url;
      return this;
    }

    public Builder withUsername(String username) {
      this.username = username;
      return this;
    }

    public Builder withPassword(String password) {
      this.password = password;
      return this;
    }

    public Builder withClusterName(String clusterName) {
      this.clusterName = clusterName;
      return this;
    }

    public Builder withDatabaseName(String databaseName) {
      this.databaseName = databaseName;
      return this;
    }

    public Builder withTableName(String tableName) {
      this.tableName = tableName;
      return this;
    }

    public Builder withTableCollapsingField(String tableCollapsingField) {
      this.tableCollapsingField = tableCollapsingField;
      return this;
    }

    public Builder withTableReplacingField(String tableReplacingField) {
      this.tableReplacingField = tableReplacingField;
      return this;
    }

    public Builder withBatchSize(int batchSize) {
      this.batchSize = batchSize;
      return this;
    }

    public Builder withFlushInterval(Duration flushInterval) {
      this.flushInterval = flushInterval;
      return this;
    }

    public Builder withMaxRetries(int maxRetries) {
      this.maxRetries = maxRetries;
      return this;
    }

    public Builder withWriteLocal(Boolean writeLocal) {
      this.writeLocal = writeLocal.booleanValue();
      return this;
    }

    public Builder withWriteLocalNodes(String writeLocalNodes) {
      this.writeLocalNodes = writeLocalNodes;
      return this;
    }

    public Builder withPartitionStrategy(String partitionStrategy) {
      this.partitionStrategy = partitionStrategy;
      return this;
    }

    public Builder withPartitionKey(String partitionKey) {
      this.partitionKey = partitionKey;
      return this;
    }

    public Builder withIgnoreDelete(boolean ignoreDelete) {
      this.ignoreDelete = ignoreDelete;
      return this;
    }

    public ClickHouseOptions build() {
      return new ClickHouseOptions(this.url, this.username, this.password, this.clusterName, this.databaseName,
          this.tableName, this.tableCollapsingField, this.tableReplacingField, this.batchSize, this.flushInterval,
          this.maxRetries,
          this.writeLocal, this.writeLocalNodes, this.partitionStrategy, this.partitionKey, this.ignoreDelete);
    }
  }
}