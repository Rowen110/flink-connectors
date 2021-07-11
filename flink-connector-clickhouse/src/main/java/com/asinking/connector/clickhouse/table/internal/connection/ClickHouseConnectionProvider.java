package com.asinking.connector.clickhouse.table.internal.connection;

import com.asinking.connector.clickhouse.table.internal.options.ClickHouseOptions;
import java.io.Serializable;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.flink.util.StringUtils;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.yandex.clickhouse.ClickHouseConnection;

public class ClickHouseConnectionProvider implements Serializable {
  private static final long serialVersionUID = 1L;

  private static final Logger LOG = LoggerFactory.getLogger(ClickHouseConnectionProvider.class);

  private static final String CLICKHOUSE_DRIVER_NAME = "ru.yandex.clickhouse.ClickHouseDriver";

  private static final Pattern PATTERN = Pattern.compile("You must use port (?<port>[0-9]+) for HTTP.");
  private final ClickHouseOptions options;
  private transient ClickHouseConnection connection;
  private transient List<ClickHouseConnection> shardConnections;

  public ClickHouseConnectionProvider(ClickHouseOptions options) {
    this.options = options;
  }

  public synchronized ClickHouseConnection getConnection() throws SQLException {
    if (this.connection == null) {
      this.connection = createConnection(this.options.getUrl(), this.options
          .getDatabaseName());
    }
    return this.connection;
  }

  private ClickHouseConnection createConnection(String url, String database) throws SQLException {
    ClickHouseConnection conn;
    LOG.info("connecting to {} database {}", url, database);
    try {
      Class.forName("ru.yandex.clickhouse.ClickHouseDriver");
    } catch (ClassNotFoundException e) {
      throw new SQLException(e);
    }
    if (this.options.getUsername().isPresent()) {
      conn = (ClickHouseConnection) DriverManager.getConnection(getJdbcUrl(url, database), this.options
          .getUsername().orElse(null), this.options.getPassword().orElse(null));
    } else {
      conn = (ClickHouseConnection) DriverManager.getConnection(getJdbcUrl(url, database));
    }
    LOG.info("success connected to {} database {}", url, database);
    return conn;
  }

  public synchronized List<ClickHouseConnection> getShardConnections() throws SQLException {
    if (this.shardConnections == null) {
      this.shardConnections = getShardConnections(getLocalNodes());
    }
    return this.shardConnections;
  }

  private synchronized List<ClickHouseConnection> getShardConnections(String localNodes) throws SQLException {
    if (this.shardConnections == null) {
      this.shardConnections = new ArrayList<>();
      String[] nodes = localNodes.split(",");
      for (String node : nodes) {
        String url = "clickhouse://" + node;
        this.shardConnections.add(createConnection(url, this.options.getDatabaseName()));
      }
    }
    return this.shardConnections;
  }

  public synchronized String getLocalNodes() throws SQLException {
    if (!StringUtils.isNullOrWhitespaceOnly(this.options.getWriteLocalNodes())) {
      LOG.info("local nodes from config {}", this.options.getWriteLocalNodes());
      return this.options.getWriteLocalNodes();
    }
    String localNodes = getLocalNodesFromClickhouse();
    LOG.info("local nodes from clickhouse {}", localNodes);
    return localNodes;
  }

  public synchronized String getLocalNodesFromClickhouse() throws SQLException {
    ClickHouseConnection conn = getConnection();
    PreparedStatement stmt = conn.prepareStatement(
        "SELECT shard_num, host_address, port FROM system.clusters WHERE cluster = ? and replica_num = 1");
    stmt.setString(1, this.options.getClusterName());
    List<String> localNodes = new ArrayList<>();
    LOG.info("try to get local nodes from clickhouse use {}", stmt);
    try (ResultSet rs = stmt.executeQuery()) {
      this.shardConnections = new ArrayList<>();
      while (rs.next()) {
        String host = rs.getString("host_address");
        int port = getActualHttpPort(host, rs.getInt("port"));
        LOG.info("get host {} port {}", host, Integer.valueOf(port));
        localNodes.add(host + ":" + port);
      }
    }
    if (localNodes.isEmpty()) {

      throw new SQLException("unable to query shards in system.clusters");
    }
    return String.join(",", localNodes);
  }

  private int getActualHttpPort(String host, int port) throws SQLException {
    try (CloseableHttpClient httpclient = HttpClients.createDefault()) {
      HttpGet request = new HttpGet((new URIBuilder()).setScheme("http").setHost(host).setPort(port).build());
      CloseableHttpResponse closeableHttpResponse = httpclient.execute(request);
      int statusCode = closeableHttpResponse.getStatusLine().getStatusCode();
      if (statusCode == 200) {
        return port;
      }
      String raw = EntityUtils.toString(closeableHttpResponse.getEntity());
      Matcher matcher = PATTERN.matcher(raw);
      if (matcher.find()) {
        return Integer.parseInt(matcher.group("port"));
      }
      throw new SQLException("Cannot query ClickHouse http port " + host + ":" + port);
    } catch (Exception e) {
      throw new SQLException("Cannot connect to ClickHouse server using HTTP", e);
    }
  }

  public void closeConnections() throws SQLException {
    if (this.connection != null) {

      this.connection.close();
    }
    if (this.shardConnections != null) {
      for (ClickHouseConnection shardConnection : this.shardConnections) {
        shardConnection.close();
      }
    }

  }

  private String getJdbcUrl(String url, String database) throws SQLException {
    try {
      return "jdbc:" + (new URIBuilder(url)).setPath("/" + database).build().toString();
    } catch (Exception e) {
      throw new SQLException(e);
    }
  }

  public String queryTableEngine(String databaseName, String tableName) throws SQLException {
    ClickHouseConnection conn = getConnection();
    try (PreparedStatement stmt = conn
        .prepareStatement("SELECT engine_full FROM system.tables WHERE database = ? AND name = ?")) {
      stmt.setString(1, databaseName);
      stmt.setString(2, tableName);
      try (ResultSet rs = stmt.executeQuery()) {
        if (rs.next()) {
          return rs.getString("engine_full");
        }
      }
    }
    throw new SQLException("table `" + databaseName + "`.`" + tableName + "` does not exist");
  }
}
