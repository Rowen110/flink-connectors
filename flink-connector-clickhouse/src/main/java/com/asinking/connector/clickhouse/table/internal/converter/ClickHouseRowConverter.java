package com.asinking.connector.clickhouse.table.internal.converter;

import com.asinking.connector.clickhouse.table.internal.utils.JdbcTypeUtil;
import java.io.Serializable;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;
import java.time.LocalDate;
import java.time.LocalTime;
import org.apache.flink.table.data.DecimalData;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.types.logical.DecimalType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.LogicalTypeRoot;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.TimestampType;
import org.apache.flink.table.types.utils.TypeConversions;
import org.apache.flink.util.Preconditions;
import ru.yandex.clickhouse.ClickHousePreparedStatement;

public class ClickHouseRowConverter implements Serializable {

  private final RowType rowType;

  private final LogicalType[] fieldTypes;

  private final DeserializationConverter[] toFlinkConverters;

  private final SerializationConverter[] toClickHouseConverters;

  private final boolean collapsing;

  public ClickHouseRowConverter(RowType rowType, boolean collapsing) {
    this.rowType = Preconditions.checkNotNull(rowType);
    this.fieldTypes = rowType.getFields().stream().map(RowType.RowField::getType)
        .toArray(type -> new LogicalType[type]);
    toFlinkConverters = new DeserializationConverter[rowType.getFieldCount()];
    toClickHouseConverters = new SerializationConverter[rowType.getFieldCount()];
    for (int i = 0; i < rowType.getFieldCount(); i++) {
      toFlinkConverters[i] = createToFlinkConverter(rowType.getTypeAt(i));
      toClickHouseConverters[i] = createNullableToClickHouseConverter(this.fieldTypes[i]);
    }
    this.collapsing = collapsing;
  }

  public RowData toFlink(ResultSet resultSet) throws SQLException {
    GenericRowData genericRowData = new GenericRowData(this.rowType.getFieldCount());
    for (int pos = 0; pos < this.rowType.getFieldCount(); pos++) {
      Object field = resultSet.getObject(pos + 1);
      genericRowData.setField(pos, toFlinkConverters[pos].deserialize(field));
    }
    return genericRowData;
  }

  public ClickHousePreparedStatement toClickHouse(RowData rowData, ClickHousePreparedStatement statement)
      throws SQLException {
    for (int index = 0; index < rowData.getArity(); ++index) {
      toClickHouseConverters[index].serialize(rowData, index, statement);
    }

    if (this.collapsing) {
      switch (rowData.getRowKind()) {
        case INSERT:
        case UPDATE_AFTER:
          statement.setInt(rowData.getArity() + 1, 1);
          break;
        case UPDATE_BEFORE:
        case DELETE:
          statement.setInt(rowData.getArity() + 1, -1);
      }
    }

    return statement;
  }

  protected DeserializationConverter createToFlinkConverter(LogicalType type) {
    int precision;
    int scale;
    switch (type.getTypeRoot()) {
      case NULL:
        return val -> null;
      case BOOLEAN:
      case FLOAT:
      case DOUBLE:
      case INTERVAL_YEAR_MONTH:
      case INTERVAL_DAY_TIME:
        return val -> val;
      case TINYINT:
        return val -> Byte.valueOf(((Integer) val).byteValue());
      case SMALLINT:
        return val -> (val instanceof Integer) ? Short.valueOf(((Integer) val).shortValue()) : val;
      case INTEGER:
        return val -> val;
      case BIGINT:
        return val -> val;
      case DECIMAL:
        precision = ((DecimalType) type).getPrecision();
        scale = ((DecimalType) type).getScale();
        return val -> (val instanceof BigInteger) ?
            DecimalData.fromBigDecimal(new BigDecimal((BigInteger) val, 0), precision, scale) :
            DecimalData.fromBigDecimal((BigDecimal) val, precision, scale);
      case DATE:
        return val -> Integer.valueOf((int) ((Date) val).toLocalDate().toEpochDay());
      case TIME_WITHOUT_TIME_ZONE:
        return val -> Integer.valueOf((int) (((Time) val).toLocalTime().toNanoOfDay() / 1000000L));
      case TIMESTAMP_WITH_TIME_ZONE:
      case TIMESTAMP_WITHOUT_TIME_ZONE:
        return val -> TimestampData.fromTimestamp((Timestamp) val);
      case CHAR:
      case VARCHAR:
        return val -> StringData.fromString((String) val);
      case BINARY:
      case VARBINARY:
        return val -> val;
    }
    throw new UnsupportedOperationException("Unsupported type:" + type);
  }

  protected SerializationConverter createNullableToClickHouseConverter(LogicalType type) {
    return wrapIntoNullableToClickHouseConverter(createToClickHouseConverter(type), type);
  }

  protected SerializationConverter wrapIntoNullableToClickHouseConverter(SerializationConverter converter,
                                                                         LogicalType type) {
    int sqlType = JdbcTypeUtil.typeInformationToSqlType(TypeConversions.fromDataTypeToLegacyInfo(
        TypeConversions.fromLogicalToDataType(type)));
    return (val, index, statement) -> {
      if (val == null || val.isNullAt(index) || LogicalTypeRoot.NULL.equals(type.getTypeRoot())) {
        statement.setNull(index + 1, sqlType);
      } else {
        converter.serialize(val, index, statement);
      }
    };
  }

  protected SerializationConverter createToClickHouseConverter(LogicalType type) {
    int timestampPrecision;
    int decimalPrecision;
    int decimalScale;
    switch (type.getTypeRoot()) {
      case BOOLEAN:
        return (val, index, statement) -> statement.setBoolean(index + 1, val.getBoolean(index));
      case TINYINT:
        return (val, index, statement) -> statement.setByte(index + 1, val.getByte(index));
      case SMALLINT:
        return (val, index, statement) -> statement.setShort(index + 1, val.getShort(index));
      case INTERVAL_YEAR_MONTH:
      case INTEGER:
        return (val, index, statement) -> statement.setInt(index + 1, val.getInt(index));
      case INTERVAL_DAY_TIME:
      case BIGINT:
        return (val, index, statement) -> statement.setLong(index + 1, val.getLong(index));
      case FLOAT:
        return (val, index, statement) -> statement.setFloat(index + 1, val.getFloat(index));
      case DOUBLE:
        return (val, index, statement) -> statement.setDouble(index + 1, val.getDouble(index));
      case CHAR:
      case VARCHAR:
        return (val, index, statement) -> statement.setString(index + 1, val.getString(index).toString());
      case BINARY:
      case VARBINARY:
        return (val, index, statement) -> statement.setBytes(index + 1, val.getBinary(index));
      case DATE:
        return (val, index, statement) -> statement
            .setDate(index + 1, Date.valueOf(LocalDate.ofEpochDay(val.getInt(index))));
      case TIME_WITHOUT_TIME_ZONE:
        return (val, index, statement) -> statement
            .setTime(index + 1, Time.valueOf(LocalTime.ofNanoOfDay(val.getInt(index) * 1000000L)));
      case TIMESTAMP_WITH_TIME_ZONE:
      case TIMESTAMP_WITHOUT_TIME_ZONE:
        timestampPrecision = ((TimestampType) type).getPrecision();
        return (val, index, statement) -> statement
            .setTimestamp(index + 1, val.getTimestamp(index, timestampPrecision).toTimestamp());
      case DECIMAL:
        decimalPrecision = ((DecimalType) type).getPrecision();
        decimalScale = ((DecimalType) type).getScale();
        return (val, index, statement) -> statement
            .setBigDecimal(index + 1, val.getDecimal(index, decimalPrecision, decimalScale).toBigDecimal());
    }
    throw new UnsupportedOperationException("Unsupported type:" + type);
  }

  @FunctionalInterface
  interface DeserializationConverter extends Serializable {
    Object deserialize(Object param1Object) throws SQLException;
  }

  @FunctionalInterface
  interface SerializationConverter extends Serializable {
    void serialize(RowData param1RowData, int param1Int, PreparedStatement param1PreparedStatement) throws SQLException;
  }
}
