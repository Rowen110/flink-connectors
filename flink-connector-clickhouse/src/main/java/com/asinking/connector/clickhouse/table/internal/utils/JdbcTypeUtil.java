package com.asinking.connector.clickhouse.table.internal.utils;

import java.sql.JDBCType;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.LocalTimeTypeInfo;
import org.apache.flink.api.common.typeinfo.PrimitiveArrayTypeInfo;
import org.apache.flink.api.common.typeinfo.SqlTimeTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.ObjectArrayTypeInfo;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.inference.TypeTransformations;
import org.apache.flink.table.types.utils.DataTypeUtils;

@Internal
public class JdbcTypeUtil {
  private static final Map<TypeInformation<?>, Integer> TYPE_MAPPING;
  private static final Map<Integer, String> SQL_TYPE_NAMES;

  static {
    Map<TypeInformation<?>, Integer> m = new HashMap<>();
    m.put(BasicTypeInfo.STRING_TYPE_INFO, JDBCType.VARCHAR.getVendorTypeNumber());
    m.put(BasicTypeInfo.BOOLEAN_TYPE_INFO, JDBCType.BOOLEAN.getVendorTypeNumber());
    m.put(BasicTypeInfo.BYTE_TYPE_INFO, JDBCType.TINYINT.getVendorTypeNumber());
    m.put(BasicTypeInfo.SHORT_TYPE_INFO, JDBCType.SMALLINT.getVendorTypeNumber());
    m.put(BasicTypeInfo.INT_TYPE_INFO, JDBCType.INTEGER.getVendorTypeNumber());
    m.put(BasicTypeInfo.LONG_TYPE_INFO, JDBCType.BIGINT.getVendorTypeNumber());
    m.put(BasicTypeInfo.FLOAT_TYPE_INFO, JDBCType.FLOAT.getVendorTypeNumber());
    m.put(BasicTypeInfo.DOUBLE_TYPE_INFO, JDBCType.FLOAT.getVendorTypeNumber());
    m.put(SqlTimeTypeInfo.DATE, JDBCType.DATE.getVendorTypeNumber());
    m.put(SqlTimeTypeInfo.TIME, JDBCType.TIME.getVendorTypeNumber());
    m.put(SqlTimeTypeInfo.TIMESTAMP, JDBCType.TIMESTAMP.getVendorTypeNumber());
    m.put(LocalTimeTypeInfo.LOCAL_DATE, JDBCType.DATE.getVendorTypeNumber());
    m.put(LocalTimeTypeInfo.LOCAL_TIME, JDBCType.TIME.getVendorTypeNumber());
    m.put(LocalTimeTypeInfo.LOCAL_DATE_TIME, JDBCType.TIMESTAMP.getVendorTypeNumber());
    m.put(BasicTypeInfo.BIG_DEC_TYPE_INFO, JDBCType.DECIMAL.getVendorTypeNumber());
    m.put(PrimitiveArrayTypeInfo.BYTE_PRIMITIVE_ARRAY_TYPE_INFO, JDBCType.BINARY.getVendorTypeNumber());
    TYPE_MAPPING = Collections.unmodifiableMap(m);
    Map<Integer, String> names = new HashMap<>();
    names.put(JDBCType.VARCHAR.getVendorTypeNumber(), JDBCType.VARCHAR.getName());
    names.put(JDBCType.BOOLEAN.getVendorTypeNumber(), JDBCType.BOOLEAN.getName());
    names.put(JDBCType.TINYINT.getVendorTypeNumber(), JDBCType.TINYINT.getName());
    names.put(JDBCType.SMALLINT.getVendorTypeNumber(), JDBCType.SMALLINT.getName());
    names.put(JDBCType.INTEGER.getVendorTypeNumber(), JDBCType.INTEGER.getName());
    names.put(JDBCType.BIGINT.getVendorTypeNumber(), JDBCType.BIGINT.getName());
    names.put(JDBCType.FLOAT.getVendorTypeNumber(), JDBCType.FLOAT.getName());
    names.put(JDBCType.FLOAT.getVendorTypeNumber(), JDBCType.FLOAT.getName());
    names.put(JDBCType.CHAR.getVendorTypeNumber(), JDBCType.CHAR.getName());
    names.put(JDBCType.DATE.getVendorTypeNumber(), JDBCType.DATE.getName());
    names.put(JDBCType.TIME.getVendorTypeNumber(), JDBCType.TIME.getName());
    names.put(JDBCType.TIMESTAMP.getVendorTypeNumber(), JDBCType.TIMESTAMP.getName());
    names.put(JDBCType.DECIMAL.getVendorTypeNumber(), JDBCType.DECIMAL.getName());
    names.put(JDBCType.BINARY.getVendorTypeNumber(), JDBCType.BINARY.getName());
    SQL_TYPE_NAMES = Collections.unmodifiableMap(names);
  }

  private JdbcTypeUtil() {
  }

  public static int typeInformationToSqlType(TypeInformation<?> type) {
    if (TYPE_MAPPING.containsKey(type)) {
      return TYPE_MAPPING.get(type);
    } else if (!(type instanceof ObjectArrayTypeInfo) && !(type instanceof PrimitiveArrayTypeInfo)) {
      throw new IllegalArgumentException("Unsupported type: " + type);
    } else {
      return JDBCType.ARRAY.getVendorTypeNumber();
    }
  }

  public static String getTypeName(int type) {
    return SQL_TYPE_NAMES.get(type);
  }

  public static String getTypeName(TypeInformation<?> type) {
    return SQL_TYPE_NAMES.get(typeInformationToSqlType(type));
  }

  public static TableSchema normalizeTableSchema(TableSchema schema) {
    TableSchema.Builder physicalSchemaBuilder = TableSchema.builder();
    schema.getTableColumns().forEach((c) -> {
      if (!c.isPhysical()) {
        DataType type =
            DataTypeUtils.transform(c.getType(), TypeTransformations.timeToSqlTypes());
        physicalSchemaBuilder.field(c.getName(), type);
      }

    });
    return physicalSchemaBuilder.build();
  }
}