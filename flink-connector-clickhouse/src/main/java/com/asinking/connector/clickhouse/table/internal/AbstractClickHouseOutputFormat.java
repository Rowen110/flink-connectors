package com.asinking.connector.clickhouse.table.internal;

import java.io.Flushable;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.function.Supplier;
import com.asinking.connector.clickhouse.table.internal.connection.ClickHouseConnectionProvider;
import com.asinking.connector.clickhouse.table.internal.converter.ClickHouseRowConverter;
import com.asinking.connector.clickhouse.table.internal.executor.ClickHouseBatchExecutor;
import com.asinking.connector.clickhouse.table.internal.executor.ClickHouseExecutor;
import com.asinking.connector.clickhouse.table.internal.options.ClickHouseOptions;
import com.asinking.connector.clickhouse.table.internal.partitioner.ClickHousePartitioner;
import org.apache.flink.api.common.io.RichOutputFormat;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.api.constraints.UniqueConstraint;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.util.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class AbstractClickHouseOutputFormat extends RichOutputFormat<RowData>
        implements Flushable {
    private static final long serialVersionUID = 1L;

    @Override
    public void configure(Configuration parameters) {}

    @FunctionalInterface
    public interface ExecutorFactory extends Supplier<ClickHouseExecutor>, Serializable {}

    public static class Builder implements Serializable {
        private static final Logger LOG = LoggerFactory.getLogger(Builder.class);

        private DataType[] fieldDataTypes;

        private ClickHouseOptions options;

        private String[] fieldNames;

        private boolean hasPrimaryKey;

        public Builder withOptions(ClickHouseOptions options) {
            this.options = options;
            return this;
        }

        public Builder withFieldDataTypes(DataType[] fieldDataTypes) {
            this.fieldDataTypes = fieldDataTypes;
            return this;
        }

        public Builder withFieldNames(String[] fieldNames) {
            this.fieldNames = fieldNames;
            return this;
        }

        public Builder withPrimaryKey(Optional<UniqueConstraint> primaryKey) {
            this.hasPrimaryKey = primaryKey.isPresent();
            return this;
        }

        public AbstractClickHouseOutputFormat build() {
            Preconditions.checkNotNull(this.options);
            Preconditions.checkNotNull(this.fieldNames);
            Preconditions.checkNotNull(this.fieldDataTypes);
            LogicalType[] logicalTypes =
                    Arrays.stream(this.fieldDataTypes)
                            .map(DataType::getLogicalType)
                            .toArray(LogicalType[]::new);
            ClickHouseRowConverter converter =
                    new ClickHouseRowConverter(
                            RowType.of(logicalTypes),
                            (this.hasPrimaryKey
                                    && this.options.getTableCollapsingField().isPresent()),
                            (this.hasPrimaryKey
                                    && this.options.getTableReplacingField().isPresent()));
            LOG.info("ClickHouseOptions: {}", this.options);
            if (this.hasPrimaryKey && this.options.getTableCollapsingField().isPresent()) {
                LOG.warn(
                        "If primary key and collapsing field is specified, connector will be in UPDATE mode.");
                this.options
                        .getTableCollapsingField()
                        .ifPresent(
                                field -> {
                                    String[] newField = new String[this.fieldNames.length + 1];
                                    System.arraycopy(
                                            this.fieldNames,
                                            0,
                                            newField,
                                            0,
                                            this.fieldNames.length);
                                    newField[this.fieldNames.length] = field;
                                    this.fieldNames = newField;
                                });
            }
            if (this.hasPrimaryKey && this.options.getTableReplacingField().isPresent()) {
                LOG.warn(
                        "If primary key and deleted flag is specified, connector will be in UPSERT mode.");
                this.options
                        .getTableReplacingField()
                        .ifPresent(
                                field -> {
                                    String[] newField = new String[this.fieldNames.length + 1];
                                    System.arraycopy(
                                            this.fieldNames,
                                            0,
                                            newField,
                                            0,
                                            this.fieldNames.length);
                                    newField[this.fieldNames.length] = field;
                                    this.fieldNames = newField;
                                });
            }
            if (this.options.getWriteLocal()) {
                return createShardOutputFormat(logicalTypes, converter);
            }
            return createBatchOutputFormat(converter);
        }

        private AbstractClickHouseOutputFormat.ExecutorFactory createExecutorFactory(
                ClickHouseRowConverter converter) {
            if (this.hasPrimaryKey && this.options.getTableCollapsingField().isPresent()) {
                if (this.options.getTableCollapsingField().isPresent()) {
                    LOG.info("use collapsing");
                    return () ->
                            ClickHouseExecutor.createCollapsingExecutor(
                                    this.options.getTableName(),
                                    this.fieldNames,
                                    converter,
                                    this.options);
                }
                throw new IllegalArgumentException(
                        "update mode. need to set `table.collapsing.field`");
            }
            if (this.hasPrimaryKey && this.options.getTableReplacingField().isPresent()) {
                if (this.options.getTableReplacingField().isPresent()) {
                    LOG.info("use replacing");
                    return () ->
                            ClickHouseExecutor.createReplacingExecutor(
                                    this.options.getTableName(),
                                    this.fieldNames,
                                    converter,
                                    this.options);
                }
                throw new IllegalArgumentException(
                        "upsert mode. need to set `table.replacing.field`.");
            }
            LOG.info("use insert only");
            String sql =
                    ClickHouseStatementFactory.getInsertIntoStatement(
                            this.options.getTableName(), this.fieldNames);
            return () ->
                    new ClickHouseBatchExecutor(
                            sql,
                            converter,
                            this.options.getFlushInterval(),
                            this.options.getMaxRetries());
        }

        private ClickHouseBatchOutputFormat createBatchOutputFormat(
                ClickHouseRowConverter converter) {
            return new ClickHouseBatchOutputFormat(
                    new ClickHouseConnectionProvider(this.options),
                    createExecutorFactory(converter),
                    this.options);
        }

        private ClickHouseShardOutputFormat createShardOutputFormat(
                LogicalType[] logicalTypes, ClickHouseRowConverter converter) {
            ClickHousePartitioner partitioner;
            List<RowData.FieldGetter> getterList;
            LOG.info("use shard mode. record will write to shard");
            switch (this.options.getPartitionStrategy()) {
                case ClickHousePartitioner.BALANCED:
                    partitioner = ClickHousePartitioner.createBalanced();
                    break;
                case ClickHousePartitioner.SHUFFLE:
                    partitioner = ClickHousePartitioner.createShuffle();
                    break;
                case ClickHousePartitioner.HASH:
                    getterList = new ArrayList<>();
                    for (String partitionKey : this.options.getPartitionKey().split(",")) {
                        int index = Arrays.asList(this.fieldNames).indexOf(partitionKey.trim());
                        if (index == -1) {
                            throw new IllegalArgumentException(
                                    "Partition key `"
                                            + partitionKey.trim()
                                            + "` not found in table schema");
                        }
                        RowData.FieldGetter getter =
                                RowData.createFieldGetter(logicalTypes[index], index);
                        getterList.add(getter);
                    }
                    partitioner = ClickHousePartitioner.createHash(getterList);
                    break;
                default:
                    throw new IllegalArgumentException(
                            "Unknown sink.partition-strategy `"
                                    + this.options.getPartitionStrategy()
                                    + "`");
            }
            return new ClickHouseShardOutputFormat(
                    new ClickHouseConnectionProvider(this.options),
                    partitioner,
                    createExecutorFactory(converter),
                    this.options);
        }
    }
}
