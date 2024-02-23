/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.inlong.sort.cdc.dm.source;

import akka.protobuf.ByteString;
import com.ververica.cdc.debezium.utils.TemporalConversions;
import dm.jdbc.driver.DmdbClob;
import lombok.extern.slf4j.Slf4j;
import oracle.sql.CHAR;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.table.data.DecimalData;
import org.apache.flink.table.data.GenericArrayData;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.types.logical.DecimalType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.types.RowKind;
import org.apache.flink.util.Collector;
import org.apache.inlong.sort.cdc.dm.table.DMAppendMetadataCollector;
import org.apache.inlong.sort.cdc.dm.table.DMDeserializationSchema;
import org.apache.inlong.sort.cdc.dm.table.DMMetadataConverter;
import org.apache.inlong.sort.cdc.dm.table.DMRecord;

import java.io.UnsupportedEncodingException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.Arrays;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Deserialization schema from DM object to Flink Table/SQL internal data structure {@link
 * RowData}.
 */
@Slf4j
public class RowDataDMDeserializationSchema
        implements
            DMDeserializationSchema<RowData> {

    private static final long serialVersionUID = 1L;

    /** TypeInformation of the produced {@link RowData}. * */
    private final TypeInformation<RowData> resultTypeInfo;

    /**
     * Runtime converter that DM record data into {@link RowData} consisted of physical
     * column values.
     */
    private final DMDeserializationRuntimeConverter physicalConverter;

    /** Whether the deserializer needs to handle metadata columns. */
    private final boolean hasMetadata;

    /**
     * A wrapped output collector which is used to append metadata columns after physical columns.
     */
    private final DMAppendMetadataCollector appendMetadataCollector;

    private static final String TIMESTAMP_PATTERN =
            "(\\d{4}-\\d{2}-\\d{2} \\d{2}:\\d{2}:\\d{2})(?: ([+-]\\d{2}:\\d{2}))?";

    public static Builder newBuilder() {
        return new Builder();
    }

    RowDataDMDeserializationSchema(
            RowType physicalDataType,
            DMMetadataConverter[] metadataConverters,
            TypeInformation<RowData> resultTypeInfo,
            ZoneId serverTimeZone) {
        this.hasMetadata = checkNotNull(metadataConverters).length > 0;
        this.appendMetadataCollector = new DMAppendMetadataCollector(metadataConverters);
        this.physicalConverter = createConverter(checkNotNull(physicalDataType), serverTimeZone);
        this.resultTypeInfo = checkNotNull(resultTypeInfo);
    }

    @Override
    public void deserialize(DMRecord record, Collector<RowData> out) throws Exception {
        log.debug("deserializing record: " + record);
        RowData physicalRow;
        if (record.isSnapshotRecord()) {
            physicalRow = (GenericRowData) physicalConverter.convert(record.getJdbcFields());
            physicalRow.setRowKind(RowKind.INSERT);
            emit(record, physicalRow, out);
        } else {
            switch (record.getOpt()) {
                case INSERT:
                    physicalRow =
                            (GenericRowData) physicalConverter.convert(record.getLogMessageFieldsAfter());
                    physicalRow.setRowKind(RowKind.INSERT);
                    emit(record, physicalRow, out);
                    break;
                case DELETE:
                    physicalRow =
                            (GenericRowData) physicalConverter.convert(record.getLogMessageFieldsBefore());
                    physicalRow.setRowKind(RowKind.DELETE);
                    emit(record, physicalRow, out);
                    break;
                case UPDATE:
                    physicalRow =
                            (GenericRowData) physicalConverter.convert(record.getLogMessageFieldsBefore());
                    physicalRow.setRowKind(RowKind.UPDATE_BEFORE);
                    emit(record, physicalRow, out);
                    physicalRow =
                            (GenericRowData) physicalConverter.convert(record.getLogMessageFieldsAfter());
                    physicalRow.setRowKind(RowKind.UPDATE_AFTER);
                    emit(record, physicalRow, out);
                    break;
                default:
                    throw new IllegalArgumentException(
                            "Unsupported log message record type: " + record.getOpt());
            }
        }
    }

    private void emit(DMRecord row, RowData physicalRow, Collector<RowData> collector) {
        if (!hasMetadata) {
            collector.collect(physicalRow);
            return;
        }

        appendMetadataCollector.inputRecord = row;
        appendMetadataCollector.outputCollector = collector;
        appendMetadataCollector.collect(physicalRow);
    }

    @Override
    public TypeInformation<RowData> getProducedType() {
        return resultTypeInfo;
    }

    /** Builder class of {@link RowDataDMDeserializationSchema}. */
    public static class Builder {

        private RowType physicalRowType;
        private TypeInformation<RowData> resultTypeInfo;
        private DMMetadataConverter[] metadataConverters = new DMMetadataConverter[0];
        private ZoneId serverTimeZone = ZoneId.of("UTC");

        public Builder setPhysicalRowType(
                RowType physicalRowType) {
            this.physicalRowType = physicalRowType;
            return this;
        }

        public Builder setMetadataConverters(
                DMMetadataConverter[] metadataConverters) {
            this.metadataConverters = metadataConverters;
            return this;
        }

        public Builder setResultTypeInfo(
                TypeInformation<RowData> resultTypeInfo) {
            this.resultTypeInfo = resultTypeInfo;
            return this;
        }

        public Builder setServerTimeZone(
                ZoneId serverTimeZone) {
            this.serverTimeZone = serverTimeZone;
            return this;
        }

        public RowDataDMDeserializationSchema build() {
            return new RowDataDMDeserializationSchema(
                    physicalRowType, metadataConverters, resultTypeInfo, serverTimeZone);
        }
    }

    private static DMDeserializationRuntimeConverter createConverter(
            LogicalType type, ZoneId serverTimeZone) {
        return wrapIntoNullableConverter(createNotNullConverter(type, serverTimeZone));
    }

    private static DMDeserializationRuntimeConverter wrapIntoNullableConverter(
            DMDeserializationRuntimeConverter converter) {
        return new DMDeserializationRuntimeConverter() {

            private static final long serialVersionUID = 1L;

            @Override
            public Object convert(Object object) throws Exception {
                if (object == null) {
                    return null;
                }
                return converter.convert(object);
            }
        };
    }

    public static DMDeserializationRuntimeConverter createNotNullConverter(
            LogicalType type, ZoneId serverTimeZone) {
        switch (type.getTypeRoot()) {
            case ROW:
                return createRowConverter((RowType) type, serverTimeZone);
            case NULL:
                return convertToNull();
            case BOOLEAN:
                return convertToBoolean();
            case TINYINT:
                return convertToTinyInt();
            case SMALLINT:
                return convertToSmallInt();
            case INTEGER:
            case INTERVAL_YEAR_MONTH:
                return convertToInt();
            case BIGINT:
            case INTERVAL_DAY_TIME:
                return convertToLong();
            case DATE:
                return convertToDate();
            case TIME_WITHOUT_TIME_ZONE:
                return convertToTime();
            case TIMESTAMP_WITHOUT_TIME_ZONE:
                return convertToTimestamp();
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                return convertToLocalTimeZoneTimestamp(serverTimeZone);
            case FLOAT:
                return convertToFloat();
            case DOUBLE:
                return convertToDouble();
            case CHAR:
            case VARCHAR:
                return convertToString();
            case BINARY:
                return convertToBinary();
            case VARBINARY:
                return convertToBytes();
            case DECIMAL:
                return createDecimalConverter((DecimalType) type);
            case ARRAY:
                return createArrayConverter();
            default:
                throw new UnsupportedOperationException("Unsupported type: " + type);
        }
    }

    private static DMDeserializationRuntimeConverter createRowConverter(
            RowType rowType, ZoneId serverTimeZone) {
        final DMDeserializationRuntimeConverter[] fieldConverters =
                rowType.getFields().stream()
                        .map(RowType.RowField::getType)
                        .map(logicType -> createConverter(logicType, serverTimeZone))
                        .toArray(DMDeserializationRuntimeConverter[]::new);
        final String[] fieldNames = rowType.getFieldNames().toArray(new String[0]);
        return new DMDeserializationRuntimeConverter() {

            private static final long serialVersionUID = 1L;

            @Override
            public Object convert(Object object) {
                int arity = fieldNames.length;
                GenericRowData row = new GenericRowData(arity);
                Map<String, Object> fieldMap = (Map<String, Object>) object;
                for (int i = 0; i < arity; i++) {
                    String fieldName = fieldNames[i];
                    Object value = fieldMap.get(fieldName);
                    try {
                        row.setField(i, fieldConverters[i].convert(value));
                    } catch (Exception e) {
                        throw new RuntimeException(
                                "Failed to convert field '" + fieldName + "' with value: " + value,
                                e);
                    }
                }
                return row;
            }
        };
    }

    private static DMDeserializationRuntimeConverter convertToNull() {
        return new DMDeserializationRuntimeConverter() {

            private static final long serialVersionUID = 1L;

            @Override
            public Object convert(Object object) {
                return null;
            }
        };
    }

    private static DMDeserializationRuntimeConverter convertToBoolean() {
        return new DMDeserializationRuntimeConverter() {

            private static final long serialVersionUID = 1L;

            @Override
            public Object convert(Object object) {
                if (object instanceof byte[]) {
                    return "1".equals(new String((byte[]) object, StandardCharsets.UTF_8));
                }
                return Boolean.parseBoolean(object.toString()) || "1".equals(object.toString());
            }
        };
    }

    private static DMDeserializationRuntimeConverter convertToTinyInt() {
        return new DMDeserializationRuntimeConverter() {

            private static final long serialVersionUID = 1L;

            @Override
            public Object convert(Object object) {
                return Byte.parseByte(object.toString());
            }
        };
    }

    private static DMDeserializationRuntimeConverter convertToSmallInt() {
        return new DMDeserializationRuntimeConverter() {

            private static final long serialVersionUID = 1L;

            @Override
            public Object convert(Object object) {
                return Short.parseShort(object.toString());
            }
        };
    }

    private static DMDeserializationRuntimeConverter convertToInt() {
        return new DMDeserializationRuntimeConverter() {

            private static final long serialVersionUID = 1L;

            @Override
            public Object convert(Object object) {
                if (object instanceof Integer) {
                    return object;
                } else if (object instanceof Long) {
                    return ((Long) object).intValue();
                } else if (object instanceof Date) {
                    return ((Date) object).toLocalDate().getYear();
                } else {
                    return Integer.parseInt(object.toString());
                }
            }
        };
    }

    private static DMDeserializationRuntimeConverter convertToLong() {
        return new DMDeserializationRuntimeConverter() {

            private static final long serialVersionUID = 1L;

            @Override
            public Object convert(Object object) {
                if (object instanceof Integer) {
                    return ((Integer) object).longValue();
                } else if (object instanceof Long) {
                    return object;
                } else {
                    return Long.parseLong(object.toString());
                }
            }
        };
    }

    private static DMDeserializationRuntimeConverter convertToDouble() {
        return new DMDeserializationRuntimeConverter() {

            private static final long serialVersionUID = 1L;

            @Override
            public Object convert(Object object) {
                if (object instanceof Float) {
                    return ((Float) object).doubleValue();
                } else if (object instanceof Double) {
                    return object;
                } else {
                    return Double.parseDouble(object.toString());
                }
            }
        };
    }

    private static DMDeserializationRuntimeConverter convertToFloat() {
        return new DMDeserializationRuntimeConverter() {

            private static final long serialVersionUID = 1L;

            @Override
            public Object convert(Object object) {
                if (object instanceof Float) {
                    return object;
                } else if (object instanceof Double) {
                    return ((Double) object).floatValue();
                } else {
                    return Float.parseFloat(object.toString());
                }
            }
        };
    }

    // for supplemental logs, dm will include the type before the actual value. remove the type.
    private static String cleanTime(String strValue) {
        if (strValue.startsWith("DATE'")) {
            return strValue.substring(5, strValue.length() - 1);
        } else if (strValue.startsWith("TIMESTAMP'")) {
            return strValue.substring(10, strValue.length() - 1);
        } else if (strValue.startsWith("DATETIME'")) {
            return strValue.substring(9, strValue.length() - 1);
        } else if (strValue.startsWith("TIME'")) {
            return strValue.substring(5, strValue.length() - 1);
        } else if (strValue.startsWith("ATE'")) {
            return strValue.substring(4);
        } else if (strValue.startsWith("IMESTAMP'")) {
            return strValue.substring(9);
        } else if (strValue.startsWith("ATETIME'")) {
            return strValue.substring(8);
        } else if (strValue.startsWith("IME'")) {
            return strValue.substring(4);
        } else {
            return strValue;
        }
    }

    private static DMDeserializationRuntimeConverter convertToDate() {
        return new DMDeserializationRuntimeConverter() {

            private static final long serialVersionUID = 1L;

            @Override
            public Object convert(Object object) {
                if (object instanceof String) {
                    object = Date.valueOf(cleanTime((String) object));
                }
                return (int) TemporalConversions.toLocalDate(object).toEpochDay();
            }
        };
    }

    private static DMDeserializationRuntimeConverter convertToTime() {
        return new DMDeserializationRuntimeConverter() {

            private static final long serialVersionUID = 1L;

            @Override
            public Object convert(Object object) {
                if (object instanceof Long) {
                    return (int) ((Long) object / 1000_000);
                }
                if (object instanceof String) {
                    object = Time.valueOf(cleanTime((String) object));
                }
                return TemporalConversions.toLocalTime(object).toSecondOfDay() * 1000;
            }
        };
    }

    private static DMDeserializationRuntimeConverter convertToTimestamp() {
        return new DMDeserializationRuntimeConverter() {

            private static final long serialVersionUID = 1L;

            @Override
            public Object convert(Object object) {
                ZoneId zoneid = ZoneId.systemDefault();
                if (object instanceof String) {
                    // if there is timezone, use that instead of system default timezone.
                    String rawTimeStampString = cleanTime((String) object);
                    Pattern pattern = Pattern.compile(TIMESTAMP_PATTERN);
                    Matcher matcher = pattern.matcher(rawTimeStampString);
                    if (matcher.find()) {
                        String timestamp = matcher.group(1);
                        String timezone = matcher.group(2);
                        if (timezone != null) {
                            zoneid = ZoneOffset.of(timezone).normalized();
                        }
                        object = Timestamp.valueOf(timestamp);
                    }
                }
                LocalDateTime localDateTime =
                        TemporalConversions.toLocalDateTime(object, zoneid);
                return TimestampData.fromLocalDateTime(localDateTime);
            }
        };
    }

    private static DMDeserializationRuntimeConverter convertToLocalTimeZoneTimestamp(
            ZoneId serverTimeZone) {
        return new DMDeserializationRuntimeConverter() {

            private static final long serialVersionUID = 1L;

            @Override
            public Object convert(Object object) {
                if (object instanceof String) {
                    object = Timestamp.valueOf(cleanTime((String) object));
                }
                if (object instanceof Timestamp) {
                    return TimestampData.fromInstant(
                            ((Timestamp) object)
                                    .toLocalDateTime()
                                    .atZone(serverTimeZone)
                                    .toInstant());
                }
                if (object instanceof LocalDateTime) {
                    return TimestampData.fromInstant(
                            ((LocalDateTime) object).atZone(serverTimeZone).toInstant());
                }
                throw new IllegalArgumentException(
                        "Unable to convert to TimestampData from unexpected value '"
                                + object
                                + "' of type "
                                + object.getClass().getName());
            }
        };
    }

    private static DMDeserializationRuntimeConverter convertToString() {
        return new DMDeserializationRuntimeConverter() {

            private static final long serialVersionUID = 1L;

            @Override
            public Object convert(Object object) {
                // support deserializing clob objects
                if (object instanceof DmdbClob) {
                    return StringData
                            .fromString(((DmdbClob) object).getSubString(1L, (int) ((DmdbClob) object).length));
                }

                String data = object.toString();
                if (data.startsWith("'") && data.endsWith("'")) {
                    data = data.substring(1, data.length() - 1);
                }
                return StringData.fromString(data);
            }
        };
    }

    private static DMDeserializationRuntimeConverter convertToBinary() {
        return new DMDeserializationRuntimeConverter() {

            private static final long serialVersionUID = 1L;

            @Override
            public Object convert(Object object) {
                if (object instanceof String) {
                    try {
                        long v = Long.parseLong((String) object);
                        byte[] bytes = ByteBuffer.allocate(8).putLong(v).array();
                        int i = 0;
                        while (i < Long.BYTES - 1 && bytes[i] == 0) {
                            i++;
                        }
                        return Arrays.copyOfRange(bytes, i, Long.BYTES);
                    } catch (NumberFormatException e) {
                        return ((String) object).getBytes(StandardCharsets.UTF_8);
                    }
                } else if (object instanceof byte[]) {
                    String str = new String((byte[]) object, StandardCharsets.US_ASCII);
                    return str.getBytes(StandardCharsets.UTF_8);
                } else if (object instanceof ByteBuffer) {
                    ByteBuffer byteBuffer = (ByteBuffer) object;
                    byte[] bytes = new byte[byteBuffer.remaining()];
                    byteBuffer.get(bytes);
                    return bytes;
                } else {
                    throw new UnsupportedOperationException(
                            "Unsupported BINARY value type: " + object.getClass().getSimpleName());
                }
            }
        };
    }

    private static DMDeserializationRuntimeConverter convertToBytes() {
        return new DMDeserializationRuntimeConverter() {

            private static final long serialVersionUID = 1L;

            @Override
            public Object convert(Object object) {
                if (object instanceof String) {
                    return ((String) object).getBytes(StandardCharsets.UTF_8);
                } else if (object instanceof byte[]) {
                    return object;
                } else if (object instanceof ByteBuffer) {
                    ByteBuffer byteBuffer = (ByteBuffer) object;
                    byte[] bytes = new byte[byteBuffer.remaining()];
                    byteBuffer.get(bytes);
                    return bytes;
                } else {
                    throw new UnsupportedOperationException(
                            "Unsupported BYTES value type: " + object.getClass().getSimpleName());
                }
            }
        };
    }

    private static DMDeserializationRuntimeConverter createDecimalConverter(
            DecimalType decimalType) {
        final int precision = decimalType.getPrecision();
        final int scale = decimalType.getScale();

        return new DMDeserializationRuntimeConverter() {

            private static final long serialVersionUID = 1L;

            @Override
            public Object convert(Object object) {
                BigDecimal bigDecimal;
                if (object instanceof String) {
                    bigDecimal = new BigDecimal((String) object);
                } else if (object instanceof Long) {
                    bigDecimal = new BigDecimal((Long) object);
                } else if (object instanceof BigInteger) {
                    bigDecimal = new BigDecimal((BigInteger) object);
                } else if (object instanceof Double) {
                    bigDecimal = BigDecimal.valueOf((Double) object);
                } else if (object instanceof BigDecimal) {
                    bigDecimal = (BigDecimal) object;
                } else {
                    throw new IllegalArgumentException(
                            "Unable to convert to decimal from unexpected value '"
                                    + object
                                    + "' of type "
                                    + object.getClass());
                }
                return DecimalData.fromBigDecimal(bigDecimal, precision, scale);
            }
        };
    }

    private static DMDeserializationRuntimeConverter createArrayConverter() {
        return new DMDeserializationRuntimeConverter() {

            private static final long serialVersionUID = 1L;

            @Override
            public Object convert(Object object) throws UnsupportedEncodingException {
                String s;
                if (object instanceof ByteString) {
                    s = ((ByteString) object).toString(StandardCharsets.UTF_8.name());
                } else {
                    s = object.toString();
                }
                String[] strArray = s.split(",");
                StringData[] stringDataArray = new StringData[strArray.length];
                for (int i = 0; i < strArray.length; i++) {
                    stringDataArray[i] = StringData.fromString(strArray[i]);
                }
                return new GenericArrayData(stringDataArray);
            }
        };
    }
}
