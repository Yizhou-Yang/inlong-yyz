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

package org.apache.inlong.sort.base.format;

import com.google.gson.Gson;
import org.apache.commons.lang3.StringUtils;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.formats.common.TimestampFormat;
import org.apache.flink.shaded.guava18.com.google.common.collect.ImmutableMap;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.type.TypeReference;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.BinaryType;
import org.apache.flink.table.types.logical.BooleanType;
import org.apache.flink.table.types.logical.CharType;
import org.apache.flink.table.types.logical.DateType;
import org.apache.flink.table.types.logical.DecimalType;
import org.apache.flink.table.types.logical.DoubleType;
import org.apache.flink.table.types.logical.FloatType;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.LocalZonedTimestampType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.RowType.RowField;
import org.apache.flink.table.types.logical.SmallIntType;
import org.apache.flink.table.types.logical.TimeType;
import org.apache.flink.table.types.logical.TimestampType;
import org.apache.flink.table.types.logical.TinyIntType;
import org.apache.flink.table.types.logical.VarBinaryType;
import org.apache.flink.table.types.logical.VarCharType;
import org.apache.flink.types.RowKind;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Map.Entry;
import java.util.function.BiFunction;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import static org.apache.inlong.sort.base.Constants.SINK_MULTIPLE_TYPE_MAP_COMPATIBLE_WITH_SPARK;

/**
 * Json dynamic format class
 * This class main handle:
 * 1. deserialize data from byte array
 * 2. parse pattern and get the real value from the raw data(contains meta data and physical data)
 * Such as:
 * 1). give a pattern "${a}{b}{c}" and the root Node contains the keys(a: '1', b: '2', c: '3')
 * the result of pared will be '123'
 * 2). give a pattern "${a}_{b}_{c}" and the root Node contains the keys(a: '1', b: '2', c: '3')
 * the result of pared will be '1_2_3'
 * 3). give a pattern "prefix_${a}_{b}_{c}_suffix" and the root Node contains the keys(a: '1', b: '2', c: '3')
 * the result of pared will be 'prefix_1_2_3_suffix'
 */
@SuppressWarnings("LanguageDetectionInspection")
public abstract class JsonDynamicSchemaFormat extends AbstractDynamicSchemaFormat<JsonNode> {

    private static final Logger LOGGER = LoggerFactory.getLogger(JsonDynamicSchemaFormat.class);

    public static final int DEFAULT_DECIMAL_PRECISION = 38;
    public static final int DEFAULT_DECIMAL_SCALE = 5;
    public static final int DEFAULT_CHAR_LENGTH = 255;
    public static final int DEFAULT_CHAR_TIME_LENGTH = 30;
    /**
     * dialect sql type pattern such as DECIMAL(38, 10) from mysql or oracle etc
     */
    private static final Pattern DIALECT_SQL_TYPE_PATTERN = Pattern.compile("([\\w, \\s]+)\\(([\\d,\\s'\\-]*)\\)");

    /**
     * The first item of array
     */
    private static final Integer FIRST = 0;

    private static final Integer ORACLE_TIMESTAMP_TIME_ZONE = -101;
    private static final Integer SQLSERVER_DATETIMEOFFSET = -155;

    private static final Map<Integer, LogicalType> SQL_TYPE_2_FLINK_TYPE_MAPPING =
            ImmutableMap.<Integer, LogicalType>builder()
                    .put(java.sql.Types.CHAR, new CharType(DEFAULT_CHAR_LENGTH))
                    .put(java.sql.Types.VARCHAR, new VarCharType(VarCharType.MAX_LENGTH))
                    .put(java.sql.Types.SMALLINT, new SmallIntType())
                    .put(java.sql.Types.INTEGER, new IntType())
                    .put(java.sql.Types.BIGINT, new BigIntType())
                    .put(java.sql.Types.REAL, new FloatType())
                    .put(java.sql.Types.DOUBLE, new DoubleType())
                    .put(java.sql.Types.FLOAT, new FloatType())
                    .put(java.sql.Types.DECIMAL, new DecimalType(DEFAULT_DECIMAL_PRECISION, DEFAULT_DECIMAL_SCALE))
                    .put(java.sql.Types.NUMERIC, new DecimalType(DEFAULT_DECIMAL_PRECISION, DEFAULT_DECIMAL_SCALE))
                    .put(java.sql.Types.BIT, new BooleanType())
                    .put(java.sql.Types.TIME, new TimeType())
                    .put(java.sql.Types.TIME_WITH_TIMEZONE, new TimeType())
                    .put(java.sql.Types.TIMESTAMP_WITH_TIMEZONE, new LocalZonedTimestampType())
                    .put(ORACLE_TIMESTAMP_TIME_ZONE, new LocalZonedTimestampType())
                    .put(SQLSERVER_DATETIMEOFFSET, new LocalZonedTimestampType())
                    .put(java.sql.Types.TIMESTAMP, new TimestampType())
                    .put(java.sql.Types.BINARY, new BinaryType(BinaryType.MAX_LENGTH))
                    .put(java.sql.Types.VARBINARY, new VarBinaryType(VarBinaryType.MAX_LENGTH))
                    .put(java.sql.Types.BLOB, new VarBinaryType(VarBinaryType.MAX_LENGTH))
                    .put(java.sql.Types.CLOB, new VarBinaryType(VarBinaryType.MAX_LENGTH))
                    .put(java.sql.Types.DATE, new DateType())
                    .put(java.sql.Types.BOOLEAN, new BooleanType())
                    .put(java.sql.Types.LONGNVARCHAR, new VarCharType(VarCharType.MAX_LENGTH))
                    .put(java.sql.Types.LONGVARBINARY, new VarCharType(VarCharType.MAX_LENGTH))
                    .put(java.sql.Types.LONGVARCHAR, new VarCharType(VarCharType.MAX_LENGTH))
                    .put(java.sql.Types.ARRAY, new VarCharType(VarCharType.MAX_LENGTH))
                    .put(java.sql.Types.NCHAR, new CharType(DEFAULT_CHAR_LENGTH))
                    .put(java.sql.Types.NCLOB, new VarBinaryType(VarBinaryType.MAX_LENGTH))
                    .put(java.sql.Types.TINYINT, new TinyIntType())
                    .put(java.sql.Types.OTHER, new VarCharType(VarCharType.MAX_LENGTH))
                    .put(Types.NVARCHAR, new VarCharType(VarCharType.MAX_LENGTH))
                    .put(Types.SQLXML, new VarCharType(VarCharType.MAX_LENGTH))
                    .put(Types.ROWID, new VarBinaryType(VarBinaryType.MAX_LENGTH))
                    .build();

    private static final Map<Integer, LogicalType> SQL_TYPE_2_SPARK_SUPPORTED_FLINK_TYPE_MAPPING =
            ImmutableMap.<Integer, LogicalType>builder()
                    .put(java.sql.Types.CHAR, new CharType(DEFAULT_CHAR_LENGTH))
                    .put(java.sql.Types.VARCHAR, new VarCharType(VarCharType.MAX_LENGTH))
                    .put(java.sql.Types.SMALLINT, new SmallIntType())
                    .put(java.sql.Types.INTEGER, new IntType())
                    .put(java.sql.Types.BIGINT, new BigIntType())
                    .put(java.sql.Types.REAL, new FloatType())
                    .put(java.sql.Types.DOUBLE, new DoubleType())
                    .put(java.sql.Types.FLOAT, new FloatType())
                    .put(java.sql.Types.DECIMAL, new DecimalType(DEFAULT_DECIMAL_PRECISION, DEFAULT_DECIMAL_SCALE))
                    .put(java.sql.Types.NUMERIC, new DecimalType(DEFAULT_DECIMAL_PRECISION, DEFAULT_DECIMAL_SCALE))
                    .put(java.sql.Types.BIT, new BooleanType())
                    .put(java.sql.Types.TIME, new VarCharType(DEFAULT_CHAR_TIME_LENGTH))
                    .put(java.sql.Types.TIME_WITH_TIMEZONE, new VarCharType(DEFAULT_CHAR_TIME_LENGTH))
                    .put(java.sql.Types.TIMESTAMP_WITH_TIMEZONE, new LocalZonedTimestampType())
                    .put(ORACLE_TIMESTAMP_TIME_ZONE, new LocalZonedTimestampType())
                    .put(SQLSERVER_DATETIMEOFFSET, new LocalZonedTimestampType())
                    .put(java.sql.Types.TIMESTAMP, new LocalZonedTimestampType())
                    .put(java.sql.Types.BINARY, new BinaryType(BinaryType.MAX_LENGTH))
                    .put(java.sql.Types.VARBINARY, new VarBinaryType(VarBinaryType.MAX_LENGTH))
                    .put(java.sql.Types.BLOB, new VarBinaryType(VarBinaryType.MAX_LENGTH))
                    .put(java.sql.Types.CLOB, new VarBinaryType(VarBinaryType.MAX_LENGTH))
                    .put(java.sql.Types.DATE, new DateType())
                    .put(java.sql.Types.BOOLEAN, new BooleanType())
                    .put(java.sql.Types.LONGNVARCHAR, new VarCharType(VarCharType.MAX_LENGTH))
                    .put(java.sql.Types.LONGVARBINARY, new VarCharType(VarCharType.MAX_LENGTH))
                    .put(java.sql.Types.LONGVARCHAR, new VarCharType(VarCharType.MAX_LENGTH))
                    .put(java.sql.Types.ARRAY, new VarCharType(VarCharType.MAX_LENGTH))
                    .put(java.sql.Types.NCHAR, new CharType(DEFAULT_CHAR_LENGTH))
                    .put(java.sql.Types.NCLOB, new VarBinaryType(VarBinaryType.MAX_LENGTH))
                    .put(java.sql.Types.TINYINT, new TinyIntType())
                    .put(java.sql.Types.OTHER, new VarCharType(VarCharType.MAX_LENGTH))
                    .put(Types.NVARCHAR, new VarCharType(VarCharType.MAX_LENGTH))
                    .put(Types.SQLXML, new VarCharType(VarCharType.MAX_LENGTH))
                    .put(Types.ROWID, new VarBinaryType(VarBinaryType.MAX_LENGTH))
                    .build();

    public static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    protected final JsonToRowDataConverters rowDataConverters;
    protected final boolean adaptSparkEngine;

    public JsonDynamicSchemaFormat(Map<String, String> properties) {
        ReadableConfig config = Configuration.fromMap(properties);
        this.adaptSparkEngine = config.get(SINK_MULTIPLE_TYPE_MAP_COMPATIBLE_WITH_SPARK);
        this.rowDataConverters =
                new JsonToRowDataConverters(
                        false,
                        false,
                        TimestampFormat.ISO_8601,
                        adaptSparkEngine);
    }

    private BiFunction<LogicalType, String, LogicalType> handleDialectSqlTypeFunction;

    private final Map<Tuple2<LogicalType, String>, LogicalType> dialectType2LogicalType =
            new HashMap<>();

    /**
     * Extract values by keys from the raw data
     *
     * @param root The raw data
     * @param keys The key list that will be used to extract
     * @return The value list maps the keys
     */
    @Override
    public List<String> extractValues(JsonNode root, String... keys) {
        if (keys == null || keys.length == 0) {
            return new ArrayList<>();
        }
        JsonNode physicalNode = getPhysicalData(root);
        if (physicalNode.isArray()) {
            // Extract from the first value when the physicalNode is array
            physicalNode = physicalNode.get(FIRST);
        }
        List<String> values = new ArrayList<>(keys.length);
        if (physicalNode == null) {
            for (String key : keys) {
                values.add(extract(root, key));
            }
            return values;
        }
        for (String key : keys) {
            String value = extract(physicalNode, key);
            if (value == null) {
                value = extract(root, key);
            }
            values.add(value);
        }
        return values;
    }

    /**
     * Extract value by key from ${@link JsonNode}
     *
     * @param jsonNode The json node
     * @param key The key that will be used to extract
     * @return The value maps the key in the json node
     */
    @Override
    public String extract(JsonNode jsonNode, String key) {
        if (jsonNode == null || key == null) {
            return null;
        }
        JsonNode value = jsonNode.get(key);
        if (value != null) {
            return value.asText();
        }
        int index = key.indexOf(".");
        if (index > 0 && index + 1 < key.length()) {
            return extract(jsonNode.get(key.substring(0, index)), key.substring(index + 1));
        }
        return null;
    }

    /**
     * Deserialize from byte array and return a ${@link JsonNode}
     *
     * @param message The byte array of raw data
     * @return The JsonNode
     * @throws IOException The exceptions may throws when deserialize
     */
    @Override
    public JsonNode deserialize(byte[] message) throws IOException {
        return OBJECT_MAPPER.readTree(message);
    }

    /**
     * Parse msg and replace the value by key from meta data and physical.
     * See details {@link JsonDynamicSchemaFormat#parse(JsonNode, String)}
     *
     * @param message The source of data rows format by bytes
     * @param pattern The pattern value
     * @return The result of parsed
     * @throws IOException The exception will throws
     */
    @Override
    public String parse(byte[] message, String pattern) throws IOException {
        return parse(deserialize(message), pattern);
    }

    /**
     * Parse msg and replace the value by key from meta data and physical.
     * Such as:
     * 1. give a pattern "${a}{b}{c}" and the root Node contains the keys(a: '1', b: '2', c: '3')
     * the result of pared will be '123'
     * 2. give a pattern "${a}_{b}_{c}" and the root Node contains the keys(a: '1', b: '2', c: '3')
     * the result of pared will be '1_2_3'
     * 3. give a pattern "prefix_${a}_{b}_{c}_suffix" and the root Node contains the keys(a: '1', b: '2', c: '3')
     * the result of pared will be 'prefix_1_2_3_suffix'
     *
     * @param rootNode The root node of json
     * @param pattern The pattern value
     * @return The result of parsed
     * @throws IOException The exception will throws
     */
    @Override
    public String parse(JsonNode rootNode, String pattern) throws IOException {
        Matcher matcher = PATTERN.matcher(pattern);
        StringBuffer sb = new StringBuffer();
        JsonNode physicalNode = getPhysicalData(rootNode);
        if (physicalNode.isArray()) {
            // Extract from the first value when the physicalNode is array
            physicalNode = physicalNode.get(FIRST);
        }
        while (matcher.find()) {
            String keyText = matcher.group(1);
            String replacement = extract(physicalNode, keyText);
            if (replacement == null) {
                replacement = extract(rootNode, keyText);
            }
            if (replacement == null) {
                // The variable replacement here is mainly used for
                // multi-sink scenario synchronization destination positioning, so the value of null cannot be ignored.
                throw new IOException(String.format("Can't find value for key: %s", keyText));
            }
            matcher.appendReplacement(sb, replacement);
        }
        matcher.appendTail(sb);
        return sb.toString();
    }

    /**
     * Get physical data from the root json node
     *
     * @param root The json root node
     * @return The physical data node
     */
    public JsonNode getPhysicalData(JsonNode root) {
        JsonNode physicalData = getUpdateAfter(root);
        if (physicalData == null) {
            physicalData = getUpdateBefore(root);
        }
        return physicalData;
    }

    /**
     * Get physical data of update after
     *
     * @param root The json root node
     * @return The physical data node of update after
     */
    public abstract JsonNode getUpdateAfter(JsonNode root);

    /**
     * Get physical data of update before
     *
     * @param root The json root node
     * @return The physical data node of update before
     */
    public abstract JsonNode getUpdateBefore(JsonNode root);

    /**
     * Convert opType to RowKind
     *
     * @param opType The opTyoe of data
     * @return The RowKind of data
     */
    public abstract List<RowKind> opType2RowKind(String opType);

    /**
     * Get opType of data
     *
     * @param root The json root node
     * @return The opType of data
     */
    public abstract String getOpType(JsonNode root);

    protected RowType extractSchemaNode(JsonNode schema, JsonNode dialectSchema, List<String> pkNames) {
        Iterator<Entry<String, JsonNode>> schemaFields = schema.fields();
        if (LOGGER.isDebugEnabled()) {
            Gson gson = new Gson();
            LOGGER.debug("raw record: {}", gson.toJson(schema));
        }
        List<RowField> fields = new ArrayList<>();
        while (schemaFields.hasNext()) {
            Entry<String, JsonNode> entry = schemaFields.next();
            String name = entry.getKey();
            LogicalType type = sqlType2FlinkType(entry.getValue().asInt());
            String dialectType = null;
            if (dialectSchema != null && dialectSchema.get(name) != null) {
                dialectType = dialectSchema.get(name).asText();
            }
            try {
                if (getHandleDialectSqlTypeFunction() == null) {
                    type = handleDialectSqlType(type, dialectType);
                } else {
                    type = getHandleDialectSqlTypeFunction().apply(type, dialectType);
                }
            } catch (Exception e) {
                throw new RuntimeException(
                        String.format("Handle dialect sql type failed, the field: %s, the dialect type: %s",
                                name, dialectType),
                        e);
            }
            if (pkNames.contains(name)) {
                type = type.copy(false);
            }
            fields.add(new RowField(name, type));
        }
        return new RowType(fields);
    }

    /**
     * set precision and scale for decimal and other types
     *
     * @param type
     * @param dialectType
     * @return
     */
    public LogicalType handleDialectSqlType(LogicalType type, String dialectType) {
        if (StringUtils.isBlank(dialectType)) {
            return type;
        }
        final Tuple2<LogicalType, String> key = Tuple2.of(type, dialectType);
        LogicalType resType = dialectType2LogicalType.get(key);
        if (resType != null) {
            return resType;
        }
        resType = handleDialectSqlTypeInternal(type, dialectType);
        dialectType2LogicalType.put(key, resType);
        return resType;
    }
    private LogicalType handleDialectSqlTypeInternal(LogicalType type, String dialectType) {
        Matcher matcher = DIALECT_SQL_TYPE_PATTERN.matcher(dialectType);
        if (!matcher.matches()) {
            return type;
        }
        String[] items = matcher.group(2).split(",");
        if (type instanceof DecimalType) {
            int precision;
            int scale = DEFAULT_DECIMAL_SCALE;
            precision = Integer.parseInt(items[0].trim());
            if (precision < DecimalType.MIN_PRECISION || precision > DecimalType.MAX_PRECISION) {
                precision = DEFAULT_DECIMAL_PRECISION;
            }
            if (items.length == 2) {
                scale = Integer.parseInt(items[1].trim());
                if (scale < DecimalType.MIN_SCALE || scale > precision) {
                    scale = DEFAULT_DECIMAL_SCALE;
                }
            }
            try {
                return new DecimalType(precision, scale);
            } catch (ValidationException e) {
                LOGGER.warn("The precision or scale is unvalid and it will be converted to STRING", e);
                return new VarCharType(VarCharType.MAX_LENGTH);
            }
        } else if (type instanceof CharType) {
            int length = Integer.parseInt(items[0].trim());
            if (length <= 0) {
                length = DEFAULT_CHAR_LENGTH;
            }
            return new CharType(length);
        } else if (type instanceof VarCharType) {
            int length = Integer.parseInt(items[0].trim());
            if (length <= 0) {
                length = Integer.MAX_VALUE;
            }
            return new VarCharType(length);
        } else if (type instanceof VarBinaryType) {
            int length = Integer.parseInt(items[0].trim());
            if (length <= 0) {
                length = Integer.MAX_VALUE;
            }
            return new VarBinaryType(length);
        } else if (type instanceof BinaryType) {
            int length = Integer.parseInt(items[0].trim());
            if (length <= 0) {
                length = 1;
            }
            return new BinaryType(length);
        } else if (matcher.group(0).equalsIgnoreCase("TINYINT(1)")) {
            return new TinyIntType();
        } else {
            return numberTypeScopeExpand(type, matcher.group(1));
        }
    }

    private LogicalType numberTypeScopeExpand(LogicalType originType, String typeOfString) {
        String type = typeOfString.toUpperCase(Locale.ROOT);
        if (type.startsWith("BIGINT UNSIGNED") || type.startsWith("UNSIGNED BIGINT")) {
            return new DecimalType(20, 0);
        }
        if (type.startsWith("UNSIGNED TINYINT") || type.startsWith("TINYINT UNSIGNED")) {
            return new SmallIntType();
        } else if (type.startsWith("UNSIGNED MEDIUMINT") || type.startsWith("MEDIUMINT UNSIGNED")
                || type.startsWith("UNSIGNED SMALLINT") || type.startsWith("SMALLINT UNSIGNED")) {
            return new IntType();
        } else if (type.startsWith("UNSIGNED INT") || type.startsWith("INT UNSIGNED")) {
            return new BigIntType();
        }
        return originType;
    }

    public BiFunction<LogicalType, String, LogicalType> getHandleDialectSqlTypeFunction() {
        return handleDialectSqlTypeFunction;
    }

    public void setHandleDialectSqlTypeFunction(
            BiFunction<LogicalType, String, LogicalType> handleDialectSqlTypeFunction) {
        this.handleDialectSqlTypeFunction = handleDialectSqlTypeFunction;
    }

    public LogicalType sqlType2FlinkType(int jdbcType) {
        Map<Integer, LogicalType> typeMap = adaptSparkEngine
                ? SQL_TYPE_2_SPARK_SUPPORTED_FLINK_TYPE_MAPPING
                : SQL_TYPE_2_FLINK_TYPE_MAPPING;
        if (typeMap.containsKey(jdbcType)) {
            return typeMap.get(jdbcType);
        } else {
            return new VarCharType(VarCharType.MAX_LENGTH);
            // throw new IllegalArgumentException("Unsupported jdbcType: " + jdbcType);
        }
    }

    /**
     * Convert json node to map
     *
     * @param data The json node
     * @return The List of json node
     * @throws IOException The exception may be thrown when executing
     */
    public List<Map<String, String>> jsonNode2Map(JsonNode data) throws IOException {
        if (data == null) {
            return new ArrayList<>();
        }
        List<Map<String, String>> values = new ArrayList<>();
        if (data.isArray()) {
            for (int i = 0; i < data.size(); i++) {
                values.add(OBJECT_MAPPER.convertValue(data.get(i), new TypeReference<Map<String, String>>() {
                }));
            }
        } else {
            values.add(OBJECT_MAPPER.convertValue(data, new TypeReference<Map<String, String>>() {
            }));
        }
        return values;
    }
}
