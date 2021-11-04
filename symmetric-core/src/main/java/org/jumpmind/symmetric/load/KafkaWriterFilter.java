/**
 * Licensed to JumpMind Inc under one or more contributor
 * license agreements.  See the NOTICE file distributed
 * with this work for additional information regarding
 * copyright ownership.  JumpMind Inc licenses this file
 * to you under the GNU General Public License, version 3.0 (GPLv3)
 * (the "License"); you may not use this file except in compliance
 * with the License.
 *
 * You should have received a copy of the GNU General Public License,
 * version 3.0 (GPLv3) along with this library; if not, see
 * <http://www.gnu.org/licenses/>.
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.jumpmind.symmetric.load;

import java.beans.PropertyDescriptor;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.Encoder;
import org.apache.avro.io.EncoderFactory;
import org.apache.commons.beanutils.BeanUtils;
import org.apache.commons.beanutils.PropertyUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.time.DateUtils;
import org.apache.commons.text.StringEscapeUtils;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.jumpmind.db.model.Table;
import org.jumpmind.properties.TypedProperties;
import org.jumpmind.symmetric.common.ParameterConstants;
import org.jumpmind.symmetric.io.data.CsvData;
import org.jumpmind.symmetric.io.data.DataContext;
import org.jumpmind.symmetric.io.data.DataEventType;
import org.jumpmind.symmetric.io.data.writer.IDatabaseWriterFilter;
import org.jumpmind.symmetric.service.IParameterService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.core.io.Resource;
import org.springframework.core.io.support.PathMatchingResourcePatternResolver;
import org.springframework.core.io.support.ResourcePatternResolver;
import org.springframework.core.type.classreading.CachingMetadataReaderFactory;
import org.springframework.core.type.classreading.MetadataReader;
import org.springframework.core.type.classreading.MetadataReaderFactory;
import org.springframework.util.ClassUtils;
import org.springframework.util.SystemPropertyUtils;

import com.google.gson.Gson;

import io.confluent.kafka.serializers.KafkaAvroSerializer;
import io.confluent.kafka.serializers.KafkaAvroSerializerConfig;

public class KafkaWriterFilter implements IDatabaseWriterFilter {
    protected final String KAFKA_TEXT_CACHE = "KAFKA_TEXT_CACHE" + this.hashCode();

    protected Map<String, List<ProducerRecord<String, Object>>> kafkaDataMap = new HashMap<String, List<ProducerRecord<String, Object>>>();
    protected String kafkaDataKey;

    private final Logger log = LoggerFactory.getLogger(getClass());

    private String url;

    private String producer;

    private String outputFormat;

    private String topicBy;

    private String messageBy;

    private String confluentUrl;

    private String schemaPackage;

    private String[] parseDatePatterns = new String[] {
            "yyyy/MM/dd HH:mm:ss.SSSSSS",
            "yyyy-MM-dd HH:mm:ss",
            "ddMMMyyyy:HH:mm:ss.SSS Z",
            "ddMMMyyyy:HH:mm:ss.SSS",
            "yyyy-MM-dd HH:mm:ss.SSS",
            "ddMMMyyyy:HH:mm:ss.SSSSSS",
            "yyyy-MM-dd",
            "yyyy-MM-dd'T'HH:mmZZZZ",
            "yyyy-MM-dd'T'HH:mm:ssZZZZ",
            "yyyy-MM-dd'T'HH:mm:ss.SSSZZZZ"
    };
    
    private List<String> schemaPackageClassNames = new ArrayList<String>();

    public final static String KAFKA_FORMAT_XML = "XML";
    public final static String KAFKA_FORMAT_JSON = "JSON";
    public final static String KAFKA_FORMAT_AVRO = "AVRO";
    public final static String KAFKA_FORMAT_CSV = "CSV";

    public final static String KAFKA_MESSAGE_BY_BATCH = "BATCH";
    public final static String KAFKA_MESSAGE_BY_ROW = "ROW";

    public final static String KAFKA_TOPIC_BY_TABLE = "TABLE";
    public final static String KAFKA_TOPIC_BY_CHANNEL = "CHANNEL";

    public final static String AVRO_CDC_SCHEMA = "{" + "\"type\":\"record\"," + "\"name\":\"cdc\"," + "\"fields\":["
            + "  { \"name\":\"table\", \"type\":\"string\" }," + "  { \"name\":\"eventType\", \"type\":\"string\" },"
            + "  { \"name\":\"data\", \"type\":{" + "     \"type\":\"array\", \"items\":{" + "         \"name\":\"column\","
            + "         \"type\":\"record\"," + "         \"fields\":[" + "            {\"name\":\"name\", \"type\":\"string\"},"
            + "            {\"name\":\"value\", \"type\":[\"null\", \"string\"]} ] }}}]}";

    Schema.Parser parser = new Schema.Parser();
    Schema schema = null;
    Map<String, Object> configs = new HashMap<String, Object>();

    Map<String, Class<?>> tableClassCache = new HashMap<String, Class<?>>();
    Map<String, String> tableNameCache = new HashMap<String, String>();
    Map<String, Map<String, String>> tableColumnCache = new HashMap<String, Map<String, String>>();

    public static KafkaProducer<String, Object> kafkaProducer;

    public KafkaWriterFilter(IParameterService parameterService) {
        schema = parser.parse(AVRO_CDC_SCHEMA);
        this.url = parameterService.getString(ParameterConstants.LOAD_ONLY_PROPERTY_PREFIX + "db.url");
        if (url == null) {
            throw new RuntimeException(
                    "Kakfa not configured properly, verify you have set the endpoint to kafka with the following property : "
                            + ParameterConstants.LOAD_ONLY_PROPERTY_PREFIX + "db.url");
        }

        this.producer = parameterService.getString(ParameterConstants.KAFKA_PRODUCER, "SymmetricDS");
        this.outputFormat = parameterService.getString(ParameterConstants.KAFKA_FORMAT, KAFKA_FORMAT_JSON);
        this.topicBy = parameterService.getString(ParameterConstants.KAFKA_TOPIC_BY, KAFKA_TOPIC_BY_CHANNEL);
        this.messageBy = parameterService.getString(ParameterConstants.KAFKA_MESSAGE_BY, KAFKA_MESSAGE_BY_BATCH);
        this.confluentUrl = parameterService.getString(ParameterConstants.KAFKA_CONFLUENT_REGISTRY_URL);
        this.schemaPackage = parameterService.getString(ParameterConstants.KAFKA_AVRO_JAVA_PACKAGE);

        if (kafkaProducer == null) {
	        configs.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, this.url);
	        configs.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
	        configs.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
	        configs.put(ProducerConfig.CLIENT_ID_CONFIG, this.producer);
	
	        if (confluentUrl != null) {
	            configs.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class.getName());
	            configs.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
	
	            configs.put(KafkaAvroSerializerConfig.SCHEMA_REGISTRY_URL_CONFIG, confluentUrl);
	        }
	        
	        TypedProperties props = parameterService.getAllParameters();
	        for (Object key : props.keySet()) {
	            if (key.toString().startsWith("kafkaclient.")) {
	                configs.put(key.toString().substring(12), props.get(key));
	            }
	        }
	        kafkaProducer = new KafkaProducer<String, Object>(configs); 
	        this.log.debug("Kafka client config: {}", configs);
        } 
    }
    
    public boolean isValidEventType(DataEventType type) {
    	return type != null && (type == DataEventType.INSERT || type == DataEventType.DELETE || type == DataEventType.UPDATE);
    }

    public boolean beforeWrite(DataContext context, Table table, CsvData data) {
        
        if (table.getNameLowerCase().startsWith("sym_") || !isValidEventType(data.getDataEventType())) {
            return true;
        } else {
            String[] rowData = data.getParsedData(CsvData.ROW_DATA);
            if (data.getDataEventType() == DataEventType.DELETE) {
                rowData = data.getParsedData(CsvData.OLD_DATA);
            }

            StringBuffer kafkaText = new StringBuffer();
            String kafkaKey = null;

            if (messageBy.equals(KAFKA_MESSAGE_BY_ROW)) {
                StringBuffer sb = new StringBuffer();
                sb.append(table.getName()).append(":");
                for (int i = 0; i < table.getPrimaryKeyColumnNames().length; i++) {
                    sb.append(":").append(rowData[i]);
                }
                kafkaKey = String.valueOf(sb.toString().hashCode());
            } else if (messageBy.equals(KAFKA_MESSAGE_BY_BATCH)) {
                String s = context.getBatch().getSourceNodeId() + "-" + context.getBatch().getBatchId();
                kafkaKey = String.valueOf(s.hashCode());
            }

            if (topicBy.equals(KAFKA_TOPIC_BY_CHANNEL)) {
                kafkaDataKey = context.getBatch().getChannelId();
            } else {
                kafkaDataKey = table.getNameLowerCase();
            }

            log.debug("Processing table {} for Kafka on topic {}", table, kafkaDataKey);

            if (kafkaDataMap.get(kafkaDataKey) == null) {
                kafkaDataMap.put(kafkaDataKey, new ArrayList<ProducerRecord<String, Object>>());
            }
            List<ProducerRecord<String, Object>> kafkaDataList = kafkaDataMap.get(kafkaDataKey);

            if (outputFormat.equals(KAFKA_FORMAT_JSON)) {
                kafkaText.append("{\"").append(table.getName()).append("\": {").append("\"eventType\": \"" + data.getDataEventType() + "\",")
                        .append("\"data\": { ");
                // Let Gson escape the json values
                Gson gson = new Gson();
                for (int i = 0; i < table.getColumnNames().length; i++) {
                    kafkaText.append("\"").append(table.getColumnNames()[i]).append("\": ");

                    kafkaText.append(gson.toJson(rowData[i]));
                    if (i + 1 < table.getColumnNames().length) {
                        kafkaText.append(",");
                    }
                }
                kafkaText.append(" } } }");
            } else if (outputFormat.equals(KAFKA_FORMAT_CSV)) {
                // Quote every non-null field, escape quote character by doubling the quote character
                kafkaText.append("\n\"TABLE\"").append(",\"").append(table.getName()).append("\",\"").append("EVENT")
                    .append("\",\"").append(data.getDataEventType()).append("\",");

                for (int i = 0; i < table.getColumnNames().length; i++) {
                    kafkaText.append("\"")
                        .append(StringUtils.replace(table.getColumnNames()[i],"\"","\"\""))
                        .append("\",");
                    if (rowData[i] != null) {
                        kafkaText.append("\"").append(StringUtils.replace(rowData[i],"\"","\"\""))
                            .append("\"");
                    }
                    if (i + 1 < table.getColumnNames().length) {
                        kafkaText.append(",");
                    }
                }
            } else if (outputFormat.equals(KAFKA_FORMAT_XML)) {
                kafkaText.append("<row entity=\"").append(StringEscapeUtils.escapeXml11(table.getName())).append("\"").append(" dml=\"").append(data.getDataEventType())
                        .append("\">");
                for (int i = 0; i < table.getColumnNames().length; i++) {
                    kafkaText.append("<data key=\"").append(StringEscapeUtils.escapeXml11(table.getColumnNames()[i])).append("\">").append(StringEscapeUtils.escapeXml11(rowData[i])).append("</data>");
                }
                kafkaText.append("</row>");
            } else if (outputFormat.equals(KAFKA_FORMAT_AVRO)) {
                if (confluentUrl != null) {
                    String tableName = getTableName(table.getName());
                    try {
                        Class<?> curClass = getClassByTableName(tableName);
                        if (curClass != null) {

                            Constructor<?> defaultConstructor = curClass.getConstructor();
                            Object pojo = defaultConstructor.newInstance();

                            for (int i = 0; i < table.getColumnNames().length; i++) {
                                String colName = getColumnName(table.getName(), table.getColumnNames()[i], pojo);
                                if (colName != null) {
                                    Class<?> propertyTypeClass = PropertyUtils.getPropertyType(pojo, colName);
                                    if (CharSequence.class.equals(propertyTypeClass)) {
                                        PropertyUtils.setSimpleProperty(pojo, colName, rowData[i]);
                                    }
                                    else if (Long.class.equals(propertyTypeClass)) {
                                        Date date = null;
                                        try {
                                            date = DateUtils.parseDate(rowData[i], parseDatePatterns);
                                        }
                                        catch (Exception e) {
                                            log.debug(rowData[i] + " was not a recognized date format so treating it as a long.");
                                        }
                                        BeanUtils.setProperty(pojo, colName, date != null ? date.getTime() : rowData[i]);
                                    }
                                    else {
                                        BeanUtils.setProperty(pojo, colName, rowData[i]);
                                    }
                                }
                            }
                            sendKafkaMessage(new ProducerRecord<String, Object>(kafkaDataKey, kafkaKey, pojo));
                        } else {
                            throw new RuntimeException("Unable to find a POJO to load for AVRO based message onto Kafka for table : " + tableName);
                        }
                    } catch (NoSuchMethodException e) {
                        log.info("Unable to find setter on POJO based on table " + table.getName(), e);
                        throw new RuntimeException(e);
                    } catch (InvocationTargetException e) {
                        log.info("Unable to invoke a default constructor on POJO based on table " + tableName, e);
                        throw new RuntimeException(e);
                    } catch (IllegalAccessException e) {
                        log.info("Unable to access a default constructor on POJO based on table " + tableName, e);
                        throw new RuntimeException(e);
                    } catch (InstantiationException e) {
                        log.info("Unable to instantiate a constructor on POJO based on table " + tableName, e);
                        throw new RuntimeException(e);
                    } 
                } else {
                    GenericData.Record avroRecord = new GenericData.Record(schema);
                    avroRecord.put("table", table.getName());
                    avroRecord.put("eventType", data.getDataEventType().toString());

                    Collection<GenericRecord> dataCollection = new ArrayList<GenericRecord>();

                    for (int i = 0; i < table.getColumnNames().length; i++) {

                        GenericRecord columnRecord = new GenericData.Record(schema.getField("data").schema().getElementType());

                        columnRecord.put("name", table.getColumnNames()[i]);
                        columnRecord.put("value", rowData[i]);

                        dataCollection.add(columnRecord);
                    }
                    avroRecord.put("data", dataCollection);
                    try {
                        kafkaText.append(datumToByteArray(schema, avroRecord));
                    } catch (IOException ioe) {
                        throw new RuntimeException("Unable to convert row data to an Avro record", ioe);
                    }
                }
            }
            kafkaDataList.add(new ProducerRecord<String, Object>(kafkaDataKey, kafkaKey, kafkaText.toString()));
        }
        return false;
    }
    
    public String getTableName(String dbTableName) {
        if (tableNameCache.containsKey(dbTableName)) {
            return tableNameCache.get(dbTableName);
        } else {
            String[] split = dbTableName.split("_");
            StringBuffer tableName = new StringBuffer();
            for (String part : split) {
                tableName.append(StringUtils.capitalize(part.toLowerCase()));
            }
            tableNameCache.put(dbTableName, tableName.toString());
            return tableName.toString();
        }
    }

    public String getColumnName(String dbTableName, String dbColumnName, Object bean) {
        if (tableColumnCache.containsKey(dbColumnName) && tableColumnCache.get(dbTableName).containsKey(dbColumnName)) {
            return tableColumnCache.get(dbTableName).get(dbColumnName);
        } else {
            String columnName = null;
            if (!tableColumnCache.containsKey(dbColumnName)) {
                tableColumnCache.put(dbTableName, new HashMap<String, String>());
            }

            String dbColumnNameSimple = dbColumnName.toLowerCase().replaceAll("[^a-z0-9]", "");
            for (PropertyDescriptor pd : PropertyUtils.getPropertyDescriptors(bean)) {
                if (pd.getName().toLowerCase().equals(dbColumnNameSimple)) {
                    columnName = pd.getName();
                }
            }

            if (columnName != null) {
                tableColumnCache.get(dbTableName).put(dbColumnName, columnName);
                return columnName;
            } else {
                return null;
            }
        }
    }

    public Class<?> getClassByTableName(String tableName) {
        Class<?> classMatch = null;

        if (tableClassCache.containsKey(tableName)) {
            classMatch = tableClassCache.get(tableName);
        } else {

            try {
                log.debug("Looking for an exact match for a POJO based on tableName " + tableName);
                classMatch = Class.forName(schemaPackage + "." + tableName);
            } catch (Exception e) {
                if (schemaPackageClassNames == null || schemaPackageClassNames.size() == 0) {
                    scanSchemaPackage();
                }
                String fullTableName = schemaPackage + "." + tableName;
                for (String scannedName : schemaPackageClassNames) {
                    if (scannedName.indexOf("$") > 0) {
                        continue;
                    }
                    if (scannedName.startsWith(fullTableName)) {
                        try {
                            log.debug("Looking for a starts with match for a POJO based on tableName " + scannedName);
                            classMatch = Class.forName(scannedName);
                            break;
                        } catch (Exception nestedException) {
                        }
                    } else if (scannedName.toLowerCase().startsWith(fullTableName)) {
                        try {
                            log.debug("Looking for a starts with match all lower case for a POJO based on tableName " + scannedName);
                            classMatch = Class.forName(scannedName);
                            break;
                        } catch (Exception nestedException) {
                        }
                    }
                }
            }
            if (classMatch != null) {
                tableClassCache.put(tableName, classMatch);
            }
        }
        return classMatch;
    }

    private String resolveBasePackage(String basePackage) {
        return ClassUtils.convertClassNameToResourcePath(SystemPropertyUtils.resolvePlaceholders(basePackage));
    }

    private void scanSchemaPackage() {
        try {
            ResourcePatternResolver resourcePatternResolver = new PathMatchingResourcePatternResolver();
            MetadataReaderFactory metadataReaderFactory = new CachingMetadataReaderFactory(resourcePatternResolver);

            String packageSearchPath = ResourcePatternResolver.CLASSPATH_ALL_URL_PREFIX + resolveBasePackage(schemaPackage) + "/"
                    + "**/*.class";
            Resource[] resources = resourcePatternResolver.getResources(packageSearchPath);
            for (Resource resource : resources) {
                if (resource.isReadable()) {
                    MetadataReader metadataReader = metadataReaderFactory.getMetadataReader(resource);
                    schemaPackageClassNames.add(metadataReader.getClassMetadata().getClassName());
                }
            }
            Collections.sort(schemaPackageClassNames, Collections.reverseOrder());
        } catch (Exception e) {
            log.warn("Unable to scan schema package : " + schemaPackage);
        }
    }

    public void afterWrite(DataContext context, Table table, CsvData data) {
    }

    public boolean handlesMissingTable(DataContext context, Table table) {
        return true;
    }

    public void earlyCommit(DataContext context) {
    }

    public void batchComplete(DataContext context) {
        if (!context.getBatch().getChannelId().equals("heartbeat") && !context.getBatch().getChannelId().equals("config")) {
            String batchFileName = "batch-" + context.getBatch().getSourceNodeId() + "-" + context.getBatch().getBatchId();
            
            log.debug("Kafka client config: {}", configs);
            try {
                if (confluentUrl == null && kafkaDataMap.size() > 0) {
                    StringBuffer kafkaText = new StringBuffer();
                    String kafkaKey = null;
                    
                    for (Map.Entry<String, List<ProducerRecord<String, Object>>> entry : kafkaDataMap.entrySet()) {
                        for (ProducerRecord<String, Object> record : entry.getValue()) {
                            if (messageBy.equals(KAFKA_MESSAGE_BY_ROW)) {
                                sendKafkaMessage(record);
                            } else {
                                kafkaKey = record.key();
                                kafkaText.append(record.value());
                            }
                        }
                        if (messageBy.equals(KAFKA_MESSAGE_BY_BATCH)) {
                            sendKafkaMessage(new ProducerRecord<String, Object>(entry.getKey(), kafkaKey, kafkaText.toString()));
                        }
                    }
                    kafkaDataMap = new HashMap<String, List<ProducerRecord<String, Object>>>();
                } 
            } catch (Exception e) {
                log.warn("Unable to write batch to Kafka " + batchFileName, e);
                e.printStackTrace();
            } finally {
                context.put(KAFKA_TEXT_CACHE, new HashMap<String, List<String>>());
                tableNameCache.clear();
                tableColumnCache = new HashMap<String, Map<String, String>>();
            }
        }
    }

    public void batchCommitted(DataContext context) {
    }

    public void batchRolledback(DataContext context) {
    }

    public void sendKafkaMessage(ProducerRecord<String, Object> record) {
        log.debug("Sending message (topic={}) (key={}) {}", record.topic(), record.key(), record.value());
        kafkaProducer.send(record);
    }

    public static byte[] datumToByteArray(Schema schema, GenericRecord datum) throws IOException {
        GenericDatumWriter<GenericRecord> writer = new GenericDatumWriter<GenericRecord>(schema);
        ByteArrayOutputStream os = new ByteArrayOutputStream();
        try {
            Encoder e = EncoderFactory.get().binaryEncoder(os, null);
            writer.write(datum, e);
            e.flush();
            byte[] byteData = os.toByteArray();
            return byteData;
        } finally {
            os.close();
        }
    }
}
