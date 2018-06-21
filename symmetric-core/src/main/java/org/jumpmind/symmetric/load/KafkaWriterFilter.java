package org.jumpmind.symmetric.load;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.Encoder;
import org.apache.avro.io.EncoderFactory;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.jumpmind.db.model.Table;
import org.jumpmind.symmetric.common.ParameterConstants;
import org.jumpmind.symmetric.io.data.CsvData;
import org.jumpmind.symmetric.io.data.DataContext;
import org.jumpmind.symmetric.io.data.DataEventType;
import org.jumpmind.symmetric.io.data.writer.IDatabaseWriterFilter;
import org.jumpmind.symmetric.service.IParameterService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KafkaWriterFilter implements IDatabaseWriterFilter {
	protected final String KAFKA_TEXT_CACHE = "KAFKA_TEXT_CACHE" + this.hashCode();

	protected Map<String, List<String>> kafkaDataMap = new HashMap<String, List<String>>();
	protected String kafkaDataKey;

	private final Logger log = LoggerFactory.getLogger(IDatabaseWriterFilter.class);

	private String url;

	private String producer;

	private String outputFormat;

	private String topicBy;

	private String messageBy;

	public final static String KAFKA_FORMAT_XML = "XML";
	public final static String KAFKA_FORMAT_JSON = "JSON";
	public final static String KAFKA_FORMAT_AVRO = "AVRO";
	public final static String KAFKA_FORMAT_CSV = "CSV";

	public final static String KAFKA_MESSAGE_BY_BATCH = "BATCH";
	public final static String KAFKA_MESSAGE_BY_ROW = "ROW";

	public final static String KAFKA_TOPIC_BY_TABLE = "TABLE";
	public final static String KAFKA_TOPIC_BY_CHANNEL = "CHANNEL";

	public final static String AVRO_CDC_SCHEMA = "{"
            + "\"type\":\"record\","
            + "\"name\":\"cdc\","
            + "\"fields\":["
            + "  { \"name\":\"table\", \"type\":\"string\" },"
            + "  { \"name\":\"eventType\", \"type\":\"string\" },"
            + "  { \"name\":\"data\", \"type\":{"
            + "     \"type\":\"array\", \"items\":{"
            + "         \"name\":\"column\","
            + "         \"type\":\"record\","
            + "         \"fields\":["
            + "            {\"name\":\"name\", \"type\":\"string\"},"
            + "            {\"name\":\"value\", \"type\":[\"null\", \"string\"]} ] }}}]}";
	
	
	
	Schema.Parser parser = new Schema.Parser();
    Schema schema = null;
    
	public KafkaWriterFilter(IParameterService parameterService) {
		log.info(AVRO_CDC_SCHEMA);
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
	}

	public boolean beforeWrite(DataContext context, Table table, CsvData data) {
		if (table.getNameLowerCase().startsWith("sym_")) {
			return true;
		} else {
			log.debug("Processing table " + table + " for Kafka");

			String[] rowData = data.getParsedData(CsvData.ROW_DATA);
			if (data.getDataEventType() == DataEventType.DELETE) {
				rowData = data.getParsedData(CsvData.OLD_DATA);
			}

			StringBuffer kafkaText = new StringBuffer();
			
			if (topicBy.equals(KAFKA_TOPIC_BY_CHANNEL)) {
				kafkaDataKey = context.getBatch().getChannelId();
			} else {
				kafkaDataKey = table.getNameLowerCase();
			}

			if (kafkaDataMap.get(kafkaDataKey) == null) {
				kafkaDataMap.put(kafkaDataKey, new ArrayList<String>());
			}
			List<String> kafkaDataList = kafkaDataMap.get(kafkaDataKey);
			
			
			if (outputFormat.equals(KAFKA_FORMAT_JSON)) {
				kafkaText.append("{\"").append(table.getName()).append("\": {")
						.append("\"eventType\": \"" + data.getDataEventType() + "\",").append("\"data\": { ");
				for (int i = 0; i < table.getColumnNames().length; i++) {
					kafkaText.append("\"")
					.append(table.getColumnNames()[i])
					.append("\": ");
					
					if (rowData[i] != null) {
					    kafkaText.append("\"");
					}
					kafkaText.append(rowData[i]);
					if (rowData[i] != null) {
					    kafkaText.append("\"");
					}
					if (i + 1 < table.getColumnNames().length) {
						kafkaText.append(",");
					}
				}
				kafkaText.append(" } } }");
			} else if (outputFormat.equals(KAFKA_FORMAT_CSV)) {
				kafkaText.append("\nTABLE").append(",").append(table.getName()).append(",").append("EVENT").append(",")
						.append(data.getDataEventType()).append(",");

				for (int i = 0; i < table.getColumnNames().length; i++) {
					kafkaText.append(table.getColumnNames()[i]).append(",").append(rowData[i]);
					if (i + 1 < table.getColumnNames().length) {
						kafkaText.append(",");
					}
				}
			} else if (outputFormat.equals(KAFKA_FORMAT_XML)) {
                kafkaText.append("<row entity=\"").append(table.getName()).append("\"")
                    .append(" dml=\"").append(data.getDataEventType()).append("\">");
                for (int i = 0; i < table.getColumnNames().length; i++) {
                    kafkaText.append("<data key=\"")
                        .append(table.getColumnNames()[i])
                        .append("\">")
                        .append(rowData[i])
                        .append("</data>");
                }
                kafkaText.append("</row>");
            } else if (outputFormat.equals(KAFKA_FORMAT_AVRO)) {
                
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
			kafkaDataList.add(kafkaText.toString());
		}
		return false;
	}

	public void afterWrite(DataContext context, Table table, CsvData data) {
	}

	public boolean handlesMissingTable(DataContext context, Table table) {
		return true;
	}

	public void earlyCommit(DataContext context) {
	}

	public void batchComplete(DataContext context) {
		if (!context.getBatch().getChannelId().equals("heartbeat")
				&& !context.getBatch().getChannelId().equals("config")) {
			String batchFileName = "batch-" + context.getBatch().getSourceNodeId() + "-"
					+ context.getBatch().getBatchId();
			log.info("Processing batch " + batchFileName + " for Kafka");
			try {
				if (kafkaDataMap.size() > 0) {
					StringBuffer kafkaText = new StringBuffer();
					for (Map.Entry<String, List<String>> entry : kafkaDataMap.entrySet()) {
						for (String row : entry.getValue()) {
							if (messageBy.equals(KAFKA_MESSAGE_BY_ROW)) {
								sendKafkaMessage(row, entry.getKey());
							} else {
								kafkaText.append(row);
							}
						}
						if (messageBy.equals(KAFKA_MESSAGE_BY_BATCH)) {
							sendKafkaMessage(kafkaText.toString(), entry.getKey());
						}
					}
					kafkaDataMap = new HashMap<String, List<String>>();
				} else {
					log.info("No text found to write to kafka");
				}
			} catch (Exception e) {
				log.warn("Unable to write batch to Kafka " + batchFileName, e);
				e.printStackTrace();
			} finally {
				context.put(KAFKA_TEXT_CACHE, new HashMap<String, List<String>>());
			}
		}
	}

	public void batchCommitted(DataContext context) {
	}

	public void batchRolledback(DataContext context) {
	}

	public void sendKafkaMessage(String kafkaText, String topic) {
		Map<String, Object> configs = new HashMap<String, Object>();

		configs.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, this.url);
		configs.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
				"org.apache.kafka.common.serialization.StringSerializer");
		configs.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
				"org.apache.kafka.common.serialization.StringSerializer");
		configs.put(ProducerConfig.CLIENT_ID_CONFIG, this.producer);

		KafkaProducer<String, String> producer = new KafkaProducer<String, String>(configs);

		producer.send(new ProducerRecord<String, String>(topic, kafkaText));
		log.debug("Data to be sent to Kafka-" + kafkaText);

		producer.close();
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
