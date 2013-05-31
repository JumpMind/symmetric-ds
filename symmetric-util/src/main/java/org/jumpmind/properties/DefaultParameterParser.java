package org.jumpmind.properties;

import java.io.IOException;
import java.io.InputStream;
import java.io.Serializable;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DefaultParameterParser {

    private static final String IGNORE_COMMENT = "#";
    private static final String COMMENT = "# ";
    private static final String DATABASE_OVERRIDABLE = "DatabaseOverridable:";
    private static final String TAGS = "Tags:";
    private static final String TYPE = "Type:";

    private String propertiesFilePath;

    private InputStream inputStream;

    final Logger log = LoggerFactory.getLogger(getClass());

    public DefaultParameterParser(InputStream inputStream) {
        this.inputStream = inputStream;
    }

    public DefaultParameterParser(String propertiesFilePath) {
        this.propertiesFilePath = propertiesFilePath;
    }

    public Map<String, ParameterMetaData> parse() {
        return parse(propertiesFilePath);
    }

    public Map<String, ParameterMetaData> parse(String fileName) {
        Map<String, ParameterMetaData> metaData = new TreeMap<String, DefaultParameterParser.ParameterMetaData>();
        try {
            if (inputStream == null) {
                inputStream = getClass().getResourceAsStream(fileName);
            }
            List<String> lines = IOUtils.readLines(inputStream);

            ParameterMetaData currentMetaData = new ParameterMetaData();
            for (String line : lines) {
                if (line.trim().startsWith(COMMENT) && line.length() > 1) {
                    line = line.substring(line.indexOf(COMMENT) + 1);
                    if (line.contains(DATABASE_OVERRIDABLE)) {
                        currentMetaData.setDatabaseOverridable(Boolean.parseBoolean(line.substring(
                                line.indexOf(DATABASE_OVERRIDABLE) + DATABASE_OVERRIDABLE.length())
                                .trim()));
                    } else if (line.contains(TAGS)) {
                        String[] tags = line.substring(line.indexOf(TAGS) + TAGS.length()).trim()
                                .split(",");
                        for (String tag : tags) {
                            currentMetaData.addTag(tag.trim());
                        }
                    } else if (line.contains(TYPE)) {
                        String type = line.substring(line.indexOf(TYPE) + TYPE.length());
                        currentMetaData.setType(type.trim());
                    } else {
                        currentMetaData.appendDescription(line);
                    }
                } else if (!line.trim().startsWith(IGNORE_COMMENT) && line.contains("=")) {
                    String key = line.substring(0, line.indexOf("="));
                    String defaultValue = line.substring(line.indexOf("=") + 1);
                    currentMetaData.setKey(key);
                    currentMetaData.setDefaultValue(defaultValue);
                    metaData.put(key, currentMetaData);
                    currentMetaData = new ParameterMetaData();
                } else if (StringUtils.isBlank(line)) {
                    // reset the metadata
                    currentMetaData = new ParameterMetaData();
                }
            }
        } catch (IOException e) {
            log.error(e.getMessage(), e);
        }
        return metaData;
    }

    public static class ParameterMetaData implements Serializable {

        public static final String TYPE_BOOLEAN = "boolean";
        public static final String TYPE_INT = "integer";
        public static final String TYPE_TEXT_BOX = "textbox";

        private static final long serialVersionUID = 1L;
        private String key;
        private String description;
        private Set<String> tags = new HashSet<String>();
        private boolean databaseOverridable;
        private String defaultValue;
        private String type = "";

        public void setType(String type) {
            this.type = type;
        }

        public String getType() {
            return type;
        }

        public String getKey() {
            return key;
        }

        public void setKey(String key) {
            this.key = key;
        }

        public String getDescription() {
            return description;
        }

        public void setDescription(String description) {
            this.description = description;
        }

        public Set<String> getTags() {
            return tags;
        }

        public void setTags(Set<String> tags) {
            this.tags = tags;
        }

        public boolean isDatabaseOverridable() {
            return databaseOverridable;
        }

        public void setDatabaseOverridable(boolean databaseOverridable) {
            this.databaseOverridable = databaseOverridable;
        }

        public String getDefaultValue() {
            return defaultValue;
        }

        public void setDefaultValue(String defaultValue) {
            this.defaultValue = defaultValue;
        }

        public void appendDescription(String value) {
            if (description == null) {
                description = value;
            } else {
                description = description + value;
            }
        }

        public boolean isBooleanType() {
            return type != null && type.equals(TYPE_BOOLEAN);
        }

        public boolean isIntType() {
            return type != null && type.equals(TYPE_INT);
        }

        public boolean isTextBoxType() {
            return type != null && type.equals(TYPE_TEXT_BOX);
        }

        public void addTag(String tag) {
            tags.add(tag);
        }
    }
}
