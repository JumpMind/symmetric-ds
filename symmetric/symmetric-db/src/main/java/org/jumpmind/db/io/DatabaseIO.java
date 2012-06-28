package org.jumpmind.db.io;

/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.Reader;
import java.io.Writer;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.StringUtils;
import org.jumpmind.db.model.Column;
import org.jumpmind.db.model.Database;
import org.jumpmind.db.model.ForeignKey;
import org.jumpmind.db.model.IIndex;
import org.jumpmind.db.model.IndexColumn;
import org.jumpmind.db.model.NonUniqueIndex;
import org.jumpmind.db.model.Reference;
import org.jumpmind.db.model.Table;
import org.jumpmind.db.model.UniqueIndex;
import org.jumpmind.db.platform.DdlException;
import org.jumpmind.util.FormatUtils;
import org.xmlpull.v1.XmlPullParser;
import org.xmlpull.v1.XmlPullParserFactory;

/*
 * This class provides functions to read and write database models from/to XML.
 */
public class DatabaseIO {

    /*
     * Reads the database model contained in the specified file.
     * 
     * @param filename The model file name
     * 
     * @return The database model
     */
    public Database read(String filename) throws DdlException {
        return read(new File(filename));
    }

    /*
     * Reads the database model contained in the specified file.
     * 
     * @param file The model file
     * 
     * @return The database model
     */
    public Database read(File file) throws DdlException {
        FileReader reader = null;
        try {
            reader = new FileReader(file);
            return read(reader);
        } catch (DdlException ex) {
            throw ex;
        } catch (Exception ex) {
            throw new DdlException(ex);
        } finally {
            IOUtils.closeQuietly(reader);
        }
    }

    public Database read(InputStream is) throws DdlException {
        try {
            return read(new InputStreamReader(is, "UTF-8"));
        } catch (DdlException ex) {
            throw ex;
        } catch (Exception ex) {
            throw new DdlException(ex);
        }
    }

    /*
     * Reads the database model given by the reader.
     * 
     * @param reader The reader that returns the model XML
     * 
     * @return The database model
     */
    public Database read(Reader reader) throws DdlException {
        return read(reader, true);
    }

    /*
     * Reads the database model given by the reader.
     * 
     * @param reader The reader that returns the model XML
     * 
     * @return The database model
     */
    public Database read(Reader reader, boolean validate) throws DdlException {
        try {
            boolean done = false;
            Database database = null;
            Table table = null;
            ForeignKey fk = null;
            IIndex index = null;

            XmlPullParser parser = XmlPullParserFactory.newInstance().newPullParser();
            parser.setInput(reader);

            int eventType = parser.getEventType();
            while (eventType != XmlPullParser.END_DOCUMENT && !done) {
                switch (eventType) {
                    case XmlPullParser.START_DOCUMENT:
                        database = new Database();
                        break;
                    case XmlPullParser.START_TAG:
                        String name = parser.getName();
                        if (name.equalsIgnoreCase("database")) {
                            for (int i = 0; i < parser.getAttributeCount(); i++) {
                                String attributeName = parser.getAttributeName(i);
                                String attributeValue = parser.getAttributeValue(i);
                                if (attributeName.equalsIgnoreCase("name")) {
                                    database.setName(attributeValue);
                                }
                            }
                        } else if (name.equalsIgnoreCase("table")) {
                            table = new Table();
                            for (int i = 0; i < parser.getAttributeCount(); i++) {
                                String attributeName = parser.getAttributeName(i);
                                String attributeValue = parser.getAttributeValue(i);
                                if (attributeName.equalsIgnoreCase("name")) {
                                    table.setName(attributeValue);
                                }
                            }
                            database.addTable(table);
                        } else if (name.equalsIgnoreCase("column")) {
                            Column column = new Column();
                            for (int i = 0; i < parser.getAttributeCount(); i++) {
                                String attributeName = parser.getAttributeName(i);
                                String attributeValue = parser.getAttributeValue(i);
                                if (attributeName.equalsIgnoreCase("name")) {
                                    column.setName(attributeValue);
                                } else if (attributeName.equalsIgnoreCase("primaryKey")) {
                                    column.setPrimaryKey(FormatUtils.toBoolean(attributeValue));
                                } else if (attributeName.equalsIgnoreCase("required")) {
                                    column.setRequired(FormatUtils.toBoolean(attributeValue));
                                } else if (attributeName.equalsIgnoreCase("type")) {
                                    column.setMappedType(attributeValue);
                                } else if (attributeName.equalsIgnoreCase("size")) {
                                    column.setSize(attributeValue);
                                } else if (attributeName.equalsIgnoreCase("default")) {
                                    if (StringUtils.isNotBlank(attributeValue)) {
                                        column.setDefaultValue(attributeValue);
                                    }
                                } else if (attributeName.equalsIgnoreCase("autoIncrement")) {
                                    column.setAutoIncrement(FormatUtils.toBoolean(attributeValue));
                                } else if (attributeName.equalsIgnoreCase("javaName")) {
                                    column.setJavaName(attributeValue);
                                }
                            }
                            if (table != null) {
                                table.addColumn(column);
                            }
                        } else if (name.equalsIgnoreCase("foreign-key")) {
                            fk = new ForeignKey();
                            for (int i = 0; i < parser.getAttributeCount(); i++) {
                                String attributeName = parser.getAttributeName(i);
                                String attributeValue = parser.getAttributeValue(i);
                                if (attributeName.equalsIgnoreCase("name")) {
                                    fk.setName(attributeValue);
                                } else if (attributeName.equalsIgnoreCase("foreignTable")) {
                                    fk.setForeignTableName(attributeValue);
                                }
                            }
                            table.addForeignKey(fk);
                        } else if (name.equalsIgnoreCase("reference")) {
                            Reference ref = new Reference();
                            for (int i = 0; i < parser.getAttributeCount(); i++) {
                                String attributeName = parser.getAttributeName(i);
                                String attributeValue = parser.getAttributeValue(i);
                                if (attributeName.equalsIgnoreCase("local")) {
                                    ref.setLocalColumnName(attributeValue);
                                } else if (attributeName.equalsIgnoreCase("foreign")) {
                                    ref.setForeignColumnName(attributeValue);
                                }
                            }
                            fk.addReference(ref);
                        } else if (name.equalsIgnoreCase("index")
                                || name.equalsIgnoreCase("unique")) {
                            if (name.equalsIgnoreCase("index")) {
                                index = new NonUniqueIndex();
                            } else {
                                index = new UniqueIndex();
                            }
                            for (int i = 0; i < parser.getAttributeCount(); i++) {
                                String attributeName = parser.getAttributeName(i);
                                String attributeValue = parser.getAttributeValue(i);
                                if (attributeName.equalsIgnoreCase("name")) {
                                    index.setName(attributeValue);
                                }
                            }
                            table.addIndex(index);
                        } else if (name.equalsIgnoreCase("index-column") || name.equalsIgnoreCase("unique-column")) {
                            IndexColumn indexColumn = new IndexColumn();
                            for (int i = 0; i < parser.getAttributeCount(); i++) {
                                String attributeName = parser.getAttributeName(i);
                                String attributeValue = parser.getAttributeValue(i);
                                if (attributeName.equalsIgnoreCase("name")) {
                                    indexColumn.setName(attributeValue);
                                } else if (attributeName.equalsIgnoreCase("size")) {
                                    indexColumn.setSize(attributeValue);
                                }
                            }

                            indexColumn.setColumn(table.getColumnWithName(indexColumn.getName()));
                            if (index != null) {
                                index.addColumn(indexColumn);
                            }
                        }
                        break;
                    case XmlPullParser.END_TAG:
                        name = parser.getName();
                        if (name.equalsIgnoreCase("database")) {
                            done = true;
                        } else if (name.equalsIgnoreCase("index")
                                || name.equalsIgnoreCase("unique")) {
                            index = null;
                        } else if (name.equalsIgnoreCase("table")) {
                            table = null;
                        } else if (name.equalsIgnoreCase("foreign-key")) {
                            fk = null;
                        }
                        break;
                }
                eventType = parser.next();
            }

            if (validate) {
                database.initialize();
            }
            return database;
        } catch (Exception e) {
            throw new DdlException(e);
        }
    }

    /*
     * Writes the database model to the specified file.
     * 
     * @param model The database model
     * 
     * @param filename The model file name
     */
    public void write(Database model, String filename) throws DdlException {
		try {
			BufferedWriter writer = null;
			try {
				writer = new BufferedWriter(new FileWriter(filename));
				write(model, writer);
				writer.flush();
			} finally {
				if (writer != null) {
					writer.close();
				}
			}
		} catch (Exception ex) {
			throw new DdlException(ex);
		}
    }

    /*
     * Writes the database model to the given output stream. Note that this
     * method does not flush the stream.
     * 
     * @param model The database model
     * 
     * @param output The output stream
     */
    public void write(Database model, OutputStream output) throws DdlException {
        Writer writer = new OutputStreamWriter(output);
        write(model, writer);
        try {
            writer.flush();
        } catch (Exception e) {
            throw new DdlException(e);
        }
    }

    /*
     * Writes the database model to the given output writer. Note that this
     * method does not flush the writer.
     * 
     * @param model The database model
     * 
     * @param output The output writer
     */

    public void write(Database model, Writer output) throws DdlException {
        write(model, output, null);
    }

    public void write(Database model, Writer output, String rootElementName) throws DdlException {
    	try {
	    	output.write("<?xml version=\"1.0\"?>\n<!DOCTYPE database SYSTEM \"" + LocalEntityResolver.DTD_PREFIX + "\">\n");
	    	if (rootElementName != null) {
	    	    output.write("<" + rootElementName + ">\n");
	    	}
	    	output.write("<database name=\"" + model.getName() + "\"");
	    	if (model.getIdMethod() != null) {
	    		output.write(" defaultIdMethod=\"" + model.getIdMethod() + "\"");
	    	}
	    	output.write(">\n");
	    	
	    	for (Table table : model.getTables()) {
	    		output.write("\t<table name=\"" + table.getName() + "\">\n");
	    		
	    		for (Column column : table.getColumns()) {
	    			output.write("\t\t<column name=\"" + column.getName() + "\"");
	    			if (column.isPrimaryKey()) {
	    				output.write(" primaryKey=\"" + column.isPrimaryKey() + "\"");
	    			}
	    			if (column.isRequired()) {
	    				output.write(" required=\"" + column.isRequired() + "\"");
	    			}
	    			if (column.getMappedType() != null) {
	    				output.write(" type=\"" + column.getMappedType() + "\"");
	    			}
	    			if (column.getSize() != null) {
	    				output.write(" size=\"" + column.getSize() + "\"");
	    			}
	    			if (column.getDefaultValue() != null) {
	    				output.write(" default=\"" + column.getDefaultValue() + "\"");
	    			}
	    			if (column.isAutoIncrement()) {
	    				output.write(" autoIncrement=\"" + column.isAutoIncrement() + "\"");
	    			}
	    			if (column.getJavaName() != null) {
	    				output.write(" javaName=\"" + column.getJavaName() + "\"");
	    			}
	    			output.write("/>\n");
	    		}

	    		for (ForeignKey fk : table.getForeignKeys()) {
	    			output.write("\t\t<foreign-key name=\"" + fk.getName() + "\" foreignTable=\"" + fk.getForeignTableName() + "\">\n");
	    			for (Reference ref : fk.getReferences()) {
	    				output.write("\t\t\t<reference local=\"" + ref.getLocalColumnName() + "\" foreign=\"" + ref.getForeignColumnName() + "\"/>\n");
	    			}
	    			output.write("\t\t</foreign-key>\n");
	    		}

	    		for (IIndex index : table.getIndices()) {
	    			if (index.isUnique()) {
	    				output.write("\t\t<unique name=\"" + index.getName() + "\">\n");
	    				for (IndexColumn column : index.getColumns()) {
	    					output.write("\t\t\t<unique-column name=\"" + column.getName() + "\"/>\n");
		    			}
	    				output.write("\t\t</unique>\n");
	    			} else {
	    				output.write("\t\t<index name=\"" + index.getName() + "\">\n");
		    			for (IndexColumn column : index.getColumns()) {
	    					output.write("\t\t\t<index-column name=\"" + column.getName() + "\"");
	    					if (column.getSize() != null) {
	    						output.write(" size=\"" + column.getSize() + "\"");
	    					}
	    					output.write("/>\n");
		    			}
	    				output.write("\t\t</index>\n");
	    			}
	    		}

	    		output.write("\t</table>\n");
	    	}

	    	output.write("</database>\n");
    	} catch (Exception e) {
    		throw new DdlException(e);
    	}
    }
}
