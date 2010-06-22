package org.jumpmind.symmetric.ddl.io;

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

import java.io.OutputStream;
import java.io.PrintWriter;
import java.io.Writer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import javax.xml.stream.XMLOutputFactory;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamWriter;

import org.apache.commons.beanutils.DynaBean;
import org.apache.commons.codec.binary.Base64;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jumpmind.symmetric.ddl.dynabean.SqlDynaBean;
import org.jumpmind.symmetric.ddl.dynabean.SqlDynaClass;
import org.jumpmind.symmetric.ddl.io.converters.ConversionException;
import org.jumpmind.symmetric.ddl.io.converters.SqlTypeConverter;
import org.jumpmind.symmetric.ddl.model.Column;
import org.jumpmind.symmetric.ddl.model.Table;

/**
 * Writes dyna beans matching a specified database model into an XML file.
 * 
 * TODO: Make names (tables, columns) XML-compliant
 * 
 * @version $Revision: 289996 $
 */
public class DataWriter
{
    /** String values with a size not bigger than this value will be written to attributes;
        if their size is longer, then a sub element is generated instead. */ 
    private static final int MAX_ATTRIBUTE_LENGTH = 255;
    /** The indentation string. */
    private static final String INDENT_STRING = "  ";

    /** Our log. */
    private final Log _log = LogFactory.getLog(DataWriter.class);

    /** The converters. */
    private ConverterConfiguration _converterConf = new ConverterConfiguration();
    /** The output stream. */
    private PrintWriter _output;
    /** The xml writer. */
    private XMLStreamWriter _writer;
    /** The output encoding. */
    private String _encoding;
    /** Whether we're pretty-printing. */
    private boolean _prettyPrinting = true;

    /**
     * Creates a data writer instance using UTF-8 encoding.
     * 
     * @param output The target to write the data XML to
     */
    public DataWriter(OutputStream output) throws DataWriterException
    {
        this(output, null);
    }

    /**
     * Creates a data writer instance.
     * 
     * @param output   The target to write the data XML to
     * @param encoding The encoding of the XML file
     */
    public DataWriter(OutputStream output, String encoding) throws DataWriterException
    {
        _output = new PrintWriter(output);
        if ((encoding == null) || (encoding.length() == 0))
        {
            _encoding = "UTF-8";
        }
        else
        {
            _encoding = encoding;
        }

        try
        {
            XMLOutputFactory factory = XMLOutputFactory.newInstance();

            _writer  = factory.createXMLStreamWriter(output, _encoding);
        }
        catch (XMLStreamException ex)
        {
            throw new DataWriterException(ex);
        }
    }

    /**
     * Creates a data writer instance using the specified writer. Note that the writer
     * needs to be configured using the specified encoding.
     * 
     * @param output   The target to write the data XML to
     * @param encoding The encoding of the writer
     */
    public DataWriter(Writer output, String encoding) throws DataWriterException
    {
        _output   = new PrintWriter(output);
        _encoding = encoding;
        try
        {
            XMLOutputFactory factory = XMLOutputFactory.newInstance();

            _writer = factory.createXMLStreamWriter(_output);
        }
        catch (XMLStreamException ex)
        {
            throw new DataWriterException(ex);
        }
    }

    /**
     * Determines whether the output shall be pretty-printed.
     *
     * @return <code>true</code> if the output is pretty-printed
     */
    public boolean isPrettyPrinting()
    {
        return _prettyPrinting;
    }

    /**
     * Specifies whether the output shall be pretty-printed.
     *
     * @param prettyPrinting <code>true</code> if the output is pretty-printed
     */
    public void setPrettyPrinting(boolean prettyPrinting)
    {
        _prettyPrinting = prettyPrinting;
    }

    /**
     * Returns the converter configuration of this data reader.
     * 
     * @return The converter configuration
     */
    public ConverterConfiguration getConverterConfiguration()
    {
        return _converterConf;
    }

    /**
     * Prints a newline if we're pretty-printing.
     */
    private void printlnIfPrettyPrinting() throws DataWriterException
    {
        if (_prettyPrinting)
        {
            try
            {
                _writer.writeCharacters("\n");
            }
            catch (XMLStreamException ex)
            {
                throw new DataWriterException(ex);
            }
        }
    }

    /**
     * Prints the indentation if we're pretty-printing.
     * 
     * @param level The indentation level
     */
    private void indentIfPrettyPrinting(int level) throws DataWriterException
    {
        if (_prettyPrinting)
        {
            try
            {
                for (int idx = 0; idx < level; idx++)
                {
                    _writer.writeCharacters(INDENT_STRING);
                }
            }
            catch (XMLStreamException ex)
            {
                throw new DataWriterException(ex);
            }
        }
    }

    /**
     * Writes the start of the XML document, i.e. the "<?xml?>" section and the start of the
     * root node.
     */
    public void writeDocumentStart() throws DataWriterException
    {
        try
        {
            _writer.writeStartDocument(_encoding, "1.0");
            printlnIfPrettyPrinting();
            _writer.writeStartElement("data");
            printlnIfPrettyPrinting();
        }
        catch (XMLStreamException ex)
        {
            throw new DataWriterException(ex);
        }
    }

    /**
     * Writes the end of the XML document, i.e. end of the root node.
     */
    public void writeDocumentEnd() throws DataWriterException
    {
        try
        {
            _writer.writeEndElement();
            printlnIfPrettyPrinting();
            _writer.writeEndDocument();
            _writer.flush();
            _writer.close();
            _output.close();
        }
        catch (XMLStreamException ex)
        {
            throw new DataWriterException(ex);
        }
    }

    /**
     * Writes the given bean.
     * 
     * @param bean The bean to write
     */
    public void write(SqlDynaBean bean) throws DataWriterException
    {
        SqlDynaClass dynaClass   = (SqlDynaClass)bean.getDynaClass();
        Table        table       = dynaClass.getTable();
        HashMap      subElements = new HashMap();

        try
        {
            indentIfPrettyPrinting(1);
            _writer.writeStartElement(table.getName());
            for (int idx = 0; idx < table.getColumnCount(); idx++)
            {
                Column           column      = table.getColumn(idx);
                Object           value       = bean.get(column.getName());
                SqlTypeConverter converter   = _converterConf.getRegisteredConverter(table, column);
                String           valueAsText = null;

                if (converter == null)
                {
                    if (value != null)
                    {
                        valueAsText = value.toString();
                    }
                }
                else
                {
                    valueAsText = converter.convertToString(value, column.getTypeCode());
                }
                if (valueAsText != null)
                {
                    // we create an attribute only if the text is not too long
                    // and if it does not contain special characters
                    if ((valueAsText.length() > MAX_ATTRIBUTE_LENGTH) || analyzeText(valueAsText, null))
                    {
                        // we defer writing the sub elements
                        subElements.put(column.getName(), valueAsText);
                    }
                    else
                    {
                        _writer.writeAttribute(column.getName(), valueAsText);
                    }
                }
            }
            if (!subElements.isEmpty())
            {
                List cutPoints = new ArrayList();

                for (Iterator it = subElements.entrySet().iterator(); it.hasNext();)
                {
                    Map.Entry entry     = (Map.Entry)it.next();
                    String    content   = entry.getValue().toString();

                    printlnIfPrettyPrinting();
                    indentIfPrettyPrinting(2);
                    _writer.writeStartElement(entry.getKey().toString());

                    // if the content contains special characters, we have to apply base64 encoding to it
                    // if the content is too short, then it has to contain special characters (otherwise
                    // it would have been written as an attribute already), otherwise we check
                    cutPoints.clear();

                    boolean writeBase64Encoded = analyzeText(content, cutPoints);

                    if (writeBase64Encoded)
                    {
                        _writer.writeAttribute(DatabaseIO.BASE64_ATTR_NAME, "true");
                        _writer.writeCData(new String(Base64.encodeBase64(content.getBytes())));
                    }
                    else
                    {
                        if (cutPoints.isEmpty())
                        {
                            _writer.writeCData(content);
                        }
                        else
                        {
                            int lastPos = 0;

                            for (Iterator cutPointIt = cutPoints.iterator(); cutPointIt.hasNext();)
                            {
                                int curPos = ((Integer)cutPointIt.next()).intValue();

                                _writer.writeCData(content.substring(lastPos, curPos));
                                lastPos = curPos;
                            }
                            if (lastPos < content.length())
                            {
                                _writer.writeCData(content.substring(lastPos));
                            }
                        }
                    }

                    _writer.writeEndElement();
                }
                printlnIfPrettyPrinting();
                indentIfPrettyPrinting(1);
            }
            _writer.writeEndElement();
            printlnIfPrettyPrinting();
        }
        catch (XMLStreamException ex)
        {
            throw new DataWriterException(ex);
        }
        catch (ConversionException ex)
        {
            throw new DataWriterException(ex);
        }
    }

    /**
     * Determines whether the given string contains special characters that cannot
     * be used in XML, and if not, finds the cut points where to split the text
     * when writing it in a CDATA section.
     * 
     * @param text      The text
     * @param cutPoints Will be filled with cut points to split the text when writing it
     *                  in a CDATA section (only if the method returns <code>false</code>)
     * @return <code>true</code> if the text contains special characters
     */
    private boolean analyzeText(String text, List cutPoints)
    {
        List tmpCutPoints          = cutPoints == null ? null : new ArrayList();
        int  numChars              = text.length();
        int  numFoundCDataEndChars = 0;

        for (int charPos = 0; charPos < numChars; charPos++)
        {
            char c = text.charAt(charPos);

            if ((c < 0x0020) && (c != '\n') && (c != '\r') && (c != '\t'))
            {
                return true;
            }
            else if (cutPoints != null)
            {
                if ((c == ']') && ((numFoundCDataEndChars == 0) || (numFoundCDataEndChars == 1)))
                {
                    numFoundCDataEndChars++;
                }
                else if ((c == '>') && (numFoundCDataEndChars == 2))
                {
                    // we have to split the CDATA right here before the '>' (see DDLUTILS-174)
                    tmpCutPoints.add(new Integer(charPos));
                    numFoundCDataEndChars = 0;
                }
                else
                {
                    numFoundCDataEndChars = 0;
                }
            }
        }
        if (cutPoints != null)
        {
            cutPoints.addAll(tmpCutPoints);
        }
        return false;
    }

    /**
     * Writes the beans contained in the given iterator.
     * 
     * @param beans The beans iterator
     */
    public void write(Iterator beans) throws DataWriterException
    {
        while (beans.hasNext())
        {
            DynaBean bean = (DynaBean)beans.next();

            if (bean instanceof SqlDynaBean)
            {
                write((SqlDynaBean)bean);
            }
            else
            {
                _log.warn("Cannot write normal dyna beans (type: "+bean.getDynaClass().getName()+")");
            }
        }
    }

    /**
     * Writes the beans contained in the given collection.
     * 
     * @param beans The beans
     */
    public void write(Collection beans) throws DataWriterException
    {
        write(beans.iterator());
    }
}
