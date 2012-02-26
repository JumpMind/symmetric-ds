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

import java.beans.IntrospectionException;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.Reader;
import java.io.Writer;

import org.apache.commons.betwixt.io.BeanReader;
import org.apache.commons.betwixt.io.BeanWriter;
import org.apache.commons.betwixt.strategy.HyphenatedNameMapper;
import org.jumpmind.db.model.Database;
import org.jumpmind.db.platform.DdlException;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;

/*
 * This class provides functions to read and write database models from/to XML.
 * 
 * @version $Revision: 481151 $
 */
public class DatabaseIO
{
    /* The name of the XML attribute use to denote that teh content of a data XML
        element uses Base64 encoding. */
    public static final String BASE64_ATTR_NAME = "base64";

    /* Whether to validate the XML. */
    private boolean _validateXml = true;
    /* Whether to use the internal dtd that comes with DdlUtils. */
    private boolean _useInternalDtd = true;

    /*
     * Returns whether XML is validated upon reading it.
     * 
     * @return <code>true</code> if read XML is validated
     */
    public boolean isValidateXml()
    {
        return _validateXml;
    }

    /*
     * Specifies whether XML shall be validated upon reading it.
     * 
     * @param validateXml <code>true</code> if read XML shall be validated
     */
    public void setValidateXml(boolean validateXml)
    {
        _validateXml = validateXml;
    }

    /*
     * Returns whether the internal dtd that comes with DdlUtils is used.
     * 
     * @return <code>true</code> if parsing uses the internal dtd
     */
    public boolean isUseInternalDtd()
    {
        return _useInternalDtd;
    }

    /*
     * Specifies whether the internal dtd is to be used.
     *
     * @param useInternalDtd Whether to use the internal dtd 
     */
    public void setUseInternalDtd(boolean useInternalDtd)
    {
        _useInternalDtd = useInternalDtd;
    }

    /*
     * Returns the commons-betwixt mapping file as an {@link org.xml.sax.InputSource} object.
     * Per default, this will be classpath resource under the path <code>/mapping.xml</code>.
     *  
     * @return The input source for the mapping
     */
    protected InputSource getBetwixtMapping()
    {
        return new InputSource(getClass().getResourceAsStream("mapping.xml"));
    }
    
    /*
     * Returns a new bean reader configured to read database models.
     * 
     * @return The reader
     */
    public BeanReader getReader() throws IntrospectionException, SAXException, IOException
    {
        BeanReader reader = new BeanReader();

        reader.getXMLIntrospector().getConfiguration().setAttributesForPrimitives(true);
        reader.getXMLIntrospector().getConfiguration().setWrapCollectionsInElement(false);
        reader.getXMLIntrospector().getConfiguration().setElementNameMapper(new HyphenatedNameMapper());
        reader.setValidating(isValidateXml());
        if (isUseInternalDtd())
        {
            reader.setEntityResolver(new LocalEntityResolver());
        }
        reader.registerMultiMapping(getBetwixtMapping());

        return reader;
    }

    /*
     * Returns a new bean writer configured to writer database models.
     * 
     * @param output The target output writer
     * @return The writer
     */
    protected BeanWriter getWriter(Writer output) throws DdlException
    {
        try
        {
            BeanWriter writer = new BeanWriter(output);
    
            writer.getXMLIntrospector().register(getBetwixtMapping());
            writer.getXMLIntrospector().getConfiguration().setAttributesForPrimitives(true);
            writer.getXMLIntrospector().getConfiguration().setWrapCollectionsInElement(false);
            writer.getXMLIntrospector().getConfiguration().setElementNameMapper(new HyphenatedNameMapper());
            writer.getBindingConfiguration().setMapIDs(false);
            writer.enablePrettyPrint();
    
            return writer;
        }
        catch (Exception ex)
        {
            throw new DdlException(ex);
        }
    }

    /*
     * Reads the database model contained in the specified file.
     * 
     * @param filename The model file name
     * @return The database model
     */
    public Database read(String filename) throws DdlException
    {
        Database model = null;

        try
        {
            model = (Database)getReader().parse(filename);
        }
        catch (Exception ex)
        {
            throw new DdlException(ex);
        }
        model.initialize();
        return model;
    }

    /*
     * Reads the database model contained in the specified file.
     * 
     * @param file The model file
     * @return The database model
     */
    public Database read(File file) throws DdlException
    {
        Database model = null;

        try
        {
            model = (Database)getReader().parse(file);
        }
        catch (Exception ex)
        {
            throw new DdlException(ex);
        }
        model.initialize();
        return model;
    }

    /*
     * Reads the database model given by the reader.
     * 
     * @param reader The reader that returns the model XML
     * @return The database model
     */
    public Database read(Reader reader) throws DdlException
    {
        Database model = null;

        try
        {
            model = (Database)getReader().parse(reader);
        }
        catch (Exception ex)
        {
            throw new DdlException(ex);
        }
        model.initialize();
        return model;
    }

    /*
     * Reads the database model from the given input source.
     *
     * @param source The input source
     * @return The database model
     */
    public Database read(InputSource source) throws DdlException
    {
        Database model = null;

        try
        {
            model = (Database)getReader().parse(source);
        }
        catch (Exception ex)
        {
            throw new DdlException(ex);
        }
        model.initialize();
        return model;
    }

    /*
     * Writes the database model to the specified file.
     * 
     * @param model    The database model
     * @param filename The model file name
     */
    public void write(Database model, String filename) throws DdlException
    {
        try
        {
            BufferedWriter writer = null;

            try
            {
                writer = new BufferedWriter(new FileWriter(filename));
    
                write(model, writer);
                writer.flush();
            }
            finally
            {
                if (writer != null)
                {
                    writer.close();
                }
            }
        }
        catch (Exception ex)
        {
            throw new DdlException(ex);
        }
    }

    /*
     * Writes the database model to the given output stream. Note that this method
     * does not flush the stream.
     * 
     * @param model  The database model
     * @param output The output stream
     */
    public void write(Database model, OutputStream output) throws DdlException
    {
        write(model, getWriter(new OutputStreamWriter(output)));
    }

    /*
     * Writes the database model to the given output writer. Note that this method
     * does not flush the writer.
     * 
     * @param model  The database model
     * @param output The output writer
     */
    public void write(Database model, Writer output) throws DdlException
    {
        write(model, getWriter(output));
    }

    /*
     * Internal method that writes the database model using the given bean writer.
     * 
     * @param model  The database model
     * @param writer The bean writer
     */
    private void write(Database model, BeanWriter writer) throws DdlException
    {
        try
        {
            writer.writeXmlDeclaration("<?xml version=\"1.0\"?>\n<!DOCTYPE database SYSTEM \"" + LocalEntityResolver.DTD_PREFIX + "\">");
            writer.write(model);
            writer.flush();
        }
        catch (Exception ex)
        {
            throw new DdlException(ex);
        }
    }
}
