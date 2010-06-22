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

import org.apache.commons.digester.Digester;
import org.jumpmind.symmetric.ddl.io.converters.SqlTypeConverter;
import org.jumpmind.symmetric.ddl.model.Column;
import org.jumpmind.symmetric.ddl.model.Database;
import org.jumpmind.symmetric.ddl.model.Table;

/**
 * Reads data XML into dyna beans matching a specified database model. Note that
 * the data sink won't be started or ended by the data reader, this has to be done
 * in the code that uses the data reader. 
 * 
 * @version $Revision: 289996 $
 */
public class DataReader extends Digester
{
    /** The database model. */
    private Database _model;
    /** The object to receive the read beans. */
    private DataSink _sink;
    /** Specifies whether the (lazy) configuration of the digester still needs to be performed. */
    private boolean  _needsConfiguration = true;
    /** The converters. */
    private ConverterConfiguration _converterConf = new ConverterConfiguration();
    /** Whether to be case sensitive or not. */
    private boolean _caseSensitive = false;

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
     * Returns the database model.
     *
     * @return The model
     */
    public Database getModel()
    {
        return _model;
    }

    /**
     * Sets the database model.
     *
     * @param model The model
     */
    public void setModel(Database model)
    {
        _model              = model;
        _needsConfiguration = true;
    }

    /**
     * Returns the data sink.
     *
     * @return The sink
     */
    public DataSink getSink()
    {
        return _sink;
    }

    /**
     * Sets the data sink.
     *
     * @param sink The sink
     */
    public void setSink(DataSink sink)
    {
        _sink               = sink;
        _needsConfiguration = true;
    }

    /**
     * Determines whether this rules object matches case sensitively.
     *
     * @return <code>true</code> if the case of the pattern matters
     */
    public boolean isCaseSensitive()
    {
        return _caseSensitive;
    }


    /**
     * Specifies whether this rules object shall match case sensitively.
     *
     * @param beCaseSensitive <code>true</code> if the case of the pattern shall matter
     */
    public void setCaseSensitive(boolean beCaseSensitive)
    {
        _caseSensitive = beCaseSensitive;
    }

    /**
     * {@inheritDoc}
     */
    protected void configure()
    {
        if (_needsConfiguration)
        {
            if (_model == null)
            {
                throw new NullPointerException("No database model specified");
            }
            if (_sink == null)
            {
                throw new NullPointerException("No data sink model specified");
            }

            DigesterRules rules = new DigesterRules();

            rules.setCaseSensitive(isCaseSensitive());
            setRules(rules);
            for (int tableIdx = 0; tableIdx < _model.getTableCount(); tableIdx++)
            {
                // TODO: For now we hardcode the root as 'data' but ultimately we should wildcard it ('?')
                Table  table = _model.getTable(tableIdx);
                String path  = "data/"+table.getName();
    
                addRule(path, new DynaSqlCreateRule(_model, table, _sink));
                for (int columnIdx = 0; columnIdx < table.getColumnCount(); columnIdx++)
                {
                    Column           column    = (Column)table.getColumn(columnIdx);
                    SqlTypeConverter converter = _converterConf.getRegisteredConverter(table, column);
    
                    addRule(path, new SetColumnPropertyRule(column, converter, isCaseSensitive()));
                    addRule(path + "/" + column.getName(), new SetColumnPropertyFromSubElementRule(column, converter));
                }
            }
            _needsConfiguration = false;
        }
        super.configure();
    }
}
