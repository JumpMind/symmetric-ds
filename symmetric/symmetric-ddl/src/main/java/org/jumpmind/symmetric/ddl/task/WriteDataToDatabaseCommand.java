package org.jumpmind.symmetric.ddl.task;

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

import java.io.File;
import java.util.ArrayList;
import java.util.Iterator;

import org.apache.tools.ant.BuildException;
import org.apache.tools.ant.DirectoryScanner;
import org.apache.tools.ant.Task;
import org.apache.tools.ant.types.FileSet;
import org.jumpmind.symmetric.ddl.Platform;
import org.jumpmind.symmetric.ddl.io.DataReader;
import org.jumpmind.symmetric.ddl.model.Database;

/**
 * Inserts the data defined by the data XML file(s) into the database. This requires the schema
 * in the database to match the schema defined by the XML files specified at the enclosing task.<br/>
 * DdlUtils will honor the order imposed by the foreign keys. Ie. first all required entries are
 * inserted, then the dependent ones. Obviously this requires that no circular references exist
 * in the schema (DdlUtils currently does not check this). Also, the referenced entries must be
 * present in the data, otherwise the task will fail. This behavior can be turned off via the
 * <code>ensureForeignKeyOrder</code> attribute.<br/>
 * In order to define data for foreign key dependencies that use auto-incrementing primary keys,
 * simply use some unique values for their columns. DdlUtils then will automatically use the real
 * primary key values when inserting the data. Note though that not every database supports the
 * retrieval of auto-increment values which is necessary for this to work.
 * 
 * @version $Revision: 289996 $
 * @ant.task name="writeDataToDatabase"
 */
public class WriteDataToDatabaseCommand extends ConvertingDatabaseCommand
{
    /** A single data file to insert. */
    private File _singleDataFile = null;
    /** The input files. */
    private ArrayList _fileSets = new ArrayList();
    /** Whether explicit values for identity columns will be used. */
    private boolean _useExplicitIdentityValues;

    /**
     * Defines whether values for identity columns in the data XML shall be used instead of
     * letting the database define the value. Unless <code>ensureForeignKeyOrder</code> is
     * set to false, setting this to <code>false</code> (the default) does not affect foreign
     * keys as DdlUtils will automatically update the values of the columns of foreign keys
     * pointing to the inserted row with the database-created values. 
     *
     * @param useExplicitIdentityValues <code>true</code> if explicitly specified identity
     *                                  column values should be inserted instead of letting
     *                                  the database define the values for these columns
     * @ant.not-required Default is <code>false</code>
     */
    public void setUseExplicitIdentityValues(boolean useExplicitIdentityValues)
    {
        _useExplicitIdentityValues = useExplicitIdentityValues;
    }

    /**
     * Adds a fileset.
     * 
     * @param fileset The additional input files
     */
    public void addConfiguredFileset(FileSet fileset)
    {
        _fileSets.add(fileset);
    }

    /**
     * Specifies the name of the single XML file that contains the data to insert into the database.
     *
     * @param dataFile The data file
     * @ant.not-required Use either this or <code>fileset</code> sub elements.
     */
    public void setDataFile(File dataFile)
    {
        _singleDataFile = dataFile;
    }

    /**
     * The maximum number of insert statements to combine in one batch. The number typically
     * depends on the JDBC driver and the amount of available memory.<br/>
     * This value is only used if <code>useBatchMode</code> is <code>true</code>.
     *
     * @param batchSize The number of objects
     * @ant.not-required The default value is 1.
     */
    public void setBatchSize(int batchSize)
    {
        getDataIO().setBatchSize(new Integer(batchSize));
    }

    /**
     * Specifies whether batch mode shall be used for inserting the data. In batch mode, insert statements
     * for the same table are bundled together and executed as one statement. This can be a lot faster
     * than single insert statements but is not supported by all JDBC drivers/databases. To achieve the
     * highest performance, you should group the data in the XML file according to the tables. This is
     * because a batch insert only works for one table at a time. Thus when the table changes in an
     * entry in the XML file, the batch is committed and then a new one is started.
     *
     * @param useBatchMode <code>true</code> if batch mode shall be used
     * @ant.not-required Per default batch mode is not used.
     */
    public void setUseBatchMode(boolean useBatchMode)
    {
        getDataIO().setUseBatchMode(useBatchMode);
    }

    /**
     * Specifies whether the foreign key order shall be honored when inserting data into the database.
     * If not, DdlUtils will simply assume that the entry order is correct, i.e. that referenced rows
     * come before referencing rows in the data XML. Note that execution will be slower when DdlUtils
     * has to ensure the foreign-key order of the data. Thus if you know that the data is specified in
     * foreign key order turn this off.
     *
     * @param ensureFKOrder <code>true</code> if the foreign key order shall be followed
     * @ant.not-required Per default foreign key order is honored.
     */
    public void setEnsureForeignKeyOrder(boolean ensureFKOrder)
    {
        getDataIO().setEnsureFKOrder(ensureFKOrder);
    }

    /**
     * {@inheritDoc}
     */
    public void execute(DatabaseTaskBase task, Database model) throws BuildException
    {
        if ((_singleDataFile != null) && !_fileSets.isEmpty())
        {
            throw new BuildException("Please use either the datafile attribute or the sub fileset element, but not both");
        }

        Platform   platform   = getPlatform();
        DataReader dataReader = null;

        platform.setIdentityOverrideOn(_useExplicitIdentityValues);
        try
        {
            dataReader = getDataIO().getConfiguredDataReader(platform, model);
            dataReader.getSink().start();
            if (_singleDataFile != null)
            {
                readSingleDataFile(task, dataReader, _singleDataFile);
            }
            else
            {
                for (Iterator it = _fileSets.iterator(); it.hasNext();)
                {
                    FileSet          fileSet    = (FileSet)it.next();
                    File             fileSetDir = fileSet.getDir(task.getProject());
                    DirectoryScanner scanner    = fileSet.getDirectoryScanner(task.getProject());
                    String[]         files      = scanner.getIncludedFiles();
    
                    for (int idx = 0; (files != null) && (idx < files.length); idx++)
                    {
                        readSingleDataFile(task, dataReader, new File(fileSetDir, files[idx]));
                    }
                }
            }
        }
        catch (Exception ex)
        {
            handleException(ex, ex.getMessage());
        }
        finally
        {
            if (dataReader != null)
            {
                dataReader.getSink().end();
            }
        }
    }

    /**
     * Reads a single data file.
     * 
     * @param task     The parent task
     * @param reader   The data reader
     * @param dataFile The schema file
     */
    private void readSingleDataFile(Task task, DataReader reader, File dataFile) throws BuildException
    {
        if (!dataFile.exists())
        {
            _log.error("Could not find data file " + dataFile.getAbsolutePath());
        }
        else if (!dataFile.isFile())
        {
            _log.error("Path " + dataFile.getAbsolutePath() + " does not denote a data file");
        }
        else if (!dataFile.canRead())
        {
            _log.error("Could not read data file " + dataFile.getAbsolutePath());
        }
        else
        {
            try
            {
                getDataIO().writeDataToDatabase(reader, dataFile.getAbsolutePath());
                _log.info("Written data from file " + dataFile.getAbsolutePath() + " to database");
            }
            catch (Exception ex)
            {
                handleException(ex, "Could not parse or write data file " + dataFile.getAbsolutePath());
            }
        }
    }
}
