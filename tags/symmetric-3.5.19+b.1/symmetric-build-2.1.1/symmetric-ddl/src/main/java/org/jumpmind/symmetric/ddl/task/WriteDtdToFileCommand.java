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
import java.io.FileWriter;

import org.apache.tools.ant.BuildException;
import org.jumpmind.symmetric.ddl.io.DataDtdWriter;
import org.jumpmind.symmetric.ddl.model.Database;

/**
 * Creates a DTD that specifies the layout for data XML files.<br/>
 * This sub task does not require a database connection, so the <code>dataSource</code>
 * sub element of the enclosing task can be omitted.
 * 
 * @version $Revision: 289996 $
 * @ant.task name="writeDtdToFile"
 */
public class WriteDtdToFileCommand extends Command
{
    /** The file to output the DTD to. */
    private File _outputFile;

    /**
     * Specifies the name of the file to write the DTD to.
     * 
     * @param outputFile The output file
     * @ant.required
     */
    public void setOutputFile(File outputFile)
    {
        _outputFile = outputFile;
    }

    /**
     * {@inheritDoc}
     */
    public boolean isRequiringModel()
    {
        return true;
    }

    /**
     * {@inheritDoc}
     */
    public void execute(DatabaseTaskBase task, Database model) throws BuildException
    {
        if (_outputFile == null)
        {
            throw new BuildException("No output file specified");
        }
        if (_outputFile.exists() && !_outputFile.canWrite())
        {
            throw new BuildException("Cannot overwrite output file " + _outputFile.getAbsolutePath());
        }

        try
        {
            FileWriter    outputWriter = new FileWriter(_outputFile);
            DataDtdWriter dtdWriter    = new DataDtdWriter();

            dtdWriter.writeDtd(model, outputWriter);
            outputWriter.close();
            _log.info("Written DTD to " + _outputFile.getAbsolutePath());
        }
        catch (Exception ex)
        {
            handleException(ex, "Failed to write to output file " + _outputFile.getAbsolutePath());
        }
    }
}
