package org.jumpmind.symmetric.ddl.util;

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

/**
 * A statement tokenizer for SQL strings that splits only at delimiters that
 * are at the end of a line or the end of the SQL (row mode).  
 * 
 * TODO: Add awareness of strings, so that semicolons within strings are not parsed
 * 
 * @version $Revision: $
 */
public class SqlTokenizer
{
    /** The SQL to tokenize. */
    private String  _sql;
    /** The index of the last character in the string. */
    private int     _lastCharIdx;
    /** The last delimiter position in the string. */
    private int     _lastDelimiterPos = -1;
    /** The next delimiter position in the string. */
    private int     _nextDelimiterPos = -1;
    /** Whether there are no more tokens. */
    private boolean _finished;

    /**
     * Creates a new sql tokenizer.
     * 
     * @param sql The sql text
     */
    public SqlTokenizer(String sql)
    {
        _sql         = sql;
        _lastCharIdx = sql.length() - 1;
    }

    /**
     * Determines whether there are more statements.
     * 
     * @return <code>true</code> if there are more statements
     */
    public boolean hasMoreStatements()
    {
        if (_finished)
        {
            return false;
        }
        else
        {
            if (_nextDelimiterPos <= _lastDelimiterPos)
            {
                _nextDelimiterPos = _sql.indexOf(';', _lastDelimiterPos + 1);
                while ((_nextDelimiterPos >= 0) && (_nextDelimiterPos < _lastCharIdx))
                {
                    char nextChar = _sql.charAt(_nextDelimiterPos + 1);

                    if ((nextChar == '\r') || (nextChar == '\n'))
                    {
                        break;
                    }
                    _nextDelimiterPos = _sql.indexOf(';', _nextDelimiterPos + 1);
                }
            }
            return (_nextDelimiterPos >= 0) || (_lastDelimiterPos < _lastCharIdx);
        }
    }

    /**
     * Returns the next statement.
     * 
     * @return The statement
     */
    public String getNextStatement()
    {
        String result = null;

        if (hasMoreStatements())
        {
            if (_nextDelimiterPos >= 0)
            {
                result            = _sql.substring(_lastDelimiterPos + 1, _nextDelimiterPos);
                _lastDelimiterPos = _nextDelimiterPos;
            }
            else
            {
                result    = _sql.substring(_lastDelimiterPos + 1);
                _finished = true;
            }
        }
        return result;
    }
}
