package org.jumpmind.symmetric.ddl.io.converters;

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

import org.apache.commons.codec.binary.Base64;

/**
 * Converts between a byte array and its Base64 encoded string representation (e.g. for use in XML).
 * 
 * @version $Revision: $
 */
public class ByteArrayBase64Converter implements SqlTypeConverter
{
    /**
     * {@inheritDoc}
     */
    public Object convertFromString(String textRep, int sqlTypeCode) throws ConversionException
    {
        try
        {
            return textRep == null ? null : Base64.decodeBase64(textRep.getBytes());
        }
        catch (Exception ex)
        {
            throw new ConversionException(ex);
        }
    }

    /**
     * {@inheritDoc}
     */
    public String convertToString(Object obj, int sqlTypeCode) throws ConversionException
    {
        try
        {
            return obj == null ? null : new String(Base64.encodeBase64((byte[])obj));
        }
        catch (Exception ex)
        {
            throw new ConversionException(ex);
        }
    }

}
