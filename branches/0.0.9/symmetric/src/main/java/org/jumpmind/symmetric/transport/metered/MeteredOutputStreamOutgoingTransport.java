/*
 * SymmetricDS is an open source database synchronization solution.
 *   
 * Copyright (C) Chris Henson <chenson42@users.sourceforge.net>
 *
 * This library is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License as published by the Free Software Foundation; either
 * version 3 of the License, or (at your option) any later version.
 *
 * This library is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this library; if not, see
 * <http://www.gnu.org/licenses/>.
 */

package org.jumpmind.symmetric.transport.metered;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;

import org.apache.commons.io.IOUtils;
import org.jumpmind.symmetric.transport.IOutgoingTransport;
import org.jumpmind.symmetric.util.MeteredOutputStream;

public class MeteredOutputStreamOutgoingTransport implements IOutgoingTransport
{
    private BufferedWriter writer;
    private MeteredOutputStream stream;

    public MeteredOutputStreamOutgoingTransport(OutputStream stream, long rate) 
    {
        this.stream = new MeteredOutputStream(stream, rate);
    }
    
    public void close() throws IOException
    {
        IOUtils.closeQuietly(writer);
        writer = null;
    }

    public boolean isOpen()
    {
        return writer != null;
    }

    public BufferedWriter open() throws IOException
    {
        writer = new BufferedWriter(new OutputStreamWriter(stream)); 
        return writer; 
    }

}
