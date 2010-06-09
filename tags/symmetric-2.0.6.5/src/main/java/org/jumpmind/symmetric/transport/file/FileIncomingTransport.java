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
package org.jumpmind.symmetric.transport.file;

import java.io.BufferedReader;
import java.io.IOException;

import org.apache.commons.io.IOUtils;
import org.jumpmind.symmetric.io.ThresholdFileWriter;
import org.jumpmind.symmetric.transport.IIncomingTransport;

/**
 * An incoming stream that reads from a file.
 */
public class FileIncomingTransport implements IIncomingTransport {

    ThresholdFileWriter fileWriter;

    BufferedReader reader;

    public FileIncomingTransport(ThresholdFileWriter fileWriter) {
        this.fileWriter = fileWriter;
    }

    public void close() throws IOException {
        IOUtils.closeQuietly(reader);
        if (fileWriter != null) {
            fileWriter.delete();
        }
    }

    public boolean isOpen() {
        return reader != null;
    }

    public BufferedReader open() throws IOException {
        reader = new BufferedReader(fileWriter.getReader());
        return reader;
    }

}
