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

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.Reader;

import org.jumpmind.symmetric.io.ThresholdFileWriter;
import org.jumpmind.symmetric.transport.IOutgoingTransport;

/**
 * An outgoing transport that writes to the file system
 */
public class FileOutgoingTransport implements IOutgoingTransport {

    BufferedWriter out;

    ThresholdFileWriter fileWriter;

    public FileOutgoingTransport(long threshold, String tempFileCategory) throws IOException {
        this.fileWriter = new ThresholdFileWriter(threshold, tempFileCategory);
    }

    public BufferedWriter open() throws IOException {
        out = new BufferedWriter(fileWriter);
        return out;
    }

    public boolean isOpen() {
        return out != null;
    }

    public void close() throws IOException {
        out.close();
        out = null;
    }

    public Reader getReader() throws IOException {
        return this.fileWriter.getReader();
    }

    public void delete() {
        if (fileWriter != null) {
            fileWriter.delete();
        }
    }

}
