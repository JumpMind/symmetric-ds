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
package org.jumpmind.symmetric.io;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.Reader;
import java.io.StringReader;
import java.io.Writer;

import org.jumpmind.symmetric.util.AppUtils;

/**
 * Write to an internal buffer up until the threshold. When the threshold is
 * reached, flush the buffer to the file and write to the file from that point
 * forward.
 */
public class ThresholdFileWriter extends Writer {

    File file;
    
    String tempFileCategory;

    BufferedWriter fileWriter;

    StringBuilder buffer;

    long threshhold;

    /**
     * @param threshold The number of bytes at which to start writing to a file
     * @param file The file to write to after the threshold has been reached
     */
    public ThresholdFileWriter(long threshold, File file) {
        this.file = file;
        this.buffer = new StringBuilder();
        this.threshhold = threshold;
    }
    
    /**
     * @param threshold The number of bytes at which to start writing to a file
     * @param tempFileCategory uses {@link AppUtils#createTempFile(String)} with this argument as the parameter
     * @see AppUtils#createTempFile(String)
     */
    public ThresholdFileWriter(long threshold, String tempFileCategory) {
        this.tempFileCategory = tempFileCategory;
        this.buffer = new StringBuilder();
        this.threshhold = threshold;
    }

    @Override
    public void close() throws IOException {
        if (fileWriter != null) {
            fileWriter.close();
        }
    }

    @Override
    public void flush() throws IOException {
        if (fileWriter != null) {
            fileWriter.flush();
        }
    }

    @Override
    public void write(char[] cbuf, int off, int len) throws IOException {
        if (fileWriter != null) {
            fileWriter.write(cbuf, off, len);
        } else if (len + buffer.length() > threshhold) {
            if (file == null) {
                file = AppUtils.createTempFile(tempFileCategory == null ? "threshold.file.writer" : tempFileCategory);
            }
            fileWriter = new BufferedWriter(new FileWriter(file));
            fileWriter.write(buffer.toString());
            fileWriter.write(cbuf, off, len);
            fileWriter.flush();
        } else {
            buffer.append(new String(cbuf), off, len);
        }
    }

    public Reader getReader() throws IOException {
        if (fileWriter != null) {
            return new FileReader(file);
        } else {
            return new StringReader(buffer.toString());
        }
    }
    
    public void delete() {
        if (file != null && file.exists()) {
            file.delete();
        }
    }

}
