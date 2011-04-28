/*
 * Licensed to JumpMind Inc under one or more contributor 
 * license agreements.  See the NOTICE file distributed
 * with this work for additional information regarding 
 * copyright ownership.  JumpMind Inc licenses this file
 * to you under the GNU Lesser General Public License (the
 * "License"); you may not use this file except in compliance
 * with the License. 
 * 
 * You should have received a copy of the GNU Lesser General Public
 * License along with this library; if not, see           
 * <http://www.gnu.org/licenses/>.
 * 
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.  */

package org.jumpmind.symmetric.io;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.StringReader;
import java.io.Writer;

import org.jumpmind.symmetric.util.AppUtils;

/**
 * Write to an internal buffer up until the threshold. When the threshold is
 * reached, flush the buffer to the file and write to the file from that point
 * forward.
 */
public class ThresholdFileWriter extends Writer {

    private File file;
    
    private String tempFileCategory;

    private BufferedWriter fileWriter;

    private StringBuilder buffer;

    private long threshhold;

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
    
    public File getFile() {
        return file;
    }
    
    public void setFile(File file) {
        this.file = file;
    }

    @Override
    public void close() throws IOException {
        if (fileWriter != null) {
            fileWriter.close();
            fileWriter = null;            
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
            fileWriter = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(file), "UTF-8"));
            fileWriter.write(buffer.toString());
            fileWriter.write(cbuf, off, len);
            fileWriter.flush();
        } else {
            buffer.append(new String(cbuf), off, len);
        }
    }

    public BufferedReader getReader() throws IOException {
        if (file != null && file.exists()) {
            return new BufferedReader(new InputStreamReader(new FileInputStream(file), "UTF-8"));
        } else {
            return new BufferedReader(new StringReader(buffer.toString()));
        }
    }
    
    public void reset() {
        this.file = null;
        this.fileWriter =  null;
        buffer.setLength(0);
    }
    
    public void delete() {
        if (file != null && file.exists()) {
            file.delete();
        }
        file = null;
        buffer.setLength(0);
    }

}