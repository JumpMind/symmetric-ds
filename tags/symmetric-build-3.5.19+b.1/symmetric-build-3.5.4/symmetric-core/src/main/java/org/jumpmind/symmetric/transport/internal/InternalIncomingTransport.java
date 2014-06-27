/**
 * Licensed to JumpMind Inc under one or more contributor
 * license agreements.  See the NOTICE file distributed
 * with this work for additional information regarding
 * copyright ownership.  JumpMind Inc licenses this file
 * to you under the GNU General Public License, version 3.0 (GPLv3)
 * (the "License"); you may not use this file except in compliance
 * with the License.
 *
 * You should have received a copy of the GNU General Public License,
 * version 3.0 (GPLv3) along with this library; if not, see
 * <http://www.gnu.org/licenses/>.
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.jumpmind.symmetric.transport.internal;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;

import org.apache.commons.io.IOUtils;
import org.jumpmind.symmetric.transport.IIncomingTransport;
import org.jumpmind.symmetric.transport.TransportUtils;

/**
 * Wraps an incoming stream that comes from memory
 */
public class InternalIncomingTransport implements IIncomingTransport {

    BufferedReader reader = null;
    
    InputStream is;

    public InternalIncomingTransport(InputStream is)  {
        this.is = is;
        this.reader = TransportUtils.toReader(is);
    }
    
    public InternalIncomingTransport(BufferedReader reader)  {
        this.reader = reader;
    }    

    public void close() {
        if (reader != null) {
            IOUtils.closeQuietly(reader);
            reader = null;
        }

        if (is != null) {
            IOUtils.closeQuietly(is);
            is = null;
        }
    }

    public boolean isOpen() {
        return reader != null || is != null;
    }

    public BufferedReader openReader() throws IOException {
        return reader;
    }
    
    public InputStream openStream() throws IOException {
        return is;
    }
    
    public String getRedirectionUrl() {
        return null;
    }
    
    public String getUrl() {
        return "";
    }

}