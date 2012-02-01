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

    public InternalIncomingTransport(InputStream pullIs) throws IOException {
        reader = TransportUtils.toReader(pullIs);
    }

    public void close() throws IOException {
        IOUtils.closeQuietly(reader);
        reader = null;
    }

    public boolean isOpen() {
        return reader != null;
    }

    public BufferedReader open() throws IOException {
        return reader;
    }
    
    public String getRedirectionUrl() {
        return null;
    }
    
    public String getUrl() {
        return "";
    }

}