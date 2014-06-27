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
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import org.apache.commons.io.IOUtils;
import org.jumpmind.symmetric.model.ChannelMap;
import org.jumpmind.symmetric.service.IConfigurationService;
import org.jumpmind.symmetric.transport.IOutgoingWithResponseTransport;
import org.jumpmind.symmetric.transport.TransportUtils;

/**
 * 
 */
public class InternalOutgoingWithResponseTransport implements IOutgoingWithResponseTransport {

    BufferedWriter writer = null;

    BufferedReader reader = null;

    boolean open = true;

    InternalOutgoingWithResponseTransport(OutputStream pushOs, InputStream respIs) throws IOException {
        writer = TransportUtils.toWriter(pushOs);
        reader = TransportUtils.toReader(respIs);
    }

    public BufferedReader readResponse() throws IOException {
        IOUtils.closeQuietly(writer);
        return reader;
    }

    public void close() {
        IOUtils.closeQuietly(writer);
        IOUtils.closeQuietly(reader);
        open = false;
    }

    public boolean isOpen() {
        return open;
    }

    public BufferedWriter open() {
        return writer;
    }

    public ChannelMap getSuspendIgnoreChannelLists(IConfigurationService configurationService) {
        return configurationService.getSuspendIgnoreChannelLists();
    }
}