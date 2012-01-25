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

import java.io.BufferedWriter;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.UnsupportedEncodingException;

import org.apache.commons.io.IOUtils;
import org.jumpmind.symmetric.common.Constants;
import org.jumpmind.symmetric.model.ChannelMap;
import org.jumpmind.symmetric.service.IConfigurationService;
import org.jumpmind.symmetric.transport.IOutgoingTransport;

/**
 * 
 */
public class InternalOutgoingTransport implements IOutgoingTransport {

    BufferedWriter writer = null;

    ChannelMap map = null;

    boolean open = true;

    public InternalOutgoingTransport(OutputStream pushOs) throws UnsupportedEncodingException {
        this(pushOs, new ChannelMap());
    }

    public InternalOutgoingTransport(OutputStream pushOs, ChannelMap map) throws UnsupportedEncodingException {
        writer = new BufferedWriter(new OutputStreamWriter(pushOs, Constants.ENCODING));
        this.map = map;
    }

    public InternalOutgoingTransport(BufferedWriter writer) {
        this.writer = writer;
    }

    public void close() {
        IOUtils.closeQuietly(writer);
        open = false;
    }

    public boolean isOpen() {
        return open;
    }

    public BufferedWriter open() {
        return writer;
    }

    public ChannelMap getSuspendIgnoreChannelLists(IConfigurationService configurationService) {
        return map;
    }

}