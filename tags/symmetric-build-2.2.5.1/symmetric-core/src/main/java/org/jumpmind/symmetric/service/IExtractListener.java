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


package org.jumpmind.symmetric.service;

import java.io.IOException;

import org.jumpmind.symmetric.model.Data;
import org.jumpmind.symmetric.model.OutgoingBatch;

/**
 * 
 */
public interface IExtractListener {

    public void init() throws IOException;

    /**
     * Gets called when the start of a batch is found.
     */
    public void startBatch(OutgoingBatch batch) throws IOException;

    /**
     * Gets called only if batch has finished and is successful.
     */
    public void endBatch(OutgoingBatch batch) throws IOException;

    public void dataExtracted(Data data, String routerId) throws IOException;

    public void done() throws IOException;
}