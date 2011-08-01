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


package org.jumpmind.symmetric.extract;

import java.io.IOException;
import java.io.Writer;

import org.jumpmind.symmetric.model.Data;
import org.jumpmind.symmetric.model.OutgoingBatch;

/**
 * Interface that is responsible for writing captured data to the transport format
 */
public interface IDataExtractor {

    public void init(Writer writer, DataExtractorContext context) throws IOException;

    public void begin(OutgoingBatch batch, Writer writer) throws IOException;

    public void preprocessTable(Data data, String routerId, Writer out, DataExtractorContext context) throws IOException;

    public void commit(OutgoingBatch batch, Writer writer) throws IOException;

    public void write(Writer writer, Data data, String routerId, DataExtractorContext context) throws IOException;

    /**
     * Give an opportunity to swap out the table name with a different one for backward compatibility
     * purposes.
     */
    public String getLegacyTableName(String currentTableName);
    
}