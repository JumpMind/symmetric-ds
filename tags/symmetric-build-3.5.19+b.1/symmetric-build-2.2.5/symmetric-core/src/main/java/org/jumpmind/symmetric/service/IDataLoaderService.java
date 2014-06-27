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

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.List;

import org.jumpmind.symmetric.load.IBatchListener;
import org.jumpmind.symmetric.load.IColumnFilter;
import org.jumpmind.symmetric.load.IDataLoader;
import org.jumpmind.symmetric.load.IDataLoaderFilter;
import org.jumpmind.symmetric.load.IDataLoaderStatistics;
import org.jumpmind.symmetric.load.csv.CsvLoader;
import org.jumpmind.symmetric.model.Node;
import org.jumpmind.symmetric.model.RemoteNodeStatus;
import org.jumpmind.symmetric.transport.ITransportManager;

/**
 * This service provides an API to load data into a SymmetricDS node's
 * database from a transport
 */
public interface IDataLoaderService {
    
    public RemoteNodeStatus loadDataFromPull(Node remote) throws IOException;
    
    public void loadDataFromPull(Node remote, RemoteNodeStatus status) throws IOException;

    public void loadData(InputStream in, OutputStream out) throws IOException;

    /**
     * This is a convenience method for a client that might need to load CSV
     * formatted data using SymmetricDS's {@link IDataLoader}.
     * 
     * @param batchData
     *                Data string formatted for the configured loader (the only
     *                supported data loader today is the {@link CsvLoader})
     * @throws IOException
     */
    public IDataLoaderStatistics loadDataBatch(String batchData) throws IOException;

    public void addDataLoaderFilter(IDataLoaderFilter filter);

    public void setDataLoaderFilters(List<IDataLoaderFilter> filters);

    public void removeDataLoaderFilter(IDataLoaderFilter filter);

    public void setTransportManager(ITransportManager transportManager);

    public void addColumnFilter(String tableName, IColumnFilter filter);

    /**
     * Remove all instances of the filter and re-register under the tables passed in.
     */
    public void reRegisterColumnFilter(String[] tableNames, IColumnFilter filter);
    
    public void addBatchListener(IBatchListener listener);

    public IDataLoader openDataLoader(BufferedReader reader) throws IOException;
}