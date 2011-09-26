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


package org.jumpmind.symmetric.transport.handler;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import org.jumpmind.symmetric.service.IDataLoaderService;
import org.jumpmind.symmetric.statistic.IStatisticManager;
import org.jumpmind.symmetric.web.PushServlet;

/**
 * Handles data pushes from nodes.
 * 
 * @see PushServlet
 */
public class PushResourceHandler extends AbstractTransportResourceHandler {

    private IDataLoaderService dataLoaderService;

    private IStatisticManager statisticManager;
    
    public void push(InputStream inputStream, OutputStream outputStream) throws IOException {
        long ts = System.currentTimeMillis();
        try {
            getDataLoaderService().loadData(inputStream, outputStream);
        } finally {
            statisticManager.incrementNodesPushed(1);
            statisticManager.incrementTotalNodesPushedTime(System.currentTimeMillis() - ts);
        }
    }

    private IDataLoaderService getDataLoaderService() {
        return dataLoaderService;
    }

    public void setDataLoaderService(IDataLoaderService dataLoaderService) {
        this.dataLoaderService = dataLoaderService;
    }
    
    public void setStatisticManager(IStatisticManager statisticManager) {
        this.statisticManager = statisticManager;
    }

}