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

package org.jumpmind.symmetric.statistic;

import java.util.Map;


/**
 * This manager provides an API record statistics
 */
public interface IStatisticManager {    
    public void flush();

    public void incrementDataLoadedErrors(String channelId, long count);

    public void incrementDataBytesLoaded(String channelId, long count);
    
    public void incrementDataLoaded(String channelId, long count);

    public void incrementDataBytesSent(String channelId, long count);
    
    public void incrementDataSent(String channelId, long count);

    public void incrementDataEventInserted(String channelId, long count);

    public void incrementDataExtractedErrors(String channelId, long count);

    public void incrementDataBytesExtracted(String channelId, long count);
    
    public void incrementDataExtracted(String channelId, long count);

    public void setDataUnRouted(String channelId, long count);

    public void incrementDataRouted(String channelId, long count);
    
    public void incrementDataSentErrors(String channelId, long count);
    
    public void incrementRestart();
    
    public void incrementNodesPulled(long count);
    
    public void incrementNodesPushed(long count);
    
    public void incrementTotalNodesPulledTime(long count);
    
    public void incrementTotalNodesPushedTime(long count);
    
    public Map<String, ChannelStats> getWorkingChannelStats();
    
    
}