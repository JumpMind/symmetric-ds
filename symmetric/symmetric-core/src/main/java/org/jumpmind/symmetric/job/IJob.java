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
 * under the License. 
 */
package org.jumpmind.symmetric.job;

import java.util.Date;

public interface IJob {

    public void start();

    public boolean stop();

    public String getName();
    
    public String getClusterLockName();
    
    public boolean isClusterable();

    public void pause();

    public void unpause();

    public boolean isPaused();

    public boolean isStarted();
    
    public boolean isAutoStartConfigured();

    public long getLastExecutionTimeInMs();

    public Date getLastFinishTime();

    public boolean isRunning();

    public long getNumberOfRuns();

    public long getTotalExecutionTimeInMs();

    public long getAverageExecutionTimeInMs();

    public String getCronExpression();

    public long getTimeBetweenRunsInMs();
    
    public void setCronExpression(String cronExpression);
    
    public void setTimeBetweenRunsInMs(long timeBetweenRunsInMs);    
    
    public boolean invoke(boolean force);

}