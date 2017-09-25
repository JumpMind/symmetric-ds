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
package org.jumpmind.util;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.apache.log4j.AppenderSkeleton;
import org.apache.log4j.spi.LoggingEvent;

public class BufferedLogAppender extends AppenderSkeleton {

    protected Map<String, List<LoggingEvent>> events = Collections.synchronizedMap(new LinkedHashMap<String,List<LoggingEvent>>());

    protected int size = 100;

    protected String filterText;

    @Override
    protected void append(LoggingEvent event) {
        Object mdc = event.getMDC("engineName");
        boolean addEvent = mdc != null;
        if (addEvent && filterText != null) {
            String message = (String) event.getMessage();
            addEvent = message.contains(filterText);
            addEvent |= event.getLoggerName().contains(filterText);
            if (mdc != null) {
                addEvent |= mdc.toString().contains(filterText);
            }
        }
        if (addEvent) {
            String engineName = mdc.toString();
            List<LoggingEvent> list = events.get(engineName);
            if (list == null) {
                list = Collections.synchronizedList(new ArrayList<LoggingEvent>(size));
                events.put(engineName, list);
            }
            
            list.add(event);
            if (list.size() > size) {
                list.remove(0);
            }
        }
    }

    public void close() {
    }

    public boolean requiresLayout() {
        return false;
    }

    public void setSize(int size) {
        this.size = size;
    }

    public int getSize() {
        return size;
    }

    public void setFilterText(String filterText) {
        if (StringUtils.isBlank(filterText)) {
            this.filterText = null;
        } else {
            this.filterText = filterText;
        }
    }

    public String getFilterText() {
        return filterText;
    }

    public List<LoggingEvent> getEvents(String engineName) {
        return events.get(engineName);
    }
}
