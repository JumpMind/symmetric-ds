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

import java.util.LinkedList;

import org.apache.commons.lang.StringUtils;
import org.apache.log4j.AppenderSkeleton;
import org.apache.log4j.spi.LoggingEvent;

public class BufferedLogAppender extends AppenderSkeleton {

    protected LinkedList<LoggingEvent> events = new LinkedList<LoggingEvent>();

    protected int size = 100;

    protected String filterText;

    @Override
    protected void append(LoggingEvent event) {
        boolean addEvent = true;
        if (filterText != null) {
            String message = (String) event.getMessage();
            addEvent = message.contains(filterText);
            addEvent |= event.getLoggerName().contains(filterText);
            Object mdc = event.getMDC("engineName");
            if (mdc != null) {
                addEvent |= mdc.toString().contains(filterText);
            }
        }
        if (addEvent) {
            events.addLast(event);
            if (events.size() > size) {
                events.removeFirst();
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
        while (events.size() > size) {
            events.remove(0);
        }
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

    public LinkedList<LoggingEvent> getEvents() {
        return events;
    }
}
