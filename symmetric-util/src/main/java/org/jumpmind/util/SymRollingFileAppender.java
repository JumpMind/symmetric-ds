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

import java.io.Serializable;
import java.io.UnsupportedEncodingException;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.TimeUnit;
import java.util.zip.CRC32;
import java.util.zip.Deflater;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.core.Appender;
import org.apache.logging.log4j.core.Core;
import org.apache.logging.log4j.core.Filter;
import org.apache.logging.log4j.core.Layout;
import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.core.appender.AbstractOutputStreamAppender;
import org.apache.logging.log4j.core.appender.rolling.DefaultRolloverStrategy;
import org.apache.logging.log4j.core.appender.rolling.DirectFileRolloverStrategy;
import org.apache.logging.log4j.core.appender.rolling.DirectWriteRolloverStrategy;
import org.apache.logging.log4j.core.appender.rolling.RollingFileManager;
import org.apache.logging.log4j.core.appender.rolling.RolloverStrategy;
import org.apache.logging.log4j.core.appender.rolling.TriggeringPolicy;
import org.apache.logging.log4j.core.config.Property;
import org.apache.logging.log4j.core.config.plugins.Plugin;
import org.apache.logging.log4j.core.config.plugins.PluginBuilderAttribute;
import org.apache.logging.log4j.core.config.plugins.PluginBuilderFactory;
import org.apache.logging.log4j.core.config.plugins.PluginElement;
import org.apache.logging.log4j.core.config.plugins.validation.constraints.Required;
import org.apache.logging.log4j.core.impl.Log4jLogEvent;
import org.apache.logging.log4j.core.net.Advertiser;
import org.apache.logging.log4j.message.SimpleMessage;
import org.apache.logging.log4j.status.StatusLogger;

@Plugin(name = "SymRollingFile", category = Core.CATEGORY_NAME, elementType = Appender.ELEMENT_TYPE, printObject = true)
public class SymRollingFileAppender extends AbstractOutputStreamAppender<RollingFileManager> {

    private String fileName;

    private String filePattern;

    private Object advertisement;

    private final Advertiser advertiser;

    private int historySize = 2048;

    private Map<String, String> loggedEventKeys;
    
    private long lastFileTime;

    private SymRollingFileAppender(final String name, final Layout<? extends Serializable> layout, final Filter filter,
            final RollingFileManager manager, final String fileName, final String filePattern,
            final boolean ignoreExceptions, final boolean immediateFlush, final Advertiser advertiser,
            final Property[] properties) {
        super(name, layout, filter, ignoreExceptions, immediateFlush, properties, manager);
        if (advertiser != null) {
            final Map<String, String> configuration = new HashMap<>(layout.getContentFormat());
            configuration.put("contentType", layout.getContentType());
            configuration.put("name", name);
            advertisement = advertiser.advertise(configuration);
        }
        this.fileName = fileName;
        this.filePattern = filePattern;
        this.advertiser = advertiser;
        this.loggedEventKeys = new LinkedHashMap<String, String>() {
            private static final long serialVersionUID = 1L;

            @Override
            protected boolean removeEldestEntry(Entry<String, String> eldest) {
                return (size() >= historySize);
            }
        };
    }

    @Override
    public synchronized void append(LogEvent event) {
        if (!isLoggerAtDebug(event)) {
            String key = toKey(event);
            if (key != null) {            
                if (loggedEventKeys.containsKey(key)) {
                    event = supressStackTrace(event, key);
                } else {
                    event = appendKey(event, key);
                    loggedEventKeys.put(key, null);                    
                }
            }
        }

        getManager().checkRollover(event);
        if (getManager().getFileTime() != lastFileTime) {
            loggedEventKeys.clear();
            lastFileTime = getManager().getFileTime();
        }
        super.append(event);
    }

    private boolean isLoggerAtDebug(LogEvent event) {
        if (event != null && event.getLevel() != null) {
            return event.getLevel().intLevel() >= Level.DEBUG.intLevel();
        }
        return false;
    }

    protected String toKey(LogEvent event) {
        if (event.getThrown() == null || event.getThrown().getStackTrace() == null) {
            return null;
        }

        try {
            StringBuilder buff = new StringBuilder(128);
            Throwable throwable = event.getThrown();
            buff.append(throwable.getClass().getSimpleName());
            if (throwable.getStackTrace().length == 0) {
                buff.append("-jvm-optimized");    
            }
            buff.append(":");
            buff.append(getThrowableHash(event.getThrown().getStackTrace(), event.getThrown().getMessage()));
            return buff.toString();
        } catch (Exception ex) {
            StatusLogger.getLogger().error("Failed to hash stack trace.", ex);
            return null;
        }
    }
    
    protected long getThrowableHash(StackTraceElement[] elements, String message) throws UnsupportedEncodingException {
        CRC32 crc = new CRC32();
        if (message != null) {
            crc.update(message.getBytes("UTF8"));
        }
        for (StackTraceElement element : elements) {
            crc.update((element.getClassName() + element.getMethodName() + element.getLineNumber()).getBytes("UTF8"));
        }
        return crc.getValue();
    }

    protected LogEvent appendKey(LogEvent event, String key) {
        String message = getMessageWithKey(event, key, ".init");
        LogEvent eventClone = new Log4jLogEvent.Builder(event).setMessage(new SimpleMessage(message)).build();
        return eventClone;
    }    

    protected LogEvent supressStackTrace(LogEvent event, String key) {
        String message = getMessageWithKey(event, key);
        LogEvent eventClone = new Log4jLogEvent.Builder(event).setMessage(new SimpleMessage(message)).setThrown(null).build();
        return eventClone;
    }

    protected String getMessageWithKey(LogEvent event, String key) {
        return getMessageWithKey(event, key, null);
    }

    protected String getMessageWithKey(LogEvent event, String key, String prefix) {
        StringBuilder buff = new StringBuilder(128);
        if (event.getMessage() != null) {
            buff.append(event.getMessage()).append(" ");
        }
        buff.append("StackTraceKey");
        if (prefix != null) {
            buff.append(prefix);
        }
        buff.append(" [").append(key).append("]");
        return buff.toString();
    }

    @Override
    public void start() {
        super.start();
        lastFileTime = getManager().getFileTime();
    }

    @Override
    public boolean stop(final long timeout, final TimeUnit timeUnit) {
        setStopping();
        final boolean stopped = super.stop(timeout, timeUnit, false);
        if (advertiser != null) {
            advertiser.unadvertise(advertisement);
        }
        setStopped();
        return stopped;
    }

    public int getHistorySize() {
        return historySize;
    }

    public void setHistorySize(int historySize) {
        this.historySize = historySize;
    }
    
    public String getFileName() {
        return fileName;
    }

    public String getFilePattern() {
        return filePattern;
    }

    public <T extends TriggeringPolicy> T getTriggeringPolicy() {
        return getManager().getTriggeringPolicy();
    }

    @PluginBuilderFactory
    public static <B extends Builder<B>> B newBuilder() {
        return new Builder<B>().asBuilder();
    }
    
    public static class Builder<B extends Builder<B>> extends AbstractOutputStreamAppender.Builder<B>
            implements org.apache.logging.log4j.core.util.Builder<SymRollingFileAppender> {

        @PluginBuilderAttribute
        private String fileName;

        @PluginBuilderAttribute
        @Required
        private String filePattern;

        @PluginBuilderAttribute
        private boolean append = true;

        @PluginBuilderAttribute
        private boolean locking;

        @PluginElement("Policy")
        @Required
        private TriggeringPolicy policy;

        @PluginElement("Strategy")
        private RolloverStrategy strategy;

        @PluginBuilderAttribute
        private boolean advertise;

        @PluginBuilderAttribute
        private String advertiseUri;

        @PluginBuilderAttribute
        private boolean createOnDemand;

        @PluginBuilderAttribute
        private String filePermissions;

        @PluginBuilderAttribute
        private String fileOwner;

        @PluginBuilderAttribute
        private String fileGroup;

        @Override
        public SymRollingFileAppender build() {
            final boolean isBufferedIo = isBufferedIo();
            final int bufferSize = getBufferSize();
            if (getName() == null) {
                LOGGER.error("RollingFileAppender '{}': No name provided.", getName());
                return null;
            }

            if (!isBufferedIo && bufferSize > 0) {
                LOGGER.warn("RollingFileAppender '{}': The bufferSize is set to {} but bufferedIO is not true",
                        getName(), bufferSize);
            }

            if (filePattern == null) {
                LOGGER.error("RollingFileAppender '{}': No file name pattern provided.", getName());
                return null;
            }

            if (policy == null) {
                LOGGER.error("RollingFileAppender '{}': No TriggeringPolicy provided.", getName());
                return null;
            }

            if (strategy == null) {
                if (fileName != null) {
                    strategy = DefaultRolloverStrategy.newBuilder()
                            .withCompressionLevelStr(String.valueOf(Deflater.DEFAULT_COMPRESSION))
                            .withConfig(getConfiguration()).build();
                } else {
                    strategy = DirectWriteRolloverStrategy.newBuilder()
                            .withCompressionLevelStr(String.valueOf(Deflater.DEFAULT_COMPRESSION))
                            .withConfig(getConfiguration()).build();
                }
            } else if (fileName == null && !(strategy instanceof DirectFileRolloverStrategy)) {
                LOGGER.error("RollingFileAppender '{}': When no file name is provided a DirectFilenameRolloverStrategy must be configured",
                        getName());
                return null;
            }

            final Layout<? extends Serializable> layout = getOrCreateLayout();
            final RollingFileManager manager = RollingFileManager.getFileManager(fileName, filePattern, append,
                    isBufferedIo, policy, strategy, advertiseUri, layout, bufferSize, isImmediateFlush(),
                    createOnDemand, filePermissions, fileOwner, fileGroup, getConfiguration());

            if (manager == null) {
                return null;
            }

            manager.initialize();

            return new SymRollingFileAppender(getName(), layout, getFilter(), manager, fileName, filePattern,
                    isIgnoreExceptions(), isImmediateFlush(), advertise ? getConfiguration().getAdvertiser() : null,
                    getPropertyArray());
        }

        public String getAdvertiseUri() {
            return advertiseUri;
        }

        public String getFileName() {
            return fileName;
        }

        public boolean isAdvertise() {
            return advertise;
        }

        public boolean isAppend() {
            return append;
        }

        public boolean isCreateOnDemand() {
            return createOnDemand;
        }

        public boolean isLocking() {
            return locking;
        }

        public String getFilePermissions() {
            return filePermissions;
        }

        public String getFileOwner() {
            return fileOwner;
        }

        public String getFileGroup() {
            return fileGroup;
        }

        public B withAdvertise(final boolean advertise) {
            this.advertise = advertise;
            return asBuilder();
        }

        public B withAdvertiseUri(final String advertiseUri) {
            this.advertiseUri = advertiseUri;
            return asBuilder();
        }

        public B withAppend(final boolean append) {
            this.append = append;
            return asBuilder();
        }

        public B withFileName(final String fileName) {
            this.fileName = fileName;
            return asBuilder();
        }

        public B withCreateOnDemand(final boolean createOnDemand) {
            this.createOnDemand = createOnDemand;
            return asBuilder();
        }

        public B withLocking(final boolean locking) {
            this.locking = locking;
            return asBuilder();
        }

        public String getFilePattern() {
            return filePattern;
        }

        public TriggeringPolicy getPolicy() {
            return policy;
        }

        public RolloverStrategy getStrategy() {
            return strategy;
        }

        public B withFilePattern(final String filePattern) {
            this.filePattern = filePattern;
            return asBuilder();
        }

        public B withPolicy(final TriggeringPolicy policy) {
            this.policy = policy;
            return asBuilder();
        }

        public B withStrategy(final RolloverStrategy strategy) {
            this.strategy = strategy;
            return asBuilder();
        }

        public B withFilePermissions(final String filePermissions) {
            this.filePermissions = filePermissions;
            return asBuilder();
        }

        public B withFileOwner(final String fileOwner) {
            this.fileOwner = fileOwner;
            return asBuilder();
        }

        public B withFileGroup(final String fileGroup) {
            this.fileGroup = fileGroup;
            return asBuilder();
        }
    }

}
