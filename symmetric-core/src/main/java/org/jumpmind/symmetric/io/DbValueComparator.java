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
package org.jumpmind.symmetric.io;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.sql.Types;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.List;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.math.NumberUtils;
import org.apache.commons.lang3.time.DateUtils;
import org.jumpmind.db.model.Column;
import org.jumpmind.db.model.TypeMap;
import org.jumpmind.symmetric.ISymmetricEngine;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DbValueComparator {
    final Logger log = LoggerFactory.getLogger(getClass());
    private ISymmetricEngine sourceEngine;
    private ISymmetricEngine targetEngine;
    private boolean stringIgnoreWhiteSpace = true;
    private boolean stringNullEqualsEmptyString = true;
    private List<SimpleDateFormat> dateFormats = new ArrayList<SimpleDateFormat>();
    private int numericScale = -1;
    private String dateTimeFormat;

    public DbValueComparator(ISymmetricEngine sourceEngine, ISymmetricEngine targetEngine) {
        this.sourceEngine = sourceEngine;
        this.targetEngine = targetEngine;
        initDateFormats();
    }

    protected void initDateFormats() {
        dateFormats.add(new SimpleDateFormat("MM-dd-yyyy HH:mm:ss:S"));
        dateFormats.add(new SimpleDateFormat("MM-dd-yyyy HH:mm:ss.S"));
    }

    public int compareValues(Column sourceColumn, Column targetColumn, String sourceValue, String targetValue) {
        if (sourceValue == null && targetValue == null) {
            return 0;
        }
        if (sourceColumn.isOfTextType()) {
            return compareText(sourceColumn, targetColumn, sourceValue, targetValue);
        } else if (sourceColumn.isOfNumericType()) {
            return compareNumeric(sourceColumn, targetColumn, sourceValue, targetValue);
        } else if (TypeMap.isDateTimeType(sourceColumn.getJdbcTypeCode())) {
            return compareDateTime(sourceColumn, targetColumn, sourceValue, targetValue);
        } else {
            return compareDefault(sourceColumn, targetColumn, sourceValue, targetValue);
        }
    }

    public int compareText(Column sourceColumn, Column targetColumn, String source, String target) {
        if (stringNullEqualsEmptyString) {
            if (source == null) {
                source = "";
            }
            if (target == null) {
                target = "";
            }
        }
        if (stringIgnoreWhiteSpace) {
            source = source != null ? source.trim() : null;
            target = target != null ? target.trim() : null;
        }
        if (source != null && target != null) {
            return source.compareTo(target);
        } else {
            return compareDefault(sourceColumn, targetColumn, source, target);
        }
    }

    public int compareNumeric(Column sourceColumn, Column targetColumn, String sourceValue, String targetValue) {
        if (StringUtils.isBlank(sourceValue) && StringUtils.isBlank(targetValue)) {
            return 0;
        }
        if (!StringUtils.isBlank(sourceValue) && StringUtils.isBlank(targetValue)) {
            return 1;
        }
        if (StringUtils.isBlank(sourceValue) && !StringUtils.isBlank(targetValue)) {
            return -1;
        }
        BigDecimal source = null;
        BigDecimal target = null;
        try {
            source = NumberUtils.createBigDecimal(sourceValue);
        } catch (NumberFormatException ex) {
            log.debug("Failed to parse [" + sourceValue + "]", ex);
        }
        try {
            target = NumberUtils.createBigDecimal(targetValue);
        } catch (NumberFormatException ex) {
            log.debug("Failed to parse [" + targetValue + "]", ex);
        }
        if (source != null && target != null) {
            if (numericScale >= 0) {
                source = source.setScale(numericScale, RoundingMode.HALF_UP);
                target = target.setScale(numericScale, RoundingMode.HALF_UP);
            }
            return source.compareTo(target);
        }
        return sourceValue.compareTo(targetValue);
    }

    public int compareDateTime(Column sourceColumn, Column targetColumn, String sourceValue, String targetValue) {
        if (sourceValue == null || targetValue == null) {
            return compareDefault(sourceColumn, targetColumn, sourceValue, targetValue);
        }
        Date sourceDate = parseDate(sourceEngine, sourceColumn, sourceValue);
        Date targetDate = parseDate(targetEngine, targetColumn, targetValue);
        if (sourceColumn.getJdbcTypeCode() != Types.DATE
                && targetColumn.getJdbcTypeCode() != Types.DATE) {
            if (dateTimeFormat != null) {
                String sourceDateFormatted = formatDateTime(sourceDate);
                String targetDateFormatted = formatDateTime(targetDate);
                return compareDefault(sourceColumn, targetColumn, sourceDateFormatted, targetDateFormatted);
            }
        } else { // if either column is a simple date, clear the time for comparison purposes.
            sourceDate = DateUtils.truncate(sourceDate, Calendar.DATE);
            targetDate = DateUtils.truncate(targetDate, Calendar.DATE);
        }
        return compareDefault(sourceColumn, targetColumn, sourceDate, targetDate);
    }

    public String formatDateTime(Date date) {
        SimpleDateFormat formatter = new SimpleDateFormat(dateTimeFormat);
        String formattedDate = formatter.format(date);
        return formattedDate;
    }

    @SuppressWarnings("unchecked")
    protected int compareDefault(Column sourceColumn, Column targetColumn, Object sourceValue, Object targetValue) {
        if (sourceValue == null && targetValue == null) {
            return 0;
        }
        if (sourceValue != null && targetValue == null) {
            return 1;
        }
        if (sourceValue == null && targetValue != null) {
            return -1;
        }
        if (sourceValue instanceof Comparable<?>) {
            return ((Comparable<Object>) sourceValue).compareTo(targetValue);
        } else if (sourceValue instanceof String) {
            return ((String) sourceValue).compareTo((String) targetValue);
        } else {
            if (sourceValue != null && sourceValue.equals(targetValue)) {
                return 0;
            } else {
                return 1;
            }
        }
    }

    protected Date parseDate(ISymmetricEngine engine, Column column, String value) {
        Date date = null;
        try {
            // Just because the source was a date doesn't mean the target column is actually a date type.
            date = engine.getDatabasePlatform().parseDate(column.getJdbcTypeCode(), value, false);
        } catch (Exception e) {
            for (SimpleDateFormat format : dateFormats) {
                try {
                    date = format.parse(value);
                    if (date != null) {
                        break;
                    }
                } catch (Exception e2) {
                    continue;
                }
            }
        }
        return date;
    }

    public int getNumericScale() {
        return numericScale;
    }

    public void setNumericScale(int numericScale) {
        this.numericScale = numericScale;
    }

    public String getDateTimeFormat() {
        return dateTimeFormat;
    }

    public void setDateTimeFormat(String format) {
        this.dateTimeFormat = format;
    }
}
