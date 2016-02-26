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
import java.util.Date;

import org.apache.commons.lang.math.NumberUtils;
import org.jumpmind.db.model.Column;
import org.jumpmind.db.model.TypeMap;
import org.jumpmind.symmetric.ISymmetricEngine;

public class DbValueComparator {

    private ISymmetricEngine sourceEngine;
    private ISymmetricEngine targetEngine;
    private boolean stringIgnoreWhiteSpace = true;
    private boolean stringNullEqualsEmptyString = true;

    public DbValueComparator(ISymmetricEngine sourceEngine, ISymmetricEngine targetEngine) {
        this.sourceEngine = sourceEngine;
        this.targetEngine = targetEngine;
    }

    @SuppressWarnings("unchecked")
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
        if (sourceValue != null && targetValue != null) {
            BigDecimal source = NumberUtils.createBigDecimal(sourceValue.toString());
            BigDecimal target = NumberUtils.createBigDecimal(targetValue.toString());
            return source.compareTo(target);
        } else {
            return compareDefault(sourceColumn, targetColumn, sourceValue, targetValue);            
        }
    }

    public int compareDateTime(Column sourceColumn, Column targetColumn, String sourceValue, String targetValue) {
        if (sourceValue == null || targetValue == null) {
            return compareDefault(sourceColumn, targetColumn, sourceValue, targetValue);
        }
        
        Date sourceDate = sourceEngine.getDatabasePlatform().parseDate(sourceColumn.getJdbcTypeCode(), sourceValue, false);
        Date targetDate = targetEngine.getDatabasePlatform().parseDate(targetColumn.getJdbcTypeCode(), targetValue, false);
        
        return compareDefault(sourceColumn, targetColumn, sourceDate, targetDate);
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
            return ((Comparable)sourceValue).compareTo(targetValue);            
        } else if (sourceValue instanceof String) {
            return ((String)sourceValue).compareTo((String)targetValue);
        } else {
            if (sourceValue.equals(targetValue)) {
                return 0;
            } else {
                return 1;
            }
        }
    }

}
