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
package org.jumpmind.db.sql;

import java.io.IOException;
import java.math.BigDecimal;
import java.sql.Blob;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Date;

import org.apache.commons.io.IOUtils;
import org.jumpmind.exception.IoException;
import org.jumpmind.exception.ParseException;
import org.jumpmind.util.FormatUtils;
import org.jumpmind.util.LinkedCaseInsensitiveMap;

public class Row extends LinkedCaseInsensitiveMap<Object> {

    private static final long serialVersionUID = 1L;

    public Row(int numberOfColumns) {
        super(numberOfColumns);
    }

    public Row(String columnName, Object value) {
        super(1);
        put(columnName, value);
    }

    public byte[] bytesValue() {
        Object obj = this.values().iterator().next();
        return toBytes(obj);
    }

    protected byte[] toBytes(Object obj) {
        if (obj != null) {
            if (obj instanceof byte[]) {
                return (byte[]) obj;
            } else if (obj instanceof Blob) {
                Blob blob = (Blob) obj;
                try {
                    return IOUtils.toByteArray(blob.getBinaryStream());
                } catch (IOException e) {
                    throw new IoException(e);
                } catch (SQLException e) {
                    throw new SqlException(e);
                }
            } else if (obj instanceof String) {
                return obj.toString().getBytes();
            } else {
                throw new IllegalStateException(String.format(
                        "Cannot translate a %s into a byte[]", obj.getClass().getName()));
            }
        } else {
            return null;
        }
    }

    public Number numberValue() {
        Object obj = this.values().iterator().next();
        if (obj != null) {
            if (obj instanceof Number) {
                return (Number) obj;
            } else {
                return new BigDecimal(obj.toString());
            }
        } else {
            return null;
        }
    }

    public Date dateValue() {
        Object obj = this.values().iterator().next();
        if (obj != null) {
            if (obj instanceof Date) {
                return (Date) obj;
            } else {
                return Timestamp.valueOf(obj.toString());
            }
        } else {
            return null;
        }
    }

    public String stringValue() {
        Object obj = this.values().iterator().next();
        if (obj != null) {
            return obj.toString();
        } else {
            return null;
        }
    }

    public String getString(String columnName) {
        return getString(columnName, true);
    }

    public byte[] getBytes(String columnName) {
        Object obj = get(columnName);
        return toBytes(obj);
    }

    public String getString(String columnName, boolean checkForColumn) {
        Object obj = this.get(columnName);
        if (obj != null) {
            return obj.toString();
        } else {
            if (checkForColumn) {
                checkForColumn(columnName);
            }
            return null;
        }
    }

    public int getInt(String columnName) {
        Object obj = this.get(columnName);
        if (obj instanceof Number) {
            return ((Number) obj).intValue();
        } else if (obj instanceof String) {
            return Integer.parseInt(obj.toString());
        } else {
            checkForColumn(columnName);
            return 0;
        }
    }

    public long getLong(String columnName) {
        Object obj = this.get(columnName);
        if (obj instanceof Number) {
            return ((Number) obj).longValue();
        } else if (obj instanceof String) {
            return Long.parseLong(obj.toString());
        } else {
            checkForColumn(columnName);
            return 0;
        }
    }

    public boolean getBoolean(String columnName) {
        Object obj = this.get(columnName);
        if ("1".equals(obj)) {
            return true;
        } else if (obj instanceof Number) {
            int value = ((Number) obj).intValue();
            return value > 0 ? true : false;
        } else if (obj instanceof Boolean) {
            return (Boolean) obj;
        } else if (obj instanceof String) {
            return Boolean.parseBoolean((String) obj);
        } else {
            checkForColumn(columnName);
            return false;
        }
    }

    public Time getTime(String columnName) {
        Object obj = this.get(columnName);
        if (obj instanceof Time) {
            return (Time) obj;
        } else {
            Date date = getDateTime(columnName);
            return new Time(date.getTime());
        }
    }

    public Date getDateTime(String columnName) {
        Object obj = this.get(columnName);
        if (obj instanceof Number) {
            long value = ((Number) obj).longValue();
            return new Date(value);
        } else if (obj instanceof Date) {
            return (Date) obj;
        } else if (obj instanceof String) {
            try {
                return getDate((String) obj, SqlConstants.TIMESTAMP_PATTERNS);
            } catch (ParseException ex) {
                // on xerial sqlite jdbc dates come back as longs
                return new Date(Long.parseLong((String) obj));
            }
        } else {
            checkForColumn(columnName);
            return null;
        }
    }

    protected void checkForColumn(String columnName) {
        if (!containsKey(columnName)) {
            throw new ColumnNotFoundException(columnName);
        }
    }

    final private java.util.Date getDate(String value, String[] pattern) {
        return FormatUtils.parseDate(value, pattern);
    }
    
    public Object[] toArray(String[] keys) {
        Object[] values = new Object[keys.length];
        for (int i = 0; i < keys.length; i++) {
            values[i] = get(keys[i]);
        }
        return values;
    }
}
