package org.jumpmind.symmetric.core.model;

/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

import java.util.ArrayList;

import org.jumpmind.symmetric.core.common.EqualsBuilder;

/**
 * Provides compatibility with Torque-style xml with separate &lt;index&gt; and
 * &lt;unique&gt; tags, but adds no functionality. All indexes are treated the
 * same by the Table.
 */
public class UniqueIndex extends Index {
    /** Unique ID for serialization purposes. */
    private static final long serialVersionUID = -4097003126550294993L;

    public UniqueIndex() {
    }

    public UniqueIndex(String name) {
        setName(name);
    }

    /**
     * {@inheritDoc}
     */
    public boolean isUnique() {
        return true;
    }

    /**
     * {@inheritDoc}
     */
    @SuppressWarnings("unchecked")
    public Object clone() throws CloneNotSupportedException {
        UniqueIndex result = new UniqueIndex();
        result.name = name;
        result.columns = (ArrayList<IndexColumn>) columns.clone();
        return result;
    }

    /**
     * {@inheritDoc}
     */
    public boolean equals(Object obj) {
        if (obj instanceof UniqueIndex) {
            UniqueIndex other = (UniqueIndex) obj;

            return new EqualsBuilder().append(name, other.name).append(columns, other.columns)
                    .isEquals();
        } else {
            return false;
        }
    }

    /**
     * {@inheritDoc}
     */
    public boolean equalsIgnoreCase(Index other) {
        if (other instanceof UniqueIndex) {
            UniqueIndex otherIndex = (UniqueIndex) other;

            boolean checkName = (name != null) && (name.length() > 0)
                    && (otherIndex.name != null) && (otherIndex.name.length() > 0);

            if ((!checkName || name.equalsIgnoreCase(otherIndex.name))
                    && (getColumnCount() == otherIndex.getColumnCount())) {
                for (int idx = 0; idx < getColumnCount(); idx++) {
                    if (!getColumn(idx).equalsIgnoreCase(otherIndex.getColumn(idx))) {
                        return false;
                    }
                }
                return true;
            }
        }
        return false;
    }

    /**
     * {@inheritDoc}
     */
    public int hashCode() {
        return columns.hashCode();
    }

    /**
     * {@inheritDoc}
     */
    public String toString() {
        StringBuffer result = new StringBuffer();

        result.append("Unique index [name=");
        result.append(getName());
        result.append("; ");
        result.append(getColumnCount());
        result.append(" columns]");

        return result.toString();
    }

    /**
     * {@inheritDoc}
     */
    public String toVerboseString() {
        StringBuffer result = new StringBuffer();

        result.append("Unique index [");
        result.append(getName());
        result.append("] columns:");
        for (int idx = 0; idx < getColumnCount(); idx++) {
            result.append(" ");
            result.append(getColumn(idx).toString());
        }

        return result.toString();
    }
}
