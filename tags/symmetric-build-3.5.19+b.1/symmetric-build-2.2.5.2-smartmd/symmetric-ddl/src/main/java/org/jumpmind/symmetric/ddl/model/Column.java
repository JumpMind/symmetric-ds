package org.jumpmind.symmetric.ddl.model;

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

import java.io.Serializable;
import java.math.BigDecimal;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;

import org.apache.commons.beanutils.ConvertUtils;
import org.apache.commons.lang.builder.EqualsBuilder;
import org.apache.commons.lang.builder.HashCodeBuilder;
import org.jumpmind.symmetric.ddl.util.Jdbc3Utils;

/**
 * Represents a column in the database model.
 * 
 * @version $Revision: 463305 $
 */
public class Column implements Cloneable, Serializable
{
    /** Unique ID for serialization purposes. */
    private static final long serialVersionUID = -6226348998874210093L;

    /** The name of the column. */
    private String _name;
    /** The java name of the column (optional and unused by DdlUtils, for Torque compatibility). */
    private String _javaName;
    /** The column's description. */
    private String _description;
    /** Whether the column is a primary key column. */
    private boolean _primaryKey;
    /** Whether the column is required, ie. it must not contain <code>NULL</code>. */
    private boolean _required;
    /** Whether the column's value is incremented automatically. */
    private boolean _autoIncrement;
    /** The JDBC type code, one of the constants in {@link java.sql.Types}. */
    private int _typeCode;
    /** The name of the JDBC type. */
    private String _type;
    /** The size of the column for JDBC types that require/support this. */
    private String _size;
    /** The size of the column for JDBC types that require/support this. */
    private Integer _sizeAsInt;
    /** The scale of the column for JDBC types that require/support this. */
    private int _scale;
    /** The default value. */
    private String _defaultValue;
    
    private String jdbcTypeName;

    /**
     * Returns the name of the column.
     * 
     * @return The name
     */
    public String getName()
    {
        return _name;
    }

    /**
     * Sets the name of the column.
     * 
     * @param name The name
     */
    public void setName(String name)
    {
        _name = name;
    }

    /**
     * Returns the java name of the column. This property is unused by DdlUtils and only
     * for Torque compatibility.
     * 
     * @return The java name
     */
    public String getJavaName()
    {
        return _javaName;
    }

    /**
     * Sets the java name of the column. This property is unused by DdlUtils and only
     * for Torque compatibility.
     * 
     * @param javaName The java name
     */
    public void setJavaName(String javaName)
    {
        _javaName = javaName;
    }

    /**
     * Returns the description of the column.
     *
     * @return The description
     */
    public String getDescription()
    {
        return _description;
    }

    /**
     * Sets the description of the column.
     *
     * @param description The description
     */
    public void setDescription(String description)
    {
        _description = description;
    }

    /**
     * Determines whether this column is a primary key column.
     * 
     * @return <code>true</code> if this column is a primary key column
     */
    public boolean isPrimaryKey()
    {
        return _primaryKey;
    }

    /**
     * Specifies whether this column is a primary key column.
     * 
     * @param primaryKey <code>true</code> if this column is a primary key column
     */
    public void setPrimaryKey(boolean primaryKey)
    {
        _primaryKey = primaryKey;
    }

    /**
     * Determines whether this column is a required column, ie. that it is not allowed
     * to contain <code>NULL</code> values.
     * 
     * @return <code>true</code> if this column is a required column
     */
    public boolean isRequired()
    {
        return _required;
    }

    /**
     * Specifies whether this column is a required column, ie. that it is not allowed
     * to contain <code>NULL</code> values.
     * 
     * @param required <code>true</code> if this column is a required column
     */
    public void setRequired(boolean required)
    {
        _required = required;
    }

    /**
     * Determines whether this column is an auto-increment column.
     * 
     * @return <code>true</code> if this column is an auto-increment column
     */
    public boolean isAutoIncrement()
    {
        return _autoIncrement;
    }

    /**
     * Specifies whether this column is an auto-increment column.
     * 
     * @param autoIncrement <code>true</code> if this column is an auto-increment column
     */
    public void setAutoIncrement(boolean autoIncrement)
    {
        _autoIncrement = autoIncrement;
    }

    /**
     * Returns the code (one of the constants in {@link java.sql.Types}) of the
     * JDBC type of the column.
     * 
     * @return The type code
     */
    public int getTypeCode()
    {
        return _typeCode;
    }

    /**
     * Sets the code (one of the constants in {@link java.sql.Types}) of the
     * JDBC type of the column. 
     * 
     * @param typeCode The type code
     */
    public void setTypeCode(int typeCode)
    {
        _type = TypeMap.getJdbcTypeName(typeCode);
        if (_type == null)
        {
            throw new ModelException("Unknown JDBC type code "+typeCode);
        }
        _typeCode = typeCode;
    }

    /**
     * Returns the JDBC type of the column.
     * 
     * @return The type
     */
    public String getType()
    {
        return _type;
    }

    /**
     * Sets the JDBC type of the column.
     *
     * @param type The type
     */
    public void setType(String type)
    {
        Integer typeCode = TypeMap.getJdbcTypeCode(type);

        if (typeCode == null)
        {
            throw new ModelException("Unknown JDBC type "+type);
        }
        else
        {
            _typeCode = typeCode.intValue();
            // we get the corresponding string value from the TypeMap in order
            // to detect extension types which we don't want in the model
            _type = TypeMap.getJdbcTypeName(_typeCode);
        }
    }

    /**
     * Determines whether this column is of a numeric type.
     * 
     * @return <code>true</code> if this column is of a numeric type
     */
    public boolean isOfNumericType()
    {
        return TypeMap.isNumericType(getTypeCode());
    }

    /**
     * Determines whether this column is of a text type.
     * 
     * @return <code>true</code> if this column is of a text type
     */
    public boolean isOfTextType()
    {
        return TypeMap.isTextType(getTypeCode());
    }

    /**
     * Determines whether this column is of a binary type.
     * 
     * @return <code>true</code> if this column is of a binary type
     */
    public boolean isOfBinaryType()
    {
        return TypeMap.isBinaryType(getTypeCode());
    }

    /**
     * Determines whether this column is of a special type.
     * 
     * @return <code>true</code> if this column is of a special type
     */
    public boolean isOfSpecialType()
    {
        return TypeMap.isSpecialType(getTypeCode());
    }
    
    /**
     * Returns the size of the column.
     * 
     * @return The size
     */
    public String getSize()
    {
        return _size;
    }

    /**
     * Returns the size of the column as an integer.
     * 
     * @return The size as an integer
     */
    public int getSizeAsInt()
    {
        return _sizeAsInt == null ? 0 : _sizeAsInt.intValue();
    }

    /**
     * Sets the size of the column. This is either a simple integer value or
     * a comma-separated pair of integer values specifying the size and scale.
     * 
     * @param size The size
     */
    public void setSize(String size)
    {
        if (size != null)
        {
            int pos = size.indexOf(",");

            _size  = size;
            if (pos < 0)
            {
                _scale     = 0;
                _sizeAsInt = new Integer(_size);
            }
            else
            {
                _sizeAsInt = new Integer(size.substring(0, pos));
                _scale     = Integer.parseInt(size.substring(pos + 1));
            }
        }
        else
        {
            _size      = null;
            _sizeAsInt = null;
            _scale     = 0;
        }
    }
    
    /**
     * Returns the scale of the column.
     * 
     * @return The scale
     */
    public int getScale()
    {
        return _scale;
    }

    /**
     * Sets the scale of the column.
     *
     * @param scale The scale
     */
    public void setScale(int scale)
    {
        setSizeAndScale(getSizeAsInt(), scale);
    }

    /**
     * Sets both the size and scale.
     * 
     * @param size  The size
     * @param scale The scale
     */
    public void setSizeAndScale(int size, int scale)
    {
        _sizeAsInt = new Integer(size);
        _scale     = scale;
        _size      = String.valueOf(size);
        if (scale > 0)
        {
            _size += "," + _scale;
        }
    }
    
    /**
     * Returns the precision radix of the column.
     * 
     * @return The precision radix
     */
    public int getPrecisionRadix()
    {
        return getSizeAsInt();
    }

    /**
     * Sets the precision radix of the column.
     * 
     * @param precisionRadix The precision radix
     */
    public void setPrecisionRadix(int precisionRadix)
    {
        _sizeAsInt = new Integer(precisionRadix);
        _size      = String.valueOf(precisionRadix);
    }

    /**
     * Returns the default value of the column.
     * 
     * @return The default value
     */
    public String getDefaultValue()
    {
        return _defaultValue;
    }

    /**
     * Tries to parse the default value of the column and returns it as an object of the
     * corresponding java type. If the value could not be parsed, then the original
     * definition is returned.
     * 
     * @return The parsed default value
     */
    public Object getParsedDefaultValue()
    {
        if ((_defaultValue != null) && (_defaultValue.length() > 0))
        {
            try
            {
                switch (_typeCode)
                {
                    case Types.TINYINT:
                    case Types.SMALLINT:
                        return new Short(_defaultValue);
                    case Types.INTEGER:
                        return new Integer(_defaultValue);
                    case Types.BIGINT:
                        return new Long(_defaultValue);
                    case Types.DECIMAL:
                    case Types.NUMERIC:
                        return new BigDecimal(_defaultValue);
                    case Types.REAL:
                        return new Float(_defaultValue);
                    case Types.DOUBLE:
                    case Types.FLOAT:
                        return new Double(_defaultValue);
                    case Types.DATE:
                        return Date.valueOf(_defaultValue);
                    case Types.TIME:
                        return Time.valueOf(_defaultValue);
                    case Types.TIMESTAMP:
                        return Timestamp.valueOf(_defaultValue);
                    case Types.BIT:
                        return ConvertUtils.convert(_defaultValue, Boolean.class);
                    default:
                        if (Jdbc3Utils.supportsJava14JdbcTypes() &&
                            (_typeCode == Jdbc3Utils.determineBooleanTypeCode()))
                        {
                            return ConvertUtils.convert(_defaultValue, Boolean.class);
                        }
                        break;
                }
            }
            catch (NumberFormatException ex)
            {
                return null;
            }
            catch (IllegalArgumentException ex)
            {
                return null;
            }
        }
        return _defaultValue;
    }

    /**
     * Sets the default value of the column. Note that this expression will be used
     * within quotation marks when generating the column, and thus is subject to
     * the conversion rules of the target database.
     * 
     * @param defaultValue The default value
     */
    public void setDefaultValue(String defaultValue)
    {
        _defaultValue = defaultValue;
    }

    /**
     * {@inheritDoc}
     */
    public Object clone() throws CloneNotSupportedException
    {
        Column result = (Column)super.clone();

        result._name            = _name;
        result._javaName        = _javaName;
        result._primaryKey      = _primaryKey;
        result._required        = _required;
        result._autoIncrement   = _autoIncrement;
        result._typeCode        = _typeCode;
        result._type            = _type;
        result._size            = _size;
        result._defaultValue    = _defaultValue;
        result._scale           = _scale;
        result._size            = _size;
        result._sizeAsInt       = _sizeAsInt;

        return result;
    }

    /**
     * {@inheritDoc}
     */
    public boolean equals(Object obj)
    {
        if (obj instanceof Column)
        {
            Column        other      = (Column)obj;
            EqualsBuilder comparator = new EqualsBuilder();

            // Note that this compares case sensitive
            comparator.append(_name,                   other._name);
            comparator.append(_primaryKey,             other._primaryKey);
            comparator.append(_required,               other._required);
            comparator.append(_autoIncrement,          other._autoIncrement);
            comparator.append(_typeCode,               other._typeCode);
            comparator.append(getParsedDefaultValue(), other.getParsedDefaultValue());

            // comparing the size makes only sense for types where it is relevant
            if ((_typeCode == Types.NUMERIC) || (_typeCode == Types.DECIMAL))
            {
                comparator.append(_size,  other._size);
                comparator.append(_scale, other._scale);
            }
            else if ((_typeCode == Types.CHAR) || (_typeCode == Types.VARCHAR) ||
                     (_typeCode == Types.BINARY) || (_typeCode == Types.VARBINARY))
            {
                comparator.append(_size, other._size);
            }

            return comparator.isEquals();
        }
        else
        {
            return false;
        }
    }

    /**
     * {@inheritDoc}
     */
    public int hashCode()
    {
        HashCodeBuilder builder = new HashCodeBuilder(17, 37);

        builder.append(_name);
        builder.append(_primaryKey);
        builder.append(_required);
        builder.append(_autoIncrement);
        builder.append(_typeCode);
        builder.append(_type);
        builder.append(_scale);
        builder.append(getParsedDefaultValue());
        if (!TypeMap.isNumericType(_typeCode))
        {
            builder.append(_size);
        }
        
        return builder.toHashCode();
    }

    /**
     * {@inheritDoc}
     */
    public String toString()
    {
        StringBuffer result = new StringBuffer();

        result.append("Column [name=");
        result.append(getName());
        result.append("; type=");
        result.append(getType());
        result.append("]");

        return result.toString();
    }

    /**
     * Returns a verbose string representation of this column.
     * 
     * @return The string representation
     */
    public String toVerboseString()
    {
        StringBuffer result = new StringBuffer();

        result.append("Column [name=");
        result.append(getName());
        result.append("; javaName=");
        result.append(getJavaName());
        result.append("; type=");
        result.append(getType());
        result.append("; typeCode=");
        result.append(getTypeCode());
        result.append("; size=");
        result.append(getSize());
        result.append("; required=");
        result.append(isRequired());
        result.append("; primaryKey=");
        result.append(isPrimaryKey());
        result.append("; autoIncrement=");
        result.append(isAutoIncrement());
        result.append("; defaultValue=");
        result.append(getDefaultValue());
        result.append("; precisionRadix=");
        result.append(getPrecisionRadix());
        result.append("; scale=");
        result.append(getScale());
        result.append("]");

        return result.toString();
    }
    
    public void setJdbcTypeName(String jdbcTypeName) {
        this.jdbcTypeName = jdbcTypeName;
    }
    
    public String getJdbcTypeName() {
        return jdbcTypeName;
    }
}
