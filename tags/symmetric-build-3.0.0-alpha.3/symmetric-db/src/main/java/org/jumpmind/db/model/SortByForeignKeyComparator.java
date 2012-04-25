package org.jumpmind.db.model;

import java.util.Comparator;

import org.apache.commons.lang.StringUtils;

/**
 * Compares two tables and considers the first table to be less than the second
 * table if the first has a foreign key dependency on the second. If the second
 * has a foreign key dependency on the first, then the first is consider
 * greater. If no dependencies exist, then the tables are considered equal.
 */
public class SortByForeignKeyComparator implements Comparator<Table> {

    public int compare(Table table1, Table table2) {
        int returnValue = 0;
        ForeignKey[] fk1s = table1.getForeignKeys();
        if (fk1s != null) {
            for (ForeignKey fk1 : fk1s) {
                String fkTableName = fk1.getForeignTableName();
                if (StringUtils.equals(fkTableName, table2.getName())) {
                    returnValue--;
                }
            }
        }

        ForeignKey[] fk2s = table2.getForeignKeys();
        if (fk2s != null) {
            for (ForeignKey fk2 : fk2s) {
                String fkTableName = fk2.getForeignTableName();
                if (StringUtils.equals(fkTableName, table1.getName())) {
                    returnValue++;
                }
            }
        }

        return returnValue;
    }

}
