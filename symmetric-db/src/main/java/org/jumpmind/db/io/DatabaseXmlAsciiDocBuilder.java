package org.jumpmind.db.io;

import static org.apache.commons.lang3.StringUtils.isNotBlank;

import java.io.File;
import java.io.FileWriter;
import java.io.PrintWriter;

import org.jumpmind.db.model.Column;
import org.jumpmind.db.model.Database;
import org.jumpmind.db.model.ForeignKey;
import org.jumpmind.db.model.Reference;
import org.jumpmind.db.model.Table;

public class DatabaseXmlAsciiDocBuilder {

    public static void main(String[] args) throws Exception {
        if (args.length == 0) {
            System.err.println("Usage: <input_xml_file> <output_asciidoc_file>");
            System.exit(-1);
        }

        Database db = DatabaseXmlUtil.read(new File(args[0]));
        PrintWriter out = new PrintWriter(new FileWriter(args[1]));

        Table[] tables = db.getTables();
        for (Table table : tables) {
            out.println("=== " + table.getName().toUpperCase());
            out.println();
            if (isNotBlank(table.getDescription())) {
                out.println(table.getDescription());
            }
            out.println();
            out.print(".");
            out.println(table.getName().toUpperCase());
            out.println("[cols=\"3,^1,^1,^1,^1,^1,5\"]");
            out.println("|===");
            out.println();
            out.println("|Name|Type|Size|Default|Keys|Not Null|Description");
            for (Column column : table.getColumns()) {
                out.print("|");
                out.print(column.getName().toUpperCase());
                out.print("|");
                out.print(column.getMappedType());
                out.print("|");
                out.print(isNotBlank(column.getSize()) ? column.getSize() : " ");
                out.print("|");
                out.print(isNotBlank(column.getDefaultValue()) ? column.getDefaultValue() : " ");
                out.print("|");
                if (column.isPrimaryKey()) {
                    out.print("PK ");
                }
                ForeignKey[] keys = table.getForeignKeys();
                boolean fk = false;
                for (ForeignKey foreignKey : keys) {
                    Reference[] references = foreignKey.getReferences();
                    for (Reference reference : references) {
                        if (reference.getLocalColumn().getName().equals(column.getName()) && !fk) {
                            out.print("FK");
                            fk = true;
                        }
                    }
                }
                out.print("|");
                if (column.isRequired()) {
                    out.print("X");
                }
                out.print("|");
                out.println(column.getDescription());
            }
            out.println("|===");
        }
        out.close();

    }

    public static String toTitle(String tableName) {
        String[] tokens = tableName.split("_");
        StringBuilder name = new StringBuilder();
        for (String string : tokens) {
            name.append(Character.toUpperCase(string.charAt(0)));
            name.append(string.substring(1));
            name.append(" ");
        }
        return name.toString();
    }
}
