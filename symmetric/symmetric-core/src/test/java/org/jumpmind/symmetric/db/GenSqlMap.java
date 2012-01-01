package org.jumpmind.symmetric.db;

import java.io.File;
import java.util.Map;
import java.util.Set;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.StringUtils;
import org.springframework.context.support.ClassPathXmlApplicationContext;

public class GenSqlMap {

    @SuppressWarnings({ "rawtypes", "unchecked" })
    public static void main(String[] args) throws Exception {
        ClassPathXmlApplicationContext ctx = new ClassPathXmlApplicationContext(
                "classpath:/symmetric-client.xml");
        Map<String, Map> beans = ctx.getBeansOfType(Map.class);
        Set<String> keys = beans.keySet();
        for (String k : keys) {
            if (k.endsWith("Sql")) {
                System.out.println(k);
                Map<String, String> sql = beans.get(k);
                String className = k.substring(0, 1).toUpperCase()
                        + k.subSequence(1, k.length() - 3) + "SqlMap";
                String fileName = className + ".java";
                File file = new File("target", fileName);
                StringBuilder b = new StringBuilder();
                b.append("package org.jumpmind.symmetric.service.impl;");
                b.append("\n");
                b.append("\n");
                b.append("import org.jumpmind.db.IDatabasePlatform;\n");
                b.append("import org.jumpmind.db.sql.AbstractSqlMap;\n");
                b.append("import java.util.Map;\n");
                b.append("\n");
                b.append("public class " + className + " extends AbstractSqlMap {");
                b.append("\n");
                b.append("\n");
                b.append("    public " + className
                        + "(IDatabasePlatform platform, Map<String, String> replacementTokens) { ");
                b.append("\n");                
                b.append("        super(platform, replacementTokens);\n");
                b.append("\n");
                for (String key : sql.keySet()) {
                    b.append("        putSql(" + v(key, ",\"\" + \n", 0) + v(sql.get(key), ");\n", 100));
                    b.append("\n");
                }
                b.append("    }");
                b.append("\n");
                b.append("\n");
                b.append("}");
                FileUtils.write(file, b);
            }
        }

    }

    protected static String v(String t, String suffix, int rightPad) {
        if (t == null) {
            return "null;\n";
        } else {
            String v = "";
            String[] lines = t.trim().split("\n");
            int longestLine = 0;
            for (int i = 0; i < lines.length; i++) {
                lines[i] = lines[i].trim();
                lines[i] = lines[i].replace("\\", "\\\\").replace("\"", "\\\"")
                        .replace("sym_", "$(prefixName)_");
                if (i > 0) {
                    lines[i] = StringUtils.leftPad(lines[i], lines[i].length() + 2);
                }
                lines[i] = "\"" + lines[i];
                longestLine = longestLine < lines[i].length() + 3 ? lines[i].length() + 3 : longestLine;
            }

            for (String line : lines) {
                v = v + StringUtils.rightPad(line, longestLine) + "\" + \n";
            }
            v = v.substring(0, v.length() - 3) + suffix;
            return v;
        }
    }

}
