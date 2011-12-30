package org.jumpmind.symmetric.db;

import java.io.File;
import java.util.Map;
import java.util.Set;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.StringUtils;
import org.springframework.context.support.ClassPathXmlApplicationContext;

public class GenTriggerText {

    /**
     * @param args
     */
    public static void main(String[] args) throws Exception {
        ClassPathXmlApplicationContext ctx = new ClassPathXmlApplicationContext(
                "classpath:/symmetric-client.xml");
        Map<String, AbstractSymmetricDialect> beans = ctx
                .getBeansOfType(AbstractSymmetricDialect.class);
        Set<String> keys = beans.keySet();
        for (String k : keys) {
            System.out.println(k);
            AbstractSymmetricDialect d = beans.get(k);
            TriggerText text = d.getTriggerText();
            String className = d
                    .getClass()
                    .getSimpleName()
                    .subSequence(0,
                            d.getClass().getSimpleName().length() - "SymmetricDialect".length())
                    + "TriggerText";
            String fileName = className + ".java";
            File file = new File("target", fileName);
            StringBuilder b = new StringBuilder();
            b.append("package " + d.getClass().getPackage().getName() + ";");
            b.append("\n");
            b.append("\n");
            b.append("import org.jumpmind.symmetric.db.TriggerText;\n");
            b.append("import java.util.HashMap;\n");            
            b.append("\n");
            b.append("public class " + className + " extends TriggerText {");
            b.append("\n");
            b.append("\n");
            b.append("    public " + className + "() { ");
            b.append("\n");
            b.append("        functionInstalledSql = " + v(text.functionInstalledSql, ";\n", 0));
            b.append("        emptyColumnTemplate = " + v(text.emptyColumnTemplate, ";\n", 0));
            b.append("        stringColumnTemplate = " + v(text.stringColumnTemplate, ";\n", 0));
            b.append("        xmlColumnTemplate = " + v(text.xmlColumnTemplate, ";\n", 0));
            b.append("        arrayColumnTemplate = " + v(text.arrayColumnTemplate, ";\n", 0));
            b.append("        numberColumnTemplate = " + v(text.numberColumnTemplate, ";\n", 0));
            b.append("        datetimeColumnTemplate = " + v(text.datetimeColumnTemplate, ";\n", 0));
            b.append("        timeColumnTemplate = " + v(text.timeColumnTemplate, ";\n", 0));
            b.append("        dateColumnTemplate = " + v(text.dateColumnTemplate, ";\n", 0));
            b.append("        clobColumnTemplate = " + v(text.clobColumnTemplate, ";\n", 0));
            b.append("        blobColumnTemplate = " + v(text.blobColumnTemplate, ";\n", 0));
            b.append("        wrappedBlobColumnTemplate = "
                    + v(text.wrappedBlobColumnTemplate, ";\n", 0));
            b.append("        booleanColumnTemplate = " + v(text.booleanColumnTemplate, ";\n", 0));
            b.append("        triggerConcatCharacter = " + v(text.triggerConcatCharacter, ";\n", 0));
            b.append("        newTriggerValue = " + v(text.newTriggerValue, ";\n", 0));
            b.append("        oldTriggerValue = " + v(text.oldTriggerValue, ";\n", 0));
            b.append("        oldColumnPrefix = " + v(text.oldColumnPrefix, ";\n", 0));
            b.append("        newColumnPrefix = " + v(text.newColumnPrefix, ";\n", 0));
            b.append("        otherColumnTemplate = " + v(text.otherColumnTemplate, ";\n", 0));
            b.append("\n");
            Map<String, String> functionTemplatesToInstall = text.functionTemplatesToInstall;
            if (functionTemplatesToInstall != null) {
                b.append("        functionTemplatesToInstall = new HashMap<String,String>();\n");
                for (String key : functionTemplatesToInstall.keySet()) {
                    b.append("        functionTemplatesToInstall.put(" + v(key, ",\n", 0)
                            + v(functionTemplatesToInstall.get(key), ");\n", 200));
                }
                b.append("\n");
            }
            Map<String, String> sqlTemplates = text.sqlTemplates;
            b.append("        sqlTemplates = new HashMap<String,String>();\n");
            for (String key : sqlTemplates.keySet()) {
                b.append("        sqlTemplates.put(" + v(key, ",\n", 0)
                        + v(sqlTemplates.get(key), ");\n", 200));
            }

            b.append("    }");
            b.append("\n");
            b.append("\n");
            b.append("}");
            FileUtils.write(file, b);
        }

    }

    protected static String v(String t, String suffix, int rightPad) {
        if (t == null) {
            return "null;\n";
        } else {
            String v = "";
            String[] lines = t.trim().split("\n");
            for (String line : lines) {
                v = v + StringUtils.rightPad("\"" + line.replace("\\","\\\\").replace("\"", "\\\""), rightPad) + "\" + \n";
            }
            v = v.substring(0, v.length() - 3) + suffix;
            return v;
        }
    }

}
