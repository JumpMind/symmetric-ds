package org.jumpmind.util;

import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.lang.StringUtils;

public final class FormatUtils {
    
    private static Pattern pattern = Pattern.compile("\\$\\((.+?)\\)");
    
    private FormatUtils() {
    }

    public static String replace(String prop, String replaceWith, String sourceString) {
        return StringUtils.replace(sourceString, "$(" + prop + ")", replaceWith);
    }

    public static String replaceTokens(String text, Map<String, String> replacements,
            boolean matchUsingPrefixSuffix) {
        if (replacements != null && replacements.size() > 0) {
            if (matchUsingPrefixSuffix) {
                Matcher matcher = pattern.matcher(text);
                StringBuffer buffer = new StringBuffer();
                while (matcher.find()) {
                    String[] match = matcher.group(1).split("\\|");
                    String replacement = replacements.get(match[0]);
                    if (replacement != null) {
                        matcher.appendReplacement(buffer, "");
                        if (match.length == 2) {
                            replacement = formatString(match[1], replacement);
                        }
                        buffer.append(replacement);
                    }
                }
                matcher.appendTail(buffer);
                text = buffer.toString();
            } else {
                for (Object key : replacements.keySet()) {
                    text = text.replaceAll(key.toString(), replacements.get(key));
                }
            }
        }
        return text;

    }

    public static String formatString(String format, String arg) {
        if (format.indexOf("d") >= 0 || format.indexOf("u") >= 0 || format.indexOf("i") >= 0) {
            return String.format(format, Long.parseLong(arg));
        } else if (format.indexOf("e") >= 0 || format.indexOf("f") >= 0) {
            return String.format(format, Double.valueOf(arg));
        } else {
            return String.format(format, arg);
        }
    }


}
