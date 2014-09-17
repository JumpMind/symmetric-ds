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
package org.jumpmind.util;

import java.text.ParsePosition;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.time.FastDateFormat;
import org.jumpmind.exception.ParseException;

public final class FormatUtils {
    
    public static final String[] TIMESTAMP_PATTERNS = { "yyyy-MM-dd HH:mm:ss.S",
            "yyyy-MM-dd HH:mm:ss",  "yyyy-MM-dd HH:mm:ss.SSS", "yyyy-MM-dd HH:mm", "yyyy-MM-dd" };

    public static final String[] TIME_PATTERNS = { "HH:mm:ss.S", "HH:mm:ss",
            "yyyy-MM-dd HH:mm:ss.S", "yyyy-MM-dd HH:mm:ss" };

    public static final FastDateFormat TIMESTAMP_FORMATTER = FastDateFormat
            .getInstance("yyyy-MM-dd HH:mm:ss.SSS");

    public static final FastDateFormat TIME_FORMATTER = FastDateFormat.getInstance("HH:mm:ss.SSS");

    public final static String WILDCARD = "*";
    
    public final static String NEGATE_TOKEN = "!";

    public final static int MAX_CHARS_TO_LOG = 1000;

    private static Pattern pattern = Pattern.compile("\\$\\((.+?)\\)");

    private FormatUtils() {
    }

    public static String replace(String prop, String replaceWith, String sourceString) {
        return StringUtils.replace(sourceString, "$(" + prop + ")", replaceWith);
    }

    public static String replaceToken(String text, String tokenToReplace, String replaceWithText,
            boolean matchUsingPrefixSuffix) {
        Map<String, String> replacements = new HashMap<String, String>(1);
        replacements.put(tokenToReplace, replaceWithText);
        return replaceTokens(text, replacements, matchUsingPrefixSuffix);
    }

    /**
     * Replace the keys found in the target text with the values found in the
     * replacements map.
     * 
     * @param text
     *            The text to replace
     * @param replacements
     *            The map that contains the replacement values
     * @param matchUsingPrefixSuffix
     *            If true, look for the $(key) pattern to replace. If false,
     *            just replace the key outright.
     * @return The text with the token keys replaced
     */
    public static String replaceTokens(String text, Map<String, String> replacements,
            boolean matchUsingPrefixSuffix) {
        if (text != null && replacements != null && replacements.size() > 0) {
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

    public static boolean toBoolean(String value) {
        if (StringUtils.isNotBlank(value)) {
            if (value.equals("1")) {
                return true;
            } else if (value.equals("0")) {
                return false;
            } else {
                return Boolean.parseBoolean(value);
            }
        } else {
            return false;
        }
    }

    public static boolean isMixedCase(String text) {
        char[] chars = text.toCharArray();
        boolean upper = false;
        boolean lower = false;
        for (char ch : chars) {
            upper |= Character.isUpperCase(ch);
            lower |= Character.isLowerCase(ch);
        }
        return upper && lower;
    }
    
    public static boolean isWildCardMatch(String text, String pattern, boolean ignoreCase) {
        boolean match = isWildCardMatch(text, pattern);
        if (ignoreCase && !match) {
            match = isWildCardMatch(text.toLowerCase(), pattern);
            if (!match) {
                match = isWildCardMatch(text.toUpperCase(), pattern);
            }
        }
        return match;
    }


    public static boolean isWildCardMatch(String text, String pattern) {
        boolean match = true;
        if (pattern.startsWith(NEGATE_TOKEN)) {
            pattern = pattern.substring(1);
        }
        // Create the cards by splitting using a RegEx. If more speed
        // is desired, a simpler character based splitting can be done.
        String[] cards = pattern.split("\\" + WILDCARD);

        for(int i = 0; i < cards.length; i++) {
            String card = cards[i];
            
            boolean foundToken = false;
            if (i == 0 && !pattern.startsWith("*")) {
                foundToken = text.startsWith(card);
            } else {
                foundToken = text.indexOf(card) != -1;
            }

            // Card not detected in the text.
            if (!foundToken) {
                return !match;
            }

            // Move ahead, towards the right of the text.
            text = text.substring(text.indexOf(card) + card.length());
        }

        return match;
    }

    /**
     * Word wrap a string where the line size for the first line is different
     * than the lines sizes for the other lines.
     * 
     * @param str
     * @param firstLineSize
     * @param nonFirstLineSize
     * @return
     */
    public static String[] wordWrap(String str, int firstLineSize, int nonFirstLineSize) {

        String[] lines = wordWrap(str, firstLineSize);

        if (lines.length > 1 && firstLineSize != nonFirstLineSize) {
            // More than one line. Re-wrap the non-first lines with the non
            // first line size
            String notFirstLinesString = StringUtils.join(lines, " ", 1, lines.length);
            String[] nonFirstLines = wordWrap(notFirstLinesString, nonFirstLineSize);
            List<String> nonFirstLineCollection = Arrays.asList(nonFirstLines);

            ArrayList<String> allLines = new ArrayList<String>();
            allLines.add(lines[0]);
            allLines.addAll(nonFirstLineCollection);

            lines = allLines.toArray(lines);
        }

        return lines;
    }

    public static String[] wordWrap(String str, int lineSize) {
        if (str != null && str.length() > lineSize) {
            Pattern regex = Pattern.compile("(\\S\\S{" + lineSize + ",}|.{1," + lineSize
                    + "})(\\s+|$)");
            List<String> list = new ArrayList<String>();
            Matcher m = regex.matcher(str);
            while (m.find()) {
                String group = m.group();
                // Preserve multiple newlines
                String[] lines = StringUtils.splitPreserveAllTokens(group, '\n');

                for (String line : lines) {
                    // Trim whitespace from end since a space on the end can
                    // push line wrap and cause an unintentional blank line.
                    line = StringUtils.removeEnd(line, " ");
                    list.add(line);
                }
            }
            return (String[]) list.toArray(new String[list.size()]);
        } else {
            return new String[] { str };
        }
    }

    public static String abbreviateForLogging(String value) {
        if (value != null) {
            value = value.trim();
        }
        return StringUtils.abbreviate(value, MAX_CHARS_TO_LOG);
    }
    
    public static Date parseDate(String str, String[] parsePatterns) {
        return parseDate(str, parsePatterns, null);
    }

    public static Date parseDate(String str, String[] parsePatterns, TimeZone timeZone) {
        if (str == null || parsePatterns == null) {
            throw new IllegalArgumentException("Date and Patterns must not be null");
        }
        
        SimpleDateFormat parser = null;
        ParsePosition pos = new ParsePosition(0);
        for (int i = 0; i < parsePatterns.length; i++) {
            if (i == 0) {
                parser = new SimpleDateFormat(parsePatterns[0]);
                if (timeZone != null) {
                    parser.setTimeZone(timeZone);
                }
            } else {
                parser.applyPattern(parsePatterns[i]);
            }
            pos.setIndex(0);
            Date date = parser.parse(str, pos);
            if (date != null && pos.getIndex() == str.length()) {
                return date;
            }
        }
        
        try {
            Date date = new Date(Long.parseLong(str));
            return date;
        } catch (NumberFormatException e) {
            
        }
        
        throw new ParseException("Unable to parse the date: " + str);
    }
    
    public static String[] splitOnSpacePreserveQuotedStrings(String source) {
        List<String> matchList = new ArrayList<String>();
        Pattern regex = Pattern.compile("[^\\s\"']+|\"([^\"]*)\"|'([^']*)'");
        Matcher regexMatcher = regex.matcher(source);
        while (regexMatcher.find()) {
            if (regexMatcher.group(1) != null) {
                // Add double-quoted string without the quotes
                matchList.add(regexMatcher.group(1));
            } else if (regexMatcher.group(2) != null) {
                // Add single-quoted string without the quotes
                matchList.add(regexMatcher.group(2));
            } else {
                // Add unquoted word
                matchList.add(regexMatcher.group());
            }
        } 
        return matchList.toArray(new String[matchList.size()]);
    }

}
