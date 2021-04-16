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
package org.jumpmind.db.io;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.HttpURLConnection;
import java.net.URL;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import org.jumpmind.db.model.Column;
import org.jumpmind.db.model.Database;
import org.jumpmind.db.model.ForeignKey;
import org.jumpmind.db.model.IIndex;
import org.jumpmind.db.model.IndexColumn;
import org.jumpmind.db.model.Reference;
import org.jumpmind.db.model.Table;
import org.jumpmind.properties.DefaultParameterParser;
import org.jumpmind.properties.DefaultParameterParser.ParameterMetaData;

import com.google.gson.Gson;

public class ReleaseNotesGenerator {

    private static int features = 0;
    private static int osFeatures = 0;

    private static int improvements = 0;
    private static int osImprovements = 0;

    private static int fixes = 0;
    private static int osFixes = 0;

    private static final String sections[] = { "fixes.ad", "whats-new.ad", "issues.ad", "tables.ad", "parameters.ad" };
    private static final String writtenSections[] = { "whats-new.ad" };

    private static final String categories[] = { "new feature", "improvement", "bug" };
    private static final String tags[] = { "security", "performance" };

    public static void main(String[] args) throws Exception {

        String directory = args[4].substring(0, args[4].lastIndexOf('/')) + '/';
        String finalNotesFile = args[4];
        String fixesFile = directory + "fixes.ad";
        String issuesFile = directory + "issues.ad";
        String tablesFile = directory + "tables.ad";
        String parametersFile = directory + "parameters.ad";

        if (args.length < 6) {
            System.err.println("wrong usage");
            System.exit(-1);
        }

        List<Issue> issues = buildIssuesFromRestAPI(args[5]);

        String properties = args[0];
        String schema = args[1];
        String proProperties = args[2];
        String proSchema = args[3];

        PrintWriter notes = new PrintWriter(new FileWriter(finalNotesFile));
        PrintWriter fixesSec = new PrintWriter(new FileWriter(fixesFile));
        PrintWriter issuesSec = new PrintWriter(new FileWriter(issuesFile));
        PrintWriter tablesSec = new PrintWriter(new FileWriter(tablesFile));
        PrintWriter parametersSec = new PrintWriter(new FileWriter(parametersFile));

        writeParametersSection(parametersSec, properties, proProperties);
        writeTablesSection(tablesSec, schema, proSchema);
        writeIssuesSection(issuesSec, issues);
        writeFixesSection(fixesSec, issues);

        writeFinalNotes(notes, sections);

        notes.close();
        fixesSec.close();
        issuesSec.close();
        tablesSec.close();
        parametersSec.close();

    }

    protected static List<Issue> buildIssuesFromRestAPI(String url) throws Exception {
        URL apiURL = new URL(url);
        HttpURLConnection conn = (HttpURLConnection) apiURL.openConnection();
        if (conn.getResponseCode() != 200) {
            throw new RuntimeException("Failed : HTTP Error code : " + conn.getResponseCode());
        }
        InputStreamReader in = new InputStreamReader(conn.getInputStream());
        BufferedReader br = new BufferedReader(in);
        StringBuffer buffer = new StringBuffer();
        buffer.append("[");
        String output;
        while ((output = br.readLine()) != null) {
            buffer.append(output);
        }
        buffer.append("]");
        String json = buffer.toString();
        json = json.replaceAll("\\}\\{", "},{");
        conn.disconnect();

        List<Issue> issues = new ArrayList<Issue>();
        @SuppressWarnings("unchecked")
        List<Map<String, String>> root = new Gson().fromJson(json, List.class);
        for (Map<String, String> map : root) {
            Issue issue = new Issue();
            issue.setId(map.get("id"));
            issue.setCategory(map.get("category"));
            issue.setProject(map.get("project"));
            issue.setPriority(map.get("priority"));
            issue.setSummary(map.get("summary"));
            issue.setTag(map.get("tag"));
            issue.setVersion(map.get("version"));
            issues.add(issue);
        }

        return issues;
    }

    protected static void writeFinalNotes(PrintWriter writer, String[] sections) {
        writer.println(ReleaseNotesConstants.NOTES_HEADER);
        writer.println();
        writer.println(ReleaseNotesConstants.OVERVIEW_HEADER);
        writer.println();
        writer.println(ReleaseNotesConstants.PRO_TOKEN_START);
        writer.println(String.format(ReleaseNotesConstants.OVERVIEW_PRO_DESC, features, improvements, fixes));
        writer.println(ReleaseNotesConstants.PRO_TOKEN_END);
        writer.println(ReleaseNotesConstants.OS_TOKEN_START);
        writer.println(String.format(ReleaseNotesConstants.OVERVIEW_DESC, osFeatures, osImprovements, osFixes));
        writer.println(ReleaseNotesConstants.OS_TOKEN_END);
        writer.println();
        for (String section : sections) {
            if (Arrays.asList(writtenSections).contains(section)) {
                writer.println(String.format(ReleaseNotesConstants.INCLUDE_FORMAT_WRITTEN, section));
            } else {
                writer.println(String.format(ReleaseNotesConstants.INCLUDE_FORMAT_GENERATED, section));
            }
        }
    }

    protected static void writeParametersSection(PrintWriter writer, String propertiesLoc, String proPropertiesLoc)
            throws Exception {
        DefaultParameterParser parser = new DefaultParameterParser(new FileInputStream(propertiesLoc));
        Map<String, ParameterMetaData> previousParamMap = parser.parse();

        parser = new DefaultParameterParser(new FileInputStream(ReleaseNotesConstants.PROPERTIES_DIR));
        Map<String, ParameterMetaData> currentParamMap = parser.parse();

        parser = new DefaultParameterParser(new FileInputStream(proPropertiesLoc));
        Map<String, ParameterMetaData> previousProParamMap = parser.parse();

        Map<String, ParameterMetaData> currentProParamMap = new HashMap<String, ParameterMetaData>();
        try {
            parser = new DefaultParameterParser(new FileInputStream(ReleaseNotesConstants.PRO_PROPERTIES_DIR));
            currentProParamMap = parser.parse();
        } catch (FileNotFoundException e) {            
        }

        Set<String> previousParams = previousParamMap.keySet();
        Set<String> currentParams = currentParamMap.keySet();
        Set<String> previousProParams = previousProParamMap.keySet();
        Set<String> currentProParams = currentProParamMap.keySet();
        Set<String> inCurrentNotPrevious = new HashSet<String>();
        Set<String> inPreviousNotCurrent = new HashSet<String>();
        Set<String> modifiedSincePrevious = new HashSet<String>();

        for (String param : currentParams) {
            if (!previousParams.contains(param)) {
                inCurrentNotPrevious.add(param);
            } else if (!currentParamMap.get(param).getDefaultValue()
                    .equalsIgnoreCase(previousParamMap.get(param).getDefaultValue())) {
                modifiedSincePrevious.add(param);
            }
        }

        for (String param : currentProParams) {
            if (!previousProParams.contains(param)) {
                inCurrentNotPrevious.add(param);
            } else if (!currentProParamMap.get(param).getDefaultValue()
                    .equalsIgnoreCase(previousProParamMap.get(param).getDefaultValue())) {
                modifiedSincePrevious.add(param);
            }
        }

        for (String param : previousParams) {
            if (!currentParams.contains(param)) {
                System.out.println("Could not find param " + param + " in current params");
                inPreviousNotCurrent.add(param);
            }
        }

        for (String param : previousProParams) {
            if (!currentProParams.contains(param)) {
                System.out.println("Could not find param " + param + " in current pro params");
                inPreviousNotCurrent.add(param);
            }
        }

        if (inCurrentNotPrevious.size() > 0 || modifiedSincePrevious.size() > 0 || inPreviousNotCurrent.size() > 0) {
            if (currentProParams.containsAll(inCurrentNotPrevious)
                    && currentProParams.containsAll(modifiedSincePrevious)
                    && previousProParams.containsAll(inPreviousNotCurrent)) {
                writer.println(ReleaseNotesConstants.PRO_TOKEN_START);
            }
            writer.println(ReleaseNotesConstants.PARAMETER_SEC_HEADER);
            writer.println(ReleaseNotesConstants.PARAMETER_SEC_DESC);
            writer.println();
            if (currentProParams.containsAll(inCurrentNotPrevious)
                    && currentProParams.containsAll(modifiedSincePrevious)
                    && previousProParams.containsAll(inPreviousNotCurrent)) {
                writer.println(ReleaseNotesConstants.PRO_TOKEN_END);
            }
        }

        if (inCurrentNotPrevious.size() > 0) {

            if (currentProParams.containsAll(inCurrentNotPrevious)) {
                writer.println(ReleaseNotesConstants.PRO_TOKEN_START);
            }
            writer.println(ReleaseNotesConstants.PARAMETER_NEW_HEADER);
            writer.println();
            if (currentProParams.containsAll(inCurrentNotPrevious)) {
                writer.println(ReleaseNotesConstants.PRO_TOKEN_END);
            }

            List<String> parameters = new ArrayList<String>(inCurrentNotPrevious);
            parameters.sort(new StringComparator());
            
            for (String parameter : parameters) {
                if (currentProParamMap.containsKey(parameter)) {
                    writer.println(ReleaseNotesConstants.PRO_TOKEN_START);
                    writer.println(String.format(ReleaseNotesConstants.PARAMETER_PRO_FORMAT, parameter));
                    writer.println();
                    writer.println(currentProParamMap.get(parameter).getDescription() != null
                            ? String.format(ReleaseNotesConstants.PARAMETER_DESC_FORMAT,
                                    currentProParamMap.get(parameter).getDescription().trim(),
                                    currentProParamMap.get(parameter).getDefaultValue())
                            : "");
                    writer.println(ReleaseNotesConstants.PRO_TOKEN_END);
                } else {
                    writer.println(String.format(ReleaseNotesConstants.PARAMETER_FORMAT, parameter));
                    writer.println();
                    writer.println(currentParamMap.get(parameter).getDescription() != null
                            ? String.format(ReleaseNotesConstants.PARAMETER_DESC_FORMAT,
                                    currentParamMap.get(parameter).getDescription().trim(),
                                    currentParamMap.get(parameter).getDefaultValue())
                            : "");
                }
                writer.println();
            }
        }

        if (modifiedSincePrevious.size() > 0 || inPreviousNotCurrent.size() > 0) {

            if (currentProParams.containsAll(modifiedSincePrevious)
                    && previousProParams.containsAll(inPreviousNotCurrent)) {
                writer.println(ReleaseNotesConstants.PRO_TOKEN_START);
            }
            writer.println(ReleaseNotesConstants.PARAMETER_MOD_HEADER);
            writer.println();
            if (currentProParams.containsAll(modifiedSincePrevious)
                    && previousProParams.containsAll(inPreviousNotCurrent)) {
                writer.println(ReleaseNotesConstants.PRO_TOKEN_END);
            }

            List<String> parameters = new ArrayList<String>(modifiedSincePrevious);
            parameters.sort(new StringComparator());

            for (String parameter : parameters) {
                if (currentProParamMap.containsKey(parameter)) {
                    writer.println(ReleaseNotesConstants.PRO_TOKEN_START);
                    writer.println(String.format(ReleaseNotesConstants.PARAMETER_PRO_FORMAT, parameter));
                    writer.println();
                    writer.println(currentProParamMap.get(parameter).getDescription() != null
                            ? String.format(ReleaseNotesConstants.PARAMETER_DESC_MOD_FORMAT,
                                    currentProParamMap.get(parameter).getDescription().trim(),
                                    previousProParamMap.get(parameter).getDefaultValue(),
                                    currentProParamMap.get(parameter).getDefaultValue())
                            : "");
                    writer.println(ReleaseNotesConstants.PRO_TOKEN_END);
                } else {
                    writer.println(String.format(ReleaseNotesConstants.PARAMETER_FORMAT, parameter));
                    writer.println();
                    writer.println(currentParamMap.get(parameter).getDescription() != null
                            ? String.format(ReleaseNotesConstants.PARAMETER_DESC_MOD_FORMAT,
                                    currentParamMap.get(parameter).getDescription().trim(),
                                    previousParamMap.get(parameter).getDefaultValue(),
                                    currentParamMap.get(parameter).getDefaultValue())
                            : "");
                }
                writer.println();
            }

            parameters = new ArrayList<String>(inPreviousNotCurrent);
            parameters.sort(new StringComparator());

            for (String parameter : inPreviousNotCurrent) {
                if (previousProParamMap.containsKey(parameter)) {
                    writer.println(ReleaseNotesConstants.PRO_TOKEN_START);
                    writer.println(String.format(ReleaseNotesConstants.PARAMETER_PRO_FORMAT, parameter));
                    writer.println();
                    writer.println(ReleaseNotesConstants.PARAMETER_DESC_REMOVED);
                    writer.println(ReleaseNotesConstants.PRO_TOKEN_END);
                } else {
                    writer.println(String.format(ReleaseNotesConstants.PARAMETER_FORMAT, parameter));
                    writer.println();
                    writer.println(ReleaseNotesConstants.PARAMETER_DESC_REMOVED);
                }
                writer.println();
            }
        }
    }

    public static <T> Predicate<T> distinctByKey(Function<? super T, ?> keyExtractor) {
        Set<Object> seen = ConcurrentHashMap.newKeySet();
        return t -> seen.add(keyExtractor.apply(t));
    }

    protected static void writeTablesSection(PrintWriter writer, String schemaLoc, String proSchemaLoc) {

        Database dbOld = DatabaseXmlUtil.read(new File(schemaLoc));
        Database dbCurrent = DatabaseXmlUtil.read(new File(ReleaseNotesConstants.SCHEMA_DIR));
        
        Database dbProOld = new Database();
        File proSchemaFile = new File(proSchemaLoc);
        if (proSchemaFile.canRead()) {
            dbProOld = DatabaseXmlUtil.read(proSchemaFile);
        }
        Database dbProCurrent = new Database();
        File dbProCurrentFile = new File(ReleaseNotesConstants.PRO_SCHEMA_DIR);
        if (dbProCurrentFile.canRead()) {
            dbProCurrent = DatabaseXmlUtil.read(dbProCurrentFile);
        }

        List<Table> tablesProOld = new ArrayList<Table>(Arrays.asList(dbProOld.getTables()));
        List<Table> tablesProCurrent = new ArrayList<Table>(Arrays.asList(dbProCurrent.getTables()));
        List<Table> tablesOld = new ArrayList<Table>(Arrays.asList(dbOld.getTables()));
        List<Table> tablesCurrent = new ArrayList<Table>(Arrays.asList(dbCurrent.getTables()));

        tablesOld.addAll(tablesProOld);
        tablesCurrent.addAll(tablesProCurrent);

        List<Table> newTables = new ArrayList<>();

        Map<Table, List<Column>> tableColMap = new HashMap<>();
        Map<Table, List<String>> tableChangeMap = new HashMap<>();

        System.out.println(tablesOld.size() + " old tables compared to " + tablesCurrent.size() + " current tables.");

        for (Table table : tablesCurrent) {
            boolean isNewTable = true;
            for (Table oldTable : tablesOld) {
                if (table.getName().equalsIgnoreCase(oldTable.getName())) {
                    isNewTable = false;

                    List<String> changeList = new ArrayList<>();

                    List<ForeignKey> foreignKeys = Arrays.asList(table.getForeignKeys());
                    List<String> primaryKeys = Arrays.asList(table.getPrimaryKeyColumnNames());
                    List<IIndex> indices = Arrays.asList(table.getIndices());

                    List<ForeignKey> oldForeignKeys = Arrays.asList(oldTable.getForeignKeys());
                    List<String> oldPrimaryKeys = Arrays.asList(oldTable.getPrimaryKeyColumnNames());
                    List<IIndex> oldIndices = Arrays.asList(oldTable.getIndices());

                    Set<IIndex> allIndices = new HashSet<>();
                    allIndices.addAll(
                            indices.stream().filter(distinctByKey(IIndex::getName)).collect(Collectors.toList()));
                    allIndices.addAll(
                            oldIndices.stream().filter(distinctByKey(IIndex::getName)).collect(Collectors.toList()));
                    allIndices = allIndices.stream().filter(distinctByKey(IIndex::getName)).collect(Collectors.toSet());

                    Set<ForeignKey> allFKeys = new HashSet<>();
                    allFKeys.addAll(foreignKeys.stream().filter(distinctByKey(ForeignKey::getName))
                            .collect(Collectors.toList()));
                    allFKeys.addAll(oldForeignKeys.stream().filter(distinctByKey(ForeignKey::getName))
                            .collect(Collectors.toList()));
                    allFKeys = allFKeys.stream().filter(distinctByKey(ForeignKey::getName)).collect(Collectors.toSet());

                    Set<String> allPKeys = new HashSet<>();
                    allPKeys.addAll(primaryKeys);
                    allPKeys.addAll(oldPrimaryKeys);

                    // Foreign Keys
                    for (ForeignKey foreignKey : allFKeys) {
                        if (foreignKeys.contains(foreignKey) && !oldForeignKeys.contains(foreignKey)) {
                            changeList.add(
                                    String.format(ReleaseNotesConstants.TABLES_ADD_FKEY_FORMAT, foreignKey.getName(),
                                            String.join(ReleaseNotesConstants.VALUE_SEPARATOR,
                                                    Arrays.asList(foreignKey.getReferences()).stream()
                                                            .map(e -> e.getLocalColumnName())
                                                            .collect(Collectors.toList()))));
                        } else if (!foreignKeys.contains(foreignKey) && oldForeignKeys.contains(foreignKey)) {
                            changeList.add(
                                    String.format(ReleaseNotesConstants.TABLES_DEL_FKEY_FORMAT, foreignKey.getName(),
                                            String.join(ReleaseNotesConstants.VALUE_SEPARATOR,
                                                    Arrays.asList(foreignKey.getReferences()).stream()
                                                            .map(e -> e.getLocalColumnName())
                                                            .collect(Collectors.toList()))));
                        } else {
                            ForeignKey oldFKey = oldForeignKeys.stream()
                                    .filter(e -> e.getName().equalsIgnoreCase(foreignKey.getName()))
                                    .collect(Collectors.toList()).get(0);
                            ForeignKey newFKey = foreignKeys.stream()
                                    .filter(e -> e.getName().equalsIgnoreCase(foreignKey.getName()))
                                    .collect(Collectors.toList()).get(0);
                            Set<Reference> oldReferencesSet = new HashSet<>(Arrays.asList(oldFKey.getReferences()));
                            Set<Reference> newReferencesSet = new HashSet<>(Arrays.asList(newFKey.getReferences()));
                            if (oldFKey.isAutoIndexPresent() != newFKey.isAutoIndexPresent()) {
                                changeList.add(String.format(
                                        ReleaseNotesConstants.MODIFIED_CONSTANT_FKEY + " "
                                                + ReleaseNotesConstants.TABLES_MOD_COLUMN_FORMAT,
                                        foreignKey.getName(), ReleaseNotesConstants.MODIFIED_CONSTANT_IS_AUTOIDX,
                                        oldFKey.isAutoIndexPresent(), newFKey.isAutoIndexPresent()));
                            }
                            if (!oldReferencesSet.equals(newReferencesSet)) {
                                changeList.add(String.format(ReleaseNotesConstants.TABLES_MOD_FKEY_FORMAT,
                                        foreignKey.getName(),
                                        String.join(ReleaseNotesConstants.VALUE_SEPARATOR,
                                                oldReferencesSet.stream().map(e -> e.getForeignColumnName())
                                                        .collect(Collectors.toList())),
                                        String.join(ReleaseNotesConstants.VALUE_SEPARATOR, newReferencesSet.stream()
                                                .map(e -> e.getForeignColumnName()).collect(Collectors.toList()))));
                            }

                        }
                    }

                    // Primary Keys
                    StringBuffer added = new StringBuffer();
                    StringBuffer removed = new StringBuffer();
                    boolean addedAny = false;
                    boolean removedAny = false;

                    for (String primaryKey : allPKeys) {

                        if (primaryKeys.contains(primaryKey) && !oldPrimaryKeys.contains(primaryKey)) {
                            added.append(primaryKey + ReleaseNotesConstants.VALUE_SEPARATOR);
                            addedAny = true;
                        } else if (!primaryKeys.contains(primaryKey) && oldPrimaryKeys.contains(primaryKey)) {
                            removed.append(primaryKey + ReleaseNotesConstants.VALUE_SEPARATOR);
                            removedAny = true;
                        }
                    }

                    if (addedAny) {
                        added.delete(added.length() - ReleaseNotesConstants.VALUE_SEPARATOR.length(), added.length());
                        changeList.add(String.format(ReleaseNotesConstants.TABLES_ADD_PKEY_FORMAT, added.toString()));
                    }
                    if (removedAny) {
                        removed.delete(removed.length() - ReleaseNotesConstants.VALUE_SEPARATOR.length(),
                                removed.length());
                        changeList.add(String.format(ReleaseNotesConstants.TABLES_DEL_PKEY_FORMAT, removed.toString()));
                    }

                    // Indices
                    for (IIndex index : allIndices) {
                        if (indices.stream().anyMatch(e -> e.getName().equals(index.getName()))
                                && !oldIndices.stream().anyMatch(e -> e.getName().equals(index.getName()))) {
                            changeList.add(String.format(ReleaseNotesConstants.TABLES_ADD_INDEX_FORMAT, index.getName(),
                                    String.join(ReleaseNotesConstants.VALUE_SEPARATOR, Arrays.asList(index.getColumns())
                                            .stream().map(e -> e.getName()).collect(Collectors.toList()))));
                        } else if (!indices.stream().anyMatch(e -> e.getName().equals(index.getName()))
                                && oldIndices.stream().anyMatch(e -> e.getName().equals(index.getName()))) {
                            changeList.add(String.format(ReleaseNotesConstants.TABLES_DEL_INDEX_FORMAT, index.getName(),
                                    String.join(ReleaseNotesConstants.VALUE_SEPARATOR, Arrays.asList(index.getColumns())
                                            .stream().map(e -> e.getName()).collect(Collectors.toList()))));

                        } else {
                            IIndex oldIndex = oldIndices.stream()
                                    .filter(e -> e.getName().equalsIgnoreCase(index.getName()))
                                    .collect(Collectors.toList()).get(0);
                            IIndex newIndex = indices.stream()
                                    .filter(e -> e.getName().equalsIgnoreCase(index.getName()))
                                    .collect(Collectors.toList()).get(0);
                            Set<IndexColumn> oldReferencesSet = new HashSet<>(Arrays.asList(oldIndex.getColumns()));
                            Set<IndexColumn> newReferencesSet = new HashSet<>(Arrays.asList(newIndex.getColumns()));

                            if (oldIndex.isUnique() != newIndex.isUnique()) {
                                changeList.add(ReleaseNotesConstants.MODIFIED_CONSTANT_INDEX + " "
                                        + String.format(ReleaseNotesConstants.TABLES_MOD_COLUMN_FORMAT, index.getName(),
                                                ReleaseNotesConstants.MODIFIED_CONSTANT_IS_UNQ, oldIndex.isUnique(),
                                                newIndex.isUnique()));
                            }
                            if (!oldReferencesSet.equals(newReferencesSet)) {
                                changeList.add(
                                        String.format(ReleaseNotesConstants.TABLES_MOD_INDEX_FORMAT, index.getName(),
                                                String.join(ReleaseNotesConstants.VALUE_SEPARATOR,
                                                        oldReferencesSet.stream().map(e -> e.getName())
                                                                .collect(Collectors.toList())),
                                                String.join(ReleaseNotesConstants.VALUE_SEPARATOR, newReferencesSet
                                                        .stream().map(e -> e.getName()).collect(Collectors.toList()))));
                            }
                        }
                    }

                    List<Column> ColsNew = table.getColumnsAsList();
                    List<Column> ColsOld = oldTable.getColumnsAsList();

                    // Columns
                    if (!table.getColumnsAsList().stream().map(Column::getName).allMatch(oldTable.getColumnsAsList()
                            .stream().map(Column::getName).collect(Collectors.toSet())::contains)) {

                        List<Column> mapList = new ArrayList<>();

                        for (Column column : ColsNew) {
                            if (!ColsOld.stream().map(Column::getName).filter(column.getName()::equals).findFirst()
                                    .isPresent()) {
                                mapList.add(column);
                            }
                        }
                        if (mapList.size() > 0)
                            tableColMap.put(table, mapList);
                    }

                    for (Column column : ColsNew) {
                        try {
                            Column oldColumn = ColsOld.stream().filter(o -> o.getName().equals(column.getName()))
                                    .findFirst().get();
                            if (oldColumn.isTimestampWithTimezone() != column.isTimestampWithTimezone()) {
                                changeList.add(String.format(ReleaseNotesConstants.TABLES_MOD_COLUMN_FORMAT,
                                        column.getName(), ReleaseNotesConstants.MODIFIED_CONSTANT_IS_TIMEZONE,
                                        oldColumn.isTimestampWithTimezone(), column.isTimestampWithTimezone()));
                            }
                            if (oldColumn.isDistributionKey() != column.isDistributionKey()) {
                                changeList.add(String.format(ReleaseNotesConstants.TABLES_MOD_COLUMN_FORMAT,
                                        column.getName(), ReleaseNotesConstants.MODIFIED_CONSTANT_IS_DIST,
                                        oldColumn.isDistributionKey(), column.isDistributionKey()));
                            }
                            if (oldColumn.isAutoIncrement() != column.isAutoIncrement()) {
                                changeList.add(String.format(ReleaseNotesConstants.TABLES_MOD_COLUMN_FORMAT,
                                        column.getName(), ReleaseNotesConstants.MODIFIED_CONSTANT_IS_AUTOINC,
                                        oldColumn.isAutoIncrement(), column.isAutoIncrement()));
                            }
                            if (oldColumn.isUnique() != column.isUnique()) {
                                changeList.add(String.format(ReleaseNotesConstants.TABLES_MOD_COLUMN_FORMAT,
                                        column.getName(), ReleaseNotesConstants.MODIFIED_CONSTANT_IS_UNQ,
                                        oldColumn.isUnique(), column.isUnique()));
                            }
                            if (oldColumn.getPrecisionRadix() != column.getPrecisionRadix()) {
                                changeList.add(String.format(ReleaseNotesConstants.TABLES_MOD_COLUMN_FORMAT,
                                        column.getName(), ReleaseNotesConstants.MODIFIED_CONSTANT_RADIX,
                                        oldColumn.getPrecisionRadix(), column.getPrecisionRadix()));
                            }
                            if (isChanged(oldColumn.getDefaultValue(), column.getDefaultValue())) {
                                changeList.add(String.format(ReleaseNotesConstants.TABLES_MOD_COLUMN_FORMAT,
                                        column.getName(), ReleaseNotesConstants.MODIFIED_CONSTANT_DEF_VAL,
                                        oldColumn.getDefaultValue() == null ? ReleaseNotesConstants.NULL
                                                : (isVarcharOrChar(column.getMappedTypeCode())
                                                        ? "\"" + column.getDefaultValue() + "\""
                                                        : column.getDefaultValue()),
                                        column.getDefaultValue() == null ? ReleaseNotesConstants.NULL
                                                : (isVarcharOrChar(column.getMappedTypeCode())
                                                        ? "\"" + column.getDefaultValue() + "\""
                                                        : column.getDefaultValue())));
                            }
                            if (isChanged(oldColumn.getMappedType(), column.getMappedType())) {
                                changeList.add(String.format(ReleaseNotesConstants.TABLES_MOD_COLUMN_FORMAT,
                                        column.getName(), ReleaseNotesConstants.MODIFIED_CONSTANT_TYPE,
                                        oldColumn.getMappedType() == null ? ReleaseNotesConstants.NULL
                                                : oldColumn.getMappedType(),
                                        column.getMappedType() == null ? ReleaseNotesConstants.NULL
                                                : column.getMappedType()));
                            }
                            if (isChanged(oldColumn.getSize(), column.getSize())) {
                                changeList.add(String.format(ReleaseNotesConstants.TABLES_MOD_COLUMN_FORMAT,
                                        column.getName(), ReleaseNotesConstants.MODIFIED_CONSTANT_SIZE,
                                        oldColumn.getSize() == null ? ReleaseNotesConstants.NULL : oldColumn.getSize(),
                                        column.getSize() == null ? ReleaseNotesConstants.NULL : column.getSize()));
                            }
                            if (oldColumn.isRequired() != column.isRequired()) {
                                changeList.add(String.format(ReleaseNotesConstants.TABLES_MOD_COLUMN_FORMAT,
                                        column.getName(), ReleaseNotesConstants.MODIFIED_CONSTANT_IS_REQ,
                                        oldColumn.isRequired(), column.isRequired()));
                            }
                        } catch (NoSuchElementException e) {
                            System.out.println("No match found for column \"" + column.getName() + "\", assuming new");
                        }
                    }

                    if (changeList.size() > 0)
                        tableChangeMap.put(table, changeList);
                }
            }
            if (isNewTable) {
                newTables.add(table);
            }
        }

        if (newTables.size() > 0 || tableColMap.keySet().size() > 0 || tableChangeMap.size() > 0) {
            if (tablesProCurrent.containsAll(newTables) && tablesProCurrent.containsAll(tableColMap.keySet())
                    && tablesProCurrent.containsAll(tableChangeMap.keySet())) {
                writer.println(ReleaseNotesConstants.PRO_TOKEN_START);
            }
            writer.println(ReleaseNotesConstants.TABLES_SEC_HEADER);
            writer.println(ReleaseNotesConstants.TABLES_SEC_DESC);
            writer.println();
            if (tablesProCurrent.containsAll(newTables) && tablesProCurrent.containsAll(tableColMap.keySet())
                    && tablesProCurrent.containsAll(tableChangeMap.keySet())) {
                writer.println(ReleaseNotesConstants.PRO_TOKEN_END);
            }

            if (newTables.size() > 0) {
                if (tablesProCurrent.containsAll(newTables)) {
                    writer.println(ReleaseNotesConstants.PRO_TOKEN_START);
                }
                writer.println(ReleaseNotesConstants.TABLES_NEW_HEADER);
                writer.println();
                writer.println(ReleaseNotesConstants.TABLES_TABLE_HEADER);
                writer.println();
                for (Table table : newTables) {
                    String desc = table.getDescription();
                    String name = table.getNameLowerCase();

                    if (tablesProCurrent.contains(table)) {
                        writer.println(ReleaseNotesConstants.PRO_TOKEN_START);
                        writer.println(String.format(ReleaseNotesConstants.TABLES_TABLE_ELEMENT_PRO, name, desc));
                        writer.println(ReleaseNotesConstants.PRO_TOKEN_END);
                        writer.println();
                    } else {
                        writer.println(String.format(ReleaseNotesConstants.TABLES_TABLE_ELEMENT, name, desc));
                        writer.println();
                    }

                }
                writer.println(ReleaseNotesConstants.TABLES_TABLE_FOOTER);
                if (tablesProCurrent.containsAll(newTables)) {
                    writer.println(ReleaseNotesConstants.PRO_TOKEN_END);
                }
            }

            if (tableColMap.keySet().size() > 0) {
                writer.println(ReleaseNotesConstants.TABLES_NEW_COL_HEADER);
                writer.println();
                List<Table> tables = new ArrayList<Table>(tableColMap.keySet());
                tables.sort(new TableComparator());
                for (Table table : tables) {
                    if (tablesProCurrent.contains(table)) {
                        writer.println(ReleaseNotesConstants.PRO_TOKEN_START);
                        writer.println(String.format(ReleaseNotesConstants.TABLES_COL_HEADER_PRO, table.getName().toUpperCase()));
                        writer.println();
                    } else {
                        writer.println(String.format(ReleaseNotesConstants.TABLES_COL_HEADER, table.getName().toUpperCase()));
                        writer.println();
                    }
                    for (Column column : tableColMap.get(table)) {
                        writer.println(String.format(ReleaseNotesConstants.TABLES_COLUMN_ELEMENT, column.getName(),
                                column.getDescription()));
                        writer.println();
                    }
                    writer.println(ReleaseNotesConstants.TABLES_TABLE_FOOTER);
                    if (tablesProCurrent.contains(table)) {
                        writer.println(ReleaseNotesConstants.PRO_TOKEN_END);
                    }
                    writer.println();
                }
            }
            writer.println();

            if (tableChangeMap.size() > 0) {
                writer.println(ReleaseNotesConstants.TABLES_MODIFIED_HEADER);
                writer.println();
                List<Table> tables = new ArrayList<Table>(tableChangeMap.keySet());
                tables.sort(new TableComparator());
                for (Table table : tables) {
                    if (tablesProCurrent.contains(table)) {
                        writer.println(ReleaseNotesConstants.PRO_TOKEN_START);
                        writer.println(
                                String.format(ReleaseNotesConstants.TABLES_MODIFIED_CAPTION_PRO, table.getName().toUpperCase()));
                    } else {
                        writer.println(String.format(ReleaseNotesConstants.TABLES_MODIFIED_CAPTION, table.getName().toUpperCase()));
                    }
                    for (String change : tableChangeMap.get(table)) {
                        writer.println(String.format(ReleaseNotesConstants.TABLES_MODIFIED_ELEMENT, change));
                    }
                    if (tablesProCurrent.contains(table)) {
                        writer.println(ReleaseNotesConstants.PRO_TOKEN_END);
                    }
                    writer.println();
                }
            }
        }
        writer.println();
    }

    private static boolean isVarcharOrChar(int jdbcTypeCode) {
        if (jdbcTypeCode == Types.LONGNVARCHAR || jdbcTypeCode == Types.NVARCHAR || jdbcTypeCode == Types.VARCHAR
                || jdbcTypeCode == Types.LONGNVARCHAR || jdbcTypeCode == Types.CHAR) {
            return true;
        } else {
            return false;
        }
    }

    private static boolean isChanged(String previous, String current) {
        if (previous == null && current != null) {
            return true;
        } else if (previous != null && current == null) {
            return true;
        } else if (previous == null && current == null) {
            return false;
        } else if (previous.equalsIgnoreCase(current)) {
            return false;
        } else if (!previous.equalsIgnoreCase(current)) {
            return true;
        } else {
            return false;
        }
    }

    private static int writeMinorVersionIssues(PrintWriter writer, List<Issue> issues, String version,
            String category) {
        int issuesWritten = 0;
        if (issues.stream().anyMatch(e -> e.getVersion().equalsIgnoreCase(version)
                && e.getProject().equalsIgnoreCase("symmetric-pro") && e.getCategory().equalsIgnoreCase(category))) {
            writer.println(ReleaseNotesConstants.PRO_TOKEN_START);
            writer.println(ReleaseNotesConstants.ISSUES_SEC_SEPARATOR);
            writer.println(ReleaseNotesConstants.ISSUES_HARDBREAKS);
            writer.println(String.format(ReleaseNotesConstants.ISSUES_VERSION_HEADER_PRO, version));

            for (Issue issue : issues) {
                if (issue.getProject().equalsIgnoreCase("symmetric-pro") && issue.getVersion().equalsIgnoreCase(version)
                        && issue.getCategory().equalsIgnoreCase(category)) {
                    String idWithLink = (String.format(ReleaseNotesConstants.ISSUE_URL, issue.getId(), issue.getId()));
                    writer.println(String.format(ReleaseNotesConstants.ISSUES_FORMAT, idWithLink, issue.getSummary()));
                    issuesWritten++;
                }
            }
            writer.println(ReleaseNotesConstants.ISSUES_SEC_SEPARATOR);
            writer.println(ReleaseNotesConstants.PRO_TOKEN_END);
        }

        if (issues.stream().anyMatch(e -> e.getVersion().equalsIgnoreCase(version)
                && !e.getProject().equalsIgnoreCase("symmetric-pro") && e.getCategory().equalsIgnoreCase(category))) {
            writer.println(ReleaseNotesConstants.ISSUES_SEC_SEPARATOR);
            writer.println(ReleaseNotesConstants.ISSUES_HARDBREAKS);
            writer.println(String.format(ReleaseNotesConstants.ISSUES_VERSION_HEADER, version));

            for (Issue issue : issues) {
                if (!issue.getProject().equalsIgnoreCase("symmetric-pro")
                        && issue.getVersion().equalsIgnoreCase(version)
                        && issue.getCategory().equalsIgnoreCase(category)) {
                    String idWithLink = (String.format(ReleaseNotesConstants.ISSUE_URL, issue.getId(), issue.getId()));
                    writer.println(String.format(ReleaseNotesConstants.ISSUES_FORMAT, idWithLink, issue.getSummary()));
                    issuesWritten++;
                }
            }
            writer.println(ReleaseNotesConstants.ISSUES_SEC_SEPARATOR);
        }
        return issuesWritten;
    }

    protected static void writeIssuesSection(PrintWriter writer, List<Issue> issues) {

        List<String> versions = new ArrayList<String>();
        Map<String, String> categoryHeaders = new HashMap<>();
        Map<String, Integer> trackers = new HashMap<>();
        Map<String, Integer> proTrackers = new HashMap<>();

        categoryHeaders.put(categories[0], ReleaseNotesConstants.ISSUES_FEATURES_HEADER);
        categoryHeaders.put(categories[1], ReleaseNotesConstants.ISSUES_IMPROVEMENTS_HEADER);
        categoryHeaders.put(categories[2], ReleaseNotesConstants.ISSUES_BUG_FIXES_HEADER);

        for (Issue issue : issues) {
            if (!versions.contains(issue.getVersion().toLowerCase())) {
                versions.add(issue.getVersion().toLowerCase());
            }
        }

        versions.sort(new VersionComparator());

        issues.sort(new IssueComparator());

        features = issues.stream().filter(e -> e.getCategory().equalsIgnoreCase(categories[0]))
                .collect(Collectors.toList()).size();
        improvements = issues.stream().filter(e -> e.getCategory().equalsIgnoreCase(categories[1]))
                .collect(Collectors.toList()).size();
        fixes = issues.stream().filter(e -> e.getCategory().equalsIgnoreCase(categories[2]))
                .collect(Collectors.toList()).size();

        osFeatures = issues.stream().filter(e -> e.getCategory().equalsIgnoreCase(categories[0])
                && !e.getProject().equalsIgnoreCase("symmetric-pro")).collect(Collectors.toList()).size();
        osImprovements = issues.stream().filter(e -> e.getCategory().equalsIgnoreCase(categories[1])
                && !e.getProject().equalsIgnoreCase("symmetric-pro")).collect(Collectors.toList()).size();
        osFixes = issues.stream().filter(e -> e.getCategory().equalsIgnoreCase(categories[2])
                && !e.getProject().equalsIgnoreCase("symmetric-pro")).collect(Collectors.toList()).size();

        trackers.put(categories[0], features);
        trackers.put(categories[1], improvements);
        trackers.put(categories[2], fixes);

        int proFeatures = issues.stream().filter(e -> e.getCategory().equalsIgnoreCase(categories[0])
                && e.getProject().equalsIgnoreCase("symmetric-pro")).collect(Collectors.toList()).size();
        int proImprovements = issues.stream().filter(e -> e.getCategory().equalsIgnoreCase(categories[1])
                && e.getProject().equalsIgnoreCase("symmetric-pro")).collect(Collectors.toList()).size();
        int proFixes = issues.stream().filter(e -> e.getCategory().equalsIgnoreCase(categories[2])
                && e.getProject().equalsIgnoreCase("symmetric-pro")).collect(Collectors.toList()).size();

        proTrackers.put(categories[0], proFeatures);
        proTrackers.put(categories[1], proImprovements);
        proTrackers.put(categories[2], proFixes);

        boolean allSymProIssues = true;
        for (String category : categories) {
            if (!trackers.get(category).equals(proTrackers.get(category))) {
                allSymProIssues = false;
            }
        }

        if (features > 0 || improvements > 0 || fixes > 0) {
            if (allSymProIssues) {
                writer.println(ReleaseNotesConstants.PRO_TOKEN_START);
            }
            writer.println(ReleaseNotesConstants.ISSUES_SEC_HEADER);
            if (allSymProIssues) {
                writer.println(ReleaseNotesConstants.PRO_TOKEN_END);
            }
        }

        for (String category : categories) {
            if (trackers.get(category) > 0) {
                if (trackers.get(category).equals(proTrackers.get(category))) {
                    writer.println(ReleaseNotesConstants.PRO_TOKEN_START);
                }
                writer.println(categoryHeaders.get(category));
                for (String version : versions) {
                    writeMinorVersionIssues(writer, issues, version, category);
                }
                if (trackers.get(category).equals(proTrackers.get(category))) {
                    writer.println(ReleaseNotesConstants.PRO_TOKEN_END);
                }
            }
        }
        writer.println();
    }

    protected static void writeFixesSection(PrintWriter writer, List<Issue> issues) {

        int osSecurityFixes = issues.stream().filter(e -> e.getTag() != null && e.getTag().equalsIgnoreCase(tags[0])
                && !e.getProject().equalsIgnoreCase("symmetric-pro")).collect(Collectors.toList()).size();
        int osPerformanceFixes = issues.stream().filter(e -> e.getTag() != null && e.getTag().equalsIgnoreCase(tags[1])
                && !e.getProject().equalsIgnoreCase("symmetric-pro")).collect(Collectors.toList()).size();

        int allSecurityFixes = issues.stream().filter(e -> e.getTag() != null && e.getTag().equalsIgnoreCase(tags[0]))
                .collect(Collectors.toList()).size();
        int allPerformanceFixes = issues.stream()
                .filter(e -> e.getTag() != null && e.getTag().equalsIgnoreCase(tags[1])).collect(Collectors.toList())
                .size();

        if (allSecurityFixes > 0) {
            if (osSecurityFixes == 0) {
                writer.println(ReleaseNotesConstants.PRO_TOKEN_START);
            }
            writer.println(ReleaseNotesConstants.FIXES_SECURITY_HEADER);
            writer.println();
            writer.println(ReleaseNotesConstants.FIXES_TABLE_HEADER);
            writer.println();
            issues.sort(new IssueComparator());
            for (Issue issue : issues) {
                if (issue.getTag() != null && issue.getTag().equalsIgnoreCase(tags[0])) {
                    String idWithLink = (String.format(ReleaseNotesConstants.ISSUE_URL, issue.getId(), issue.getId()));
                    if (issue.getProject().equalsIgnoreCase("symmetric-pro")) {
                        writer.println(ReleaseNotesConstants.PRO_TOKEN_START);
                        writer.println(String.format(ReleaseNotesConstants.FIXES_TABLE_ELEMENT_PRO, idWithLink,
                                issue.getSummary(),
                                issue.getPriority().substring(0, 1).toUpperCase() + issue.getPriority().substring(1)));
                        writer.println(ReleaseNotesConstants.PRO_TOKEN_END);
                    } else {
                        writer.println(String.format(ReleaseNotesConstants.FIXES_TABLE_ELEMENT, idWithLink,
                                issue.getSummary(),
                                issue.getPriority().substring(0, 1).toUpperCase() + issue.getPriority().substring(1)));
                    }
                    writer.println();

                }
            }
            writer.println(ReleaseNotesConstants.FIXES_TABLE_FOOTER);
            if (osSecurityFixes == 0) {
                writer.println(ReleaseNotesConstants.PRO_TOKEN_END);
            }
        }

        writer.println();

        if (allPerformanceFixes > 0) {
            if (osPerformanceFixes == 0) {
                writer.println(ReleaseNotesConstants.PRO_TOKEN_START);
            }
            writer.println(ReleaseNotesConstants.FIXES_PERFORMANCE_HEADER);
            writer.println();
            writer.println(ReleaseNotesConstants.FIXES_TABLE_HEADER);
            writer.println();
            for (Issue issue : issues) {
                if (issue.getTag() != null && issue.getTag().equalsIgnoreCase(tags[1])) {
                    String idWithLink = (String.format(ReleaseNotesConstants.ISSUE_URL, issue.getId(), issue.getId()));
                    if (issue.getProject().equalsIgnoreCase("symmetric-pro")) {
                        writer.println(ReleaseNotesConstants.PRO_TOKEN_START);
                        writer.println(String.format(ReleaseNotesConstants.FIXES_TABLE_ELEMENT_PRO, idWithLink,
                                issue.getSummary(),
                                issue.getPriority().substring(0, 1).toUpperCase() + issue.getPriority().substring(1)));
                        writer.println(ReleaseNotesConstants.PRO_TOKEN_END);
                    } else {
                        writer.println(String.format(ReleaseNotesConstants.FIXES_TABLE_ELEMENT, idWithLink,
                                issue.getSummary(),
                                issue.getPriority().substring(0, 1).toUpperCase() + issue.getPriority().substring(1)));
                    }
                    writer.println();
                }
            }
            writer.println(ReleaseNotesConstants.FIXES_TABLE_FOOTER);
            if (osPerformanceFixes == 0) {
                writer.println(ReleaseNotesConstants.PRO_TOKEN_END);
            }
        }
    }

    static class VersionComparator implements Comparator<String> {
        @Override
        public int compare(String v1, String v2) {
            return compareTo(v1.split("\\."), v2.split("\\."), 0);
        }
        
        private int compareTo(String[] v1, String[] v2, int index) {
            int compareTo = 0;
            if (v1.length > index && v1.length > index) {
                try {
                    compareTo = Integer.compare(Integer.parseInt(v1[index]), Integer.parseInt(v2[index]));
                    if (compareTo == 0) {
                        compareTo = compareTo(v1, v2, index + 1);
                    }
                } catch (NumberFormatException e) {
                }
            } else if (v1.length != v2.length) {
                compareTo = v1.length > v2.length ? 1 : -1;
            }
            return compareTo;
        }
    }

    static class TableComparator implements Comparator<Table> {
        @Override
        public int compare(Table t1, Table t2) {
            return t1.getNameLowerCase().compareTo(t2.getNameLowerCase());
        }    
    }
    
    static class StringComparator implements Comparator<String> {
        @Override
        public int compare(String s1, String s2) {
            return s1.compareTo(s2);
        }
    }
    
    static class IssueComparator implements Comparator<Issue> {
        @Override
        public int compare(Issue i1, Issue i2) {
            return i1.getId().compareTo(i2.getId());
        }
    }
}
