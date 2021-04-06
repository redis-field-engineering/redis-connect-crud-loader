package com.redislabs.cdc.loader.core;

import com.opencsv.CSVReader;
import com.redislabs.cdc.loader.config.LoaderConfig;
import com.redislabs.cdc.loader.connections.JDBCConnectionProvider;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.validator.GenericValidator;
import picocli.CommandLine;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.sql.*;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Map;

/**
 *
 * @author Virag Tripathi
 *
 */
@Getter
@Setter
@Slf4j
@CommandLine.Command(name = "crudloader",
        description = "Load CSV data to source database and execute random Insert, Update and Delete events.")
public class CRUDLoader implements Runnable {

    private String SQL_INSERT = "INSERT INTO ${table}(${keys}) VALUES(${values})";
    private static final String TABLE_REGEX = "\\$\\{table}";
    private static final String KEYS_REGEX = "\\$\\{keys}";
    private static final String VALUES_REGEX = "\\$\\{values}";

    private static final Map<String, Object> sourceConfig = LoaderConfig.INSTANCE.getEnvConfig().getConnection("source");
    private static final String tableName = (String) sourceConfig.get("tableName");
    private static final String csvFile = (String) sourceConfig.get("csvFile");
    private static final String select = (String) sourceConfig.get("select");
    private static final String updatedSelect = (String) sourceConfig.get("updatedSelect");
    private static final String update = (String) sourceConfig.get("update");
    private static final String delete = (String) sourceConfig.get("delete");
    private static final int batchSize = (int) sourceConfig.get("batchSize");
    private static final int iteration = (int) sourceConfig.get("iteration");
    private static final String type = (String) sourceConfig.get("type");
    private Connection connection;
    private ArrayList<String[]> insertDataList;
    private ArrayList<String> updateDataList;
    private ReadFile readFile;
    private File filePath;

    @CommandLine.Option(names = {"-s", "--separator"}, description = "CSV records separator", paramLabel = "<char>", defaultValue = ",", showDefaultValue = CommandLine.Help.Visibility.ALWAYS)
    private char separator = ',';
    @CommandLine.Option(names = {"-t", "--truncateBeforeLoad"}, description = "Truncate the source table before load", paramLabel = "<boolean>")
    private boolean truncateBeforeLoad = true;

    /**
     * Parse CSV file using OpenCSV library and load in
     * given database table.
     * @throws Exception Throws exception
     */
    private void doInsert(Connection connection) throws Exception {
        CSVReader csvReader;
        try {
            filePath = new File(System.getProperty(LoaderConfig.INSTANCE.getCONFIG_LOCATION_PROPERTY())
                    .concat(File.separator).concat(csvFile));

            csvReader = new CSVReader(new FileReader(filePath));
            log.info("Loading {} into {} table with batchSize={}.", filePath, CRUDLoader.tableName, batchSize);

        } catch (Exception e) {
            e.printStackTrace();
            throw new Exception("Error occurred while executing file. "
                    + e.getMessage());
        }

        String[] headerRow = csvReader.readNext();

        if (null == headerRow) {
            throw new FileNotFoundException(
                    "No columns defined in given CSV file." +
                            "Please check the CSV file format.");
        }

        String questionmarks = StringUtils.repeat("?"+getSeparator(), headerRow.length);
        questionmarks = (String) questionmarks.subSequence(0, questionmarks
                .length() - 1);

        String insert_query = SQL_INSERT.replaceFirst(TABLE_REGEX, CRUDLoader.tableName);
        insert_query = insert_query
                .replaceFirst(KEYS_REGEX, StringUtils.join(headerRow,
                        getSeparator()));
        insert_query = insert_query.replaceFirst(VALUES_REGEX, questionmarks);

        log.info("Insert Query: {}", insert_query);

        String[] rowData;
        PreparedStatement ps;

        try {
            ps = connection.prepareStatement(insert_query);

            int count = 0;
            String datePattern = "yyyy-MM-dd hh:mm:ss";
            insertDataList = new ArrayList<>();

            while ((rowData = csvReader.readNext()) != null) {

                int index = 1;
                for (String columnData : rowData) {
                    if (GenericValidator.isDate(columnData, datePattern, true)) {
                        String input = columnData.replace(" ", "T");
                        LocalDateTime ldt = LocalDateTime.parse(input);
                        ps.setTimestamp(index++, Timestamp.valueOf(ldt));
                    } else if (GenericValidator.isDouble(columnData)) {
                        ps.setDouble(index++, Double.parseDouble(columnData));
                    } else if (GenericValidator.isInt(columnData)) {
                        ps.setInt(index++, Integer.parseInt(columnData));
                    } else {
                        ps.setString(index++, columnData);
                    }
                }
                ps.addBatch();
                insertDataList.add(rowData);

                if (++count % batchSize == 0) {
                    ps.executeBatch();
                }

                if (log.isDebugEnabled()) {
                    log.debug("##### Start Record {} #####", rowData[0]);
                    log.debug("{} record for primary key {}-> {}", type, rowData[0], rowData);
                    log.debug("##### End Record {} #####", rowData[0]);
                }

            }

            log.info("Inserted {} row(s) into {} table.", count, tableName);
            ps.executeBatch(); // insert remaining records
            log.info("{} row(s) affected!", count);

            // commit and close connection
            connection.commit();
            ps.close();
            csvReader.close();
        } catch (Exception e) {
            connection.rollback();
            e.printStackTrace();
            throw new Exception(
                    "Error occurred while loading data from file to database."
                            + e.getMessage());
        }
    }

    private void doSelect(Connection connection) {
        filePath = new File(System.getProperty(LoaderConfig.INSTANCE.getCONFIG_LOCATION_PROPERTY())
                .concat(File.separator).concat(select));
        log.info("[OUTPUT FROM SELECT] {}", filePath.getAbsolutePath());
        try {
            readFile = new ReadFile();
            Statement st = connection.createStatement();
            ResultSet rs = st.executeQuery(readFile.readFileAsString(filePath.getAbsolutePath()));
            ResultSetMetaData rsmd = rs.getMetaData();
            while (rs.next())
            {
                for (int i=1; i<=rsmd.getColumnCount(); i++)
                    //if (log.isDebugEnabled()) {
                        log.info("{{} : {}}", rsmd.getColumnName(i), rs.getString(i));
                    //}
            }
            rs.close();
            st.close();
        }
        catch (Exception e) {
            log.error(e.getMessage());
        }
    }

    private void doUpdate(Connection connection) {
        int count = 0;
        filePath = new File(System.getProperty(LoaderConfig.INSTANCE.getCONFIG_LOCATION_PROPERTY())
                .concat(File.separator).concat(update));
        log.info("\n[Performing UPDATE] ... ");
        try {
            readFile = new ReadFile();
            updateDataList = readFile.readFileAsList(filePath.getAbsolutePath());
            Statement st = connection.createStatement();

            for (String sql : updateDataList) {
                st.addBatch(sql);
                if(++count % batchSize == 0) {
                    st.executeBatch();
                }
            }

            log.info("Updated {} row(s) in {} table.", count, tableName);
            st.executeBatch(); // update remaining records
            log.info("{} row(s) affected!", count);

            st.close();
        }
        catch (Exception e) {
            log.error(e.getMessage());
        }
    }

    private void doUpdatedSelect(Connection connection) {
        filePath = new File(System.getProperty(LoaderConfig.INSTANCE.getCONFIG_LOCATION_PROPERTY())
                .concat(File.separator).concat(updatedSelect));
        log.info("[OUTPUT FROM UPDATED SELECT] {}", filePath.getAbsolutePath());
        try {
            readFile = new ReadFile();
            Statement st = connection.createStatement();
            ResultSet rs = st.executeQuery(readFile.readFileAsString(filePath.getAbsolutePath()));
            ResultSetMetaData rsmd = rs.getMetaData();
            while (rs.next())
            {
                for (int i=1; i<=rsmd.getColumnCount(); i++)
                    log.info("{{} : {}}", rsmd.getColumnName(i), rs.getString(i));
            }
            rs.close();
            st.close();

        }
        catch (Exception e) {
            log.error(e.getMessage());
        }
    }

    private void doDelete(Connection connection) {
        filePath = new File(System.getProperty(LoaderConfig.INSTANCE.getCONFIG_LOCATION_PROPERTY())
                .concat(File.separator).concat(delete));
        log.info("\n[Performing DELETE] ... ");
        try {
            readFile = new ReadFile();
            Statement st = connection.createStatement();
            int count = st.executeUpdate(readFile.readFileAsString(filePath.getAbsolutePath()));

            log.info("Deleted {} row(s) from {} table.", count, tableName);
            st.executeBatch(); // update remaining records
            log.info("{} row(s) affected!", count);

            st.close();
        }
        catch (Exception e) {
            log.error(e.getMessage());
        }
    }

    private void doDeleteAll(Connection connection) {
        log.info("\n[Performing DELETE ALL ROWS] ... ");
        try {
            Statement st = connection.createStatement();
            st.executeUpdate("DELETE FROM " + tableName);

            st.close();
        }
        catch (Exception e) {
            log.error(e.getMessage());
        }
    }

    private void doCount(Connection connection) {
        int select_count;
        try {
            Statement stmt = connection.createStatement();
            String select_count_query = "SELECT COUNT(*) FROM " + tableName;
            ResultSet rs = stmt.executeQuery(select_count_query);
            rs.next();
            select_count = rs.getInt(1);
            log.info("Total records in {}={}.", tableName, select_count);

            stmt.close();
            rs.close();
        } catch(Exception e) {
            e.printStackTrace();
            log.error(e.getMessage());
        }

    }

    private void runAll()
    {
        try {
            log.info("##### CRUDLoader started with {} iteration(s).", iteration);
            CoreConfig coreConfig = new CoreConfig();
            JDBCConnectionProvider JDBC_CONNECTION_PROVIDER = new JDBCConnectionProvider();
            connection = JDBC_CONNECTION_PROVIDER.getConnection(coreConfig.getConnectionId());
            for (int i=1; i<=iteration; i++) {
                doDeleteAll(connection);
                doSelect(connection);
                doCount(connection);
                try {
                    if (csvFile != null) {
                        doInsert(connection);
                        doCount(connection);
                    } else {
                        log.error("CSV data file is missing for the load.");
                        log.info("Skipping csv load and exiting..");
                        System.exit(0);
                    }

                } catch (Exception e) {
                    e.printStackTrace();
                    log.error(e.getMessage());
                }
                doSelect(connection); doCount(connection);
                if (update != null) {
                    doUpdate(connection);  doCount(connection);
                } else {
                    log.info("Skipping update..");
                }
                doUpdatedSelect(connection); doCount(connection);
                if (delete != null) {
                    doDelete(connection);  doCount(connection);
                } else {
                    log.info("Skipping delete..");
                }
                doSelect(connection); doCount(connection);

            }
            log.info("##### CRUDLoader ended with {} iteration(s).", iteration);
            connection.close();
        } catch (Exception e) {
            log.error(e.getMessage());
        }

    }

    @Override
    public void run() {
        runAll();
    }

}