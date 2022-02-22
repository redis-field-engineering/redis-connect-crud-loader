package com.redis.connect.crud.loader.core;

import com.opencsv.CSVReader;
import com.redis.connect.crud.loader.config.LoaderConfig;
import com.redis.connect.crud.loader.connections.JDBCConnectionProvider;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.commons.validator.GenericValidator;
import picocli.CommandLine;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.lang.management.ManagementFactory;
import java.sql.*;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Locale;
import java.util.Map;

/**
 * @author Virag Tripathi
 */
@Getter
@Setter
@Slf4j
@CommandLine.Command(name = "crudloader",
        description = "Load CSV data to source database and execute random Insert, Update and Delete events.")
public class CRUDLoader implements Runnable {

    private static final String WHOAMI = "CRUDLoader";
    private final String instanceId;

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
    private int batchSize = (int) sourceConfig.getOrDefault("batchSize", 100);
    private int iteration = (int) sourceConfig.getOrDefault("iteration", 1);
    private static final String type = (String) sourceConfig.get("type");
    private char separator = (char) sourceConfig.getOrDefault("separator", ',');
    private boolean truncateBeforeLoad = (boolean) sourceConfig.getOrDefault("truncateBeforeLoad", true);

    private Connection connection;
    private ReadFile readFile;
    private File filePath;

    public CRUDLoader() {
        this.instanceId = ManagementFactory.getRuntimeMXBean().getName();
    }

    /**
     * Parse CSV file using OpenCSV library and load in
     * given database table.
     *
     * @throws Exception Throws exception
     */
    private void doInsert(Connection connection) throws Exception {
        Instant start = Instant.now();
        CSVReader csvReader;
        PreparedStatement ps;
        try {
            filePath = new File(System.getProperty(LoaderConfig.INSTANCE.getCONFIG_LOCATION_PROPERTY())
                    .concat(File.separator).concat(csvFile));

            csvReader = new CSVReader(new FileReader(filePath));
            log.info("Loading {} into {} table with batchSize={}.", filePath, CRUDLoader.tableName, batchSize);

        } catch (Exception e) {
            e.printStackTrace();
            throw new Exception("Error occurred while executing file. "
                    + ExceptionUtils.getRootCauseMessage(e));
        }

        String[] headerRow = csvReader.readNext();

        if (null == headerRow) {
            throw new FileNotFoundException(
                    "No columns defined in given CSV file. " +
                            "Please check the CSV file format.");
        }

        String questionmarks = StringUtils.repeat("?" + getSeparator(), headerRow.length);
        questionmarks = (String) questionmarks.subSequence(0, questionmarks
                .length() - 1);

        String insert_query = SQL_INSERT.replaceFirst(TABLE_REGEX, CRUDLoader.tableName);
        insert_query = insert_query
                .replaceFirst(KEYS_REGEX, StringUtils.join(headerRow,
                        this.separator));
        insert_query = insert_query.replaceFirst(VALUES_REGEX, questionmarks);

        log.info("Insert Query: {}", insert_query);

        try {
            ps = connection.prepareStatement(insert_query);

            int count = 0;
            String[] rowData;

            while ((rowData = csvReader.readNext()) != null) {

                int index = 1;
                for (String columnData : rowData) {
                    if (GenericValidator.isDate(columnData, DateTimeUtil.yyyy_MM_dd_HH_mm_ss.getDisplayName(), true)) {
                        String input = columnData.replace(" ", "T");
                        LocalDateTime ldt = LocalDateTime.parse(input);
                        ps.setTimestamp(index++, Timestamp.valueOf(ldt));
                    } else if ((GenericValidator.isDate(columnData, DateTimeUtil.yyyy_mm_dd.getDisplayName(),
                            true)) || (GenericValidator.isDate(columnData,
                            DateTimeUtil.yyyy_MM_dd.getDisplayName(), true))) {
                        Date d = Date.valueOf(columnData);
                        ps.setDate(index++, d);
                    } else if ((GenericValidator.isDate(columnData, DateTimeUtil.dd_MM_yyyy.getDisplayName(),
                            true))) {
                        DateTimeFormatter formatter = DateTimeFormatter.ofPattern(DateTimeUtil.dd_MM_yyyy.getDisplayName(), Locale.ENGLISH);
                        LocalDate d = LocalDate.parse(columnData, formatter);
                        ps.setDate(index++, Date.valueOf(d));
                    } else if (GenericValidator.isDouble(columnData)) {
                        ps.setDouble(index++, Double.parseDouble(columnData));
                    } else if (GenericValidator.isInt(columnData)) {
                        ps.setInt(index++, Integer.parseInt(columnData));
                    } else {
                        ps.setString(index++, columnData);
                    }
                }
                ps.addBatch();

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

            ps.close();
            csvReader.close();

            Instant finish = Instant.now();
            long timeElapsed = Duration.between(start, finish).toMillis();
            log.info("It took {} ms to load {} csv records.", timeElapsed, count);
        } catch (Exception e) {
            e.printStackTrace();
            throw new Exception(
                    "Error occurred while loading data from csv file to the database. "
                            + ExceptionUtils.getRootCauseMessage(e));
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
            while (rs.next()) {
                for (int i = 1; i <= rsmd.getColumnCount(); i++)
                    if (log.isDebugEnabled()) {
                        log.debug("{{} : {}}", rsmd.getColumnName(i), rs.getString(i));
                    }
            }
            rs.close();
            st.close();
        } catch (Exception e) {
            e.printStackTrace();
            log.error("Instance: {} {} failed during select " + "MESSAGE: {} STACKTRACE: {}",
                    instanceId, WHOAMI,
                    ExceptionUtils.getRootCauseMessage(e), ExceptionUtils.getRootCauseStackTrace(e));
        }
    }

    private void doUpdate(Connection connection) {
        int count = 0;
        filePath = new File(System.getProperty(LoaderConfig.INSTANCE.getCONFIG_LOCATION_PROPERTY())
                .concat(File.separator).concat(update));
        log.info("\n[Performing UPDATE] ... ");
        try {
            readFile = new ReadFile();
            ArrayList<String> updateDataList;
            updateDataList = readFile.readFileAsList(filePath.getAbsolutePath());
            Statement st = connection.createStatement();

            for (String sql : updateDataList) {
                st.addBatch(sql);
                if (++count % batchSize == 0) {
                    st.executeBatch();
                }
            }

            log.info("Updated {} row(s) in {} table.", count, tableName);
            st.executeBatch(); // update remaining records
            log.info("{} row(s) affected!", count);

            st.close();
        } catch (Exception e) {
            e.printStackTrace();
            log.error("Instance: {} {} failed during update " + "MESSAGE: {} STACKTRACE: {}",
                    instanceId, WHOAMI,
                    ExceptionUtils.getRootCauseMessage(e), ExceptionUtils.getRootCauseStackTrace(e));
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
            while (rs.next()) {
                for (int i = 1; i <= rsmd.getColumnCount(); i++)
                    log.info("{{} : {}}", rsmd.getColumnName(i), rs.getString(i));
            }
            rs.close();
            st.close();

        } catch (Exception e) {
            e.printStackTrace();
            log.error("Instance: {} {} failed during updatedSelect " + "MESSAGE: {} STACKTRACE: {}",
                    instanceId, WHOAMI,
                    ExceptionUtils.getRootCauseMessage(e), ExceptionUtils.getRootCauseStackTrace(e));
        }
    }

    private void doDelete(Connection connection) {
        int count = 0;
        filePath = new File(System.getProperty(LoaderConfig.INSTANCE.getCONFIG_LOCATION_PROPERTY())
                .concat(File.separator).concat(delete));
        log.info("\n[Performing DELETE] ... ");
        try {
            readFile = new ReadFile();
            ArrayList<String> deletedDataList;
            deletedDataList = readFile.readFileAsList(filePath.getAbsolutePath());
            Statement st = connection.createStatement();

            for (String sql : deletedDataList) {
                st.addBatch(sql);
                if (++count % batchSize == 0) {
                    st.executeBatch();
                }
            }

            log.info("Deleted {} row(s) in {} table.", count, tableName);
            st.executeBatch(); // update remaining records
            log.info("{} row(s) affected!", count);

            st.close();
        } catch (Exception e) {
            e.printStackTrace();
            log.error("Instance: {} {} failed during delete " + "MESSAGE: {} STACKTRACE: {}",
                    instanceId, WHOAMI,
                    ExceptionUtils.getRootCauseMessage(e), ExceptionUtils.getRootCauseStackTrace(e));
        }
    }

    private void doDeleteAll(Connection connection) {
        log.info("\n[Performing DELETE ALL ROWS] ... ");
        try {
            Statement st = connection.createStatement();
            st.executeUpdate("DELETE FROM " + tableName);

            st.close();
        } catch (Exception e) {
            e.printStackTrace();
            log.error("Instance: {} {} failed during deleteAll " + "MESSAGE: {} STACKTRACE: {}",
                    instanceId, WHOAMI,
                    ExceptionUtils.getRootCauseMessage(e), ExceptionUtils.getRootCauseStackTrace(e));
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
        } catch (Exception e) {
            e.printStackTrace();
            log.error("Instance: {} {} failed during count " + "MESSAGE: {} STACKTRACE: {}",
                    instanceId, WHOAMI,
                    ExceptionUtils.getRootCauseMessage(e), ExceptionUtils.getRootCauseStackTrace(e));
        }

    }

    private void runAll() {
        try {
            log.info("Instance: {} {} started with {} iteration(s).", ManagementFactory.getRuntimeMXBean().getName(), WHOAMI, iteration);
            Instant start = Instant.now();
            CoreConfig coreConfig = new CoreConfig();
            JDBCConnectionProvider JDBC_CONNECTION_PROVIDER = new JDBCConnectionProvider();
            connection = JDBC_CONNECTION_PROVIDER.getConnection(coreConfig.getConnectionId());
            for (int i = 1; i <= iteration; i++) {
                if (truncateBeforeLoad) {
                    doDeleteAll(connection);
                } else {
                    log.info("Skipping truncate..");
                }
                if (select != null) {
                    doSelect(connection);
                } else {
                    log.info("Skipping select..");
                }

                doCount(connection);

                if (csvFile != null) {
                    doInsert(connection);
                    doCount(connection);
                } else {
                    log.error("CSV data file is missing for the load.");
                    log.info("Skipping csv load and exiting..");
                    System.exit(0);
                }

                if (update != null) {
                    doUpdate(connection);
                    doCount(connection);
                } else {
                    log.info("Skipping update..");
                }
                if (updatedSelect != null) {
                    doUpdatedSelect(connection);
                } else {
                    log.info("Skipping updatedSelect..");
                }
                if (delete != null) {
                    doDelete(connection);
                    doCount(connection);
                } else {
                    log.info("Skipping delete..");
                }
                if (select != null) {
                    doSelect(connection);
                } else {
                    log.info("Skipping select..");
                }
                doCount(connection);

            }

            log.info("Instance: {} {} ended with {} iteration(s).", instanceId, WHOAMI, iteration);

            Instant finish = Instant.now();
            long timeElapsed = Duration.between(start, finish).toMillis();
            log.info("It took {} ms to finish {} iterations.", timeElapsed, iteration);
            connection.close();
        } catch (Exception e) {
            e.printStackTrace();
            log.error("Instance: {} {} failed during runAll " + "MESSAGE: {} STACKTRACE: {}",
                    instanceId, WHOAMI,
                    ExceptionUtils.getRootCauseMessage(e), ExceptionUtils.getRootCauseStackTrace(e));
        }

    }

    @Override
    public void run() {
        runAll();
    }

}