package com.redis.connect.crud.loader.source.rdb;

import com.redis.connect.crud.loader.connections.JDBCConnectionProvider;
import com.redis.connect.crud.loader.core.CoreConfig;
import com.redis.connect.crud.loader.config.LoaderConfig;
import com.redis.connect.crud.loader.core.ReadFile;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.exception.ExceptionUtils;
import picocli.CommandLine;

import java.io.File;
import java.lang.management.ManagementFactory;
import java.sql.Connection;
import java.sql.Statement;
import java.time.Duration;
import java.time.Instant;
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
@CommandLine.Command(name = "loadsql",
        description = "Load data into source table using sql insert statements.")
public class LoadRDB implements Runnable {
    private Connection connection;

    private static final String WHOAMI = "LoadRDB";
    private static final Map<String, Object> sourceConfig = LoaderConfig.INSTANCE.getEnvConfig().getConnection("source");
    private static final String tableName = (String) sourceConfig.get("tableName");
    private int batchSize = (int) sourceConfig.getOrDefault("batchSize", 100);
    private boolean truncateBeforeLoad = (boolean) sourceConfig.getOrDefault("truncateBeforeLoad", true);

    private String lineQuery;
    private ArrayList<String> fileQuery;

    @Override
    public void run() {
        log.info("Instance: {} {} started.", ManagementFactory.getRuntimeMXBean().getName(), WHOAMI);

        Instant start = Instant.now();

        try {
            CoreConfig coreConfig = new CoreConfig();
            JDBCConnectionProvider JDBC_CONNECTION_PROVIDER = new JDBCConnectionProvider();
            connection = JDBC_CONNECTION_PROVIDER.getConnection(coreConfig.getConnectionId());
            if(truncateBeforeLoad) {
                //delete data from table before loading csv
                connection.createStatement().execute("DELETE FROM " + tableName);
            } else {
                log.info("Skipping truncate..");
            }
            load(connection);

            log.info("Instance: {} {} ended.", ManagementFactory.getRuntimeMXBean().getName(), WHOAMI);

            Instant finish = Instant.now();
            long timeElapsed = Duration.between(start, finish).toMillis();
            log.info("It took {} ms to finish the data load process.", timeElapsed);

            connection.close();

        } catch (Exception e) {
            e.printStackTrace();
            log.error("Instance: {} {} failed during run " + "MESSAGE: {} STACKTRACE: {}",
                    ManagementFactory.getRuntimeMXBean().getName(), WHOAMI,
                    ExceptionUtils.getRootCauseMessage(e), ExceptionUtils.getRootCauseStackTrace(e));
        }

    }

    private void load(Connection connection) throws Exception {
        try {
            int count = 0;
            ReadFile readFile = new ReadFile();
            Statement loadStatement = connection.createStatement();

            lineQuery = (String) sourceConfig.get("loadQuery");

            if(lineQuery == null) {
                String loadQueryFile = (String) sourceConfig.get("loadQueryFile");
                if (loadQueryFile != null) {
                    File filePath = new File(System.getProperty(LoaderConfig.INSTANCE.getCONFIG_LOCATION_PROPERTY())
                            .concat(File.separator).concat(loadQueryFile));
                    fileQuery = readFile.readFileAsList(filePath.getAbsolutePath());

                    for (String row : fileQuery) {
                        loadStatement.addBatch(row);
                        if(++count % batchSize == 0) {
                            loadStatement.executeBatch();
                        }
                    }
                    log.info("Loading {} row(s) into {} table with batchsize {}.", count, tableName, batchSize);
                } else {
                    log.error("SQL file with insert statements is missing for the load.");
                    log.info("Skipping sql load and exiting..");

                }
            }

            loadStatement.executeBatch(); // insert remaining records
            log.info("{} row(s) affected!", count);

            loadStatement.close();
        } catch (Exception e) {
            e.printStackTrace();
            throw new Exception(
                    "Error occurred while loading data from insert statements. "
                            + ExceptionUtils.getRootCauseMessage(e));
        }

    }

}