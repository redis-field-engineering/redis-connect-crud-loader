package com.redislabs.connect.crud.loader.source.rdb;

import com.redislabs.connect.crud.loader.config.LoaderConfig;
import com.redislabs.connect.crud.loader.connections.JDBCConnectionProvider;
import com.redislabs.connect.crud.loader.core.CoreConfig;
import com.redislabs.connect.crud.loader.core.ReadFile;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import picocli.CommandLine;

import java.io.File;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
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

    private static final Map<String, Object> sourceConfig = LoaderConfig.INSTANCE.getEnvConfig().getConnection("source");
    private static final String tableName = (String) sourceConfig.get("tableName");
    private static final int batchSize = (int) sourceConfig.get("batchSize");

    private String lineQuery;
    private ArrayList<String> fileQuery;
    @CommandLine.Option(names = "--truncateBeforeLoad", description = "Truncate the source table before load", paramLabel = "<boolean>")
    private boolean truncateBeforeLoad = true;

    @Override
    public void run() {
        try {
            CoreConfig coreConfig = new CoreConfig();
            JDBCConnectionProvider JDBC_CONNECTION_PROVIDER = new JDBCConnectionProvider();
            connection = JDBC_CONNECTION_PROVIDER.getConnection(coreConfig.getConnectionId());
            if(truncateBeforeLoad) {
                //delete data from table before loading csv
                connection.createStatement().execute("DELETE FROM " + tableName);
            }
            load(connection);

            connection.close();

        } catch (Exception e) {
            e.printStackTrace();
            log.error(e.getMessage());
        }

    }

    private void load(Connection connection) {
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
                    log.error("SQL file is missing for the load.");
                    log.info("Skipping sql load and exiting..");

                }
            }

            loadStatement.executeBatch(); // insert remaining records
            log.info("{} row(s) affected!", count);

            loadStatement.close();
        } catch (SQLException sqe) {
            sqe.printStackTrace();
            log.error(String.valueOf(sqe));
        } catch (Exception e) {
            e.printStackTrace();
            log.error(e.getMessage());
        }

    }

}