package com.redis.connect.crud.loader.core;

import com.github.javafaker.Faker;
import com.redis.connect.crud.loader.config.LoaderConfig;
import com.redis.connect.crud.loader.connections.JDBCConnectionProvider;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.exception.ExceptionUtils;
import picocli.CommandLine;

import java.lang.management.ManagementFactory;
import java.sql.*;
import java.text.SimpleDateFormat;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author Virag Tripathi
 */
@Getter
@Setter
@Slf4j
@CommandLine.Command(name = "genloader",
        description = "Load data into emp table using java faker.")
public class GenLoader implements Runnable {

    private final String instanceId;
    private Connection connection;

    private static final String WHOAMI = "GenLoader";
    private static final Map<String, Object> sourceConfig = LoaderConfig.INSTANCE.getEnvConfig().getConnection("source");
    private static final String tableName = (String) sourceConfig.get("tableName");
    private int batchSize = (int) sourceConfig.getOrDefault("batchSize", 100);
    private int iteration = (int) sourceConfig.getOrDefault("iteration", 1);
    private static int counter = (int) sourceConfig.getOrDefault("counter", 1);
    private static final AtomicInteger count = new AtomicInteger(0);
    private boolean truncateBeforeLoad = (boolean) sourceConfig.getOrDefault("truncateBeforeLoad", true);
    private String lineQuery;
    private ArrayList<String> fileQuery;

    private static Faker faker = new Faker();

    public GenLoader() {
        this.instanceId = ManagementFactory.getRuntimeMXBean().getName();
    }

    private void doInsert(Connection connection) throws Exception {

        try {
            Statement statement = connection.createStatement();

            //Executing SQL query and fetching the result Statement fetchData = conn.createStatement(); // Execute the Insert command
            String insertQuery = "INSERT INTO " + tableName + " VALUES(?,?,?,?,?,?,?,?,?)";
            PreparedStatement preparedStmt = connection.prepareStatement(insertQuery, Statement.RETURN_GENERATED_KEYS);
            preparedStmt.setInt(1, count.incrementAndGet()); // empno
            preparedStmt.setString(2, faker.name().firstName()); // fname
            preparedStmt.setString(3, faker.name().lastName()); // lname
            preparedStmt.setString(4, faker.job().position()); // job
            preparedStmt.setInt(5, Integer.parseInt(faker.number().digits(2))); // mgr
            preparedStmt.setTimestamp(6, getRandomDOB()); // hiredate
            preparedStmt.setDouble(7, faker.number().randomDouble(2, 75000, 300000)); // sal
            preparedStmt.setDouble(8, faker.number().randomDouble(2, 5000, 100000)); // comm
            preparedStmt.setInt(9, Integer.parseInt(faker.number().digits(2))); // dept
            preparedStmt.executeUpdate();

            statement.close();
            preparedStmt.close();
        } catch (Exception e) {
            e.printStackTrace();
            throw new Exception(
                    "Error occurred while loading data from insert statements. "
                            + ExceptionUtils.getRootCauseMessage(e));
        }

    }

    public static Timestamp getRandomDOB() {
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.S");
        Faker faker = new Faker();

        return Timestamp.valueOf(sdf.format(faker.date().birthday()));
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

    private void runAll() {
        try {
            log.info("Instance: {} {} started with {} iteration(s).", ManagementFactory.getRuntimeMXBean().getName(), WHOAMI, iteration);
            Instant start = Instant.now();
            CoreConfig coreConfig = new CoreConfig();
            JDBCConnectionProvider JDBC_CONNECTION_PROVIDER = new JDBCConnectionProvider();
            connection = JDBC_CONNECTION_PROVIDER.getConnection(coreConfig.getConnectionId());

            if (truncateBeforeLoad) {
                doDeleteAll(connection);
            } else {
                log.info("Skipping truncate..");
            }

            for (int i = counter; i <= iteration; i++) {
                doInsert(connection);
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