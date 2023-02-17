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
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.Statement;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.time.Duration;
import java.time.Instant;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author Virag Tripathi
 */
@Getter
@Setter
@Slf4j
@CommandLine.Command(name = "genloader",
        description = "Load data into database tables using java faker.")
public class GenLoader implements Runnable {

    private final String instanceId;
    private static Connection connection;

    private static final String WHOAMI = "GenLoader";
    private static final Map<String, Object> sourceConfig = LoaderConfig.INSTANCE.getEnvConfig().getConnection("source");
    private static final String tableName = (String) sourceConfig.getOrDefault("tableName", "emp");
    private int iteration = (int) sourceConfig.getOrDefault("iteration", 1);
    private static int counter = (int) sourceConfig.getOrDefault("counter", 1);
    private static final AtomicInteger count = new AtomicInteger(0);
    private boolean truncateBeforeLoad = (boolean) sourceConfig.getOrDefault("truncateBeforeLoad", true);

    private static Faker faker = new Faker();

    public GenLoader() {
        this.instanceId = ManagementFactory.getRuntimeMXBean().getName();
    }

    private void doEmpInsert(Connection connection) throws Exception {

        try {

            String insertQuery = "INSERT INTO " + tableName + " VALUES(?,?,?,?,?,?,?,?,?)";

            PreparedStatement preparedStmt = connection.prepareStatement(insertQuery);
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

            preparedStmt.close();
        } catch (Exception e) {
            e.printStackTrace();
            throw new Exception(
                    "Error occurred while loading data from insert statements. "
                            + ExceptionUtils.getRootCauseMessage(e));
        }

    }

    /*
    @CommandLine.Command(name = "beers")
    public void doBeersInsert(Connection connection) throws Exception {

        try {

            String insertQuery = "INSERT INTO " + tableName + " VALUES(?,?,?,?,?,?,?,?,?,?,?,?)";

            PreparedStatement preparedStmt = connection.prepareStatement(insertQuery);
            preparedStmt.setInt(1, count.incrementAndGet()); // id
            preparedStmt.setInt(2, faker.random().nextInt(1, 1000000)); // brewery_id
            preparedStmt.setString(3, faker.beer().name()); // name
            preparedStmt.setDouble(4, faker.number().randomDouble(1, 1, 15)); // abv i.e. alcohol by volume
            preparedStmt.setInt(5, faker.random().nextInt(5, 120)); // ibu i.e. International Bitterness Unit
            preparedStmt.setInt(6, faker.random().nextInt(1, 60)); // srm i.e. Standard Reference Method
            preparedStmt.setString(7, null); // upc i.e.
            preparedStmt.setString(8, null); // filepath
            preparedStmt.setString(9, null); // description
            preparedStmt.setString(10, null); // add_user
            preparedStmt.setString(11, faker.beer().style()); // style_name
            preparedStmt.setString(12, faker.beer().style()); // cat_name

            preparedStmt.executeUpdate();

            preparedStmt.close();
        } catch (Exception e) {
            e.printStackTrace();
            throw new Exception(
                    "Error occurred while loading data from insert statements. "
                            + ExceptionUtils.getRootCauseMessage(e));
        }
    }
    */

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

            log.info("\n[Performing INSERT in {} with {} rows] ... ", tableName, iteration);

            for (int i = counter; i <= iteration; i++) {
                doEmpInsert(connection);
                //doBeersInsert(connection);
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