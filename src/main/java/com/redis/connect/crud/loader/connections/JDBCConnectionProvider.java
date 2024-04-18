package com.redis.connect.crud.loader.connections;

import com.redis.connect.crud.loader.config.LoaderConfig;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import lombok.extern.slf4j.Slf4j;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

/**
 *
 * @author Virag Tripathi
 *
 */

@Slf4j
public class JDBCConnectionProvider implements ConnectionProvider<Connection> {

    private final Map<String,HikariDataSource> DATA_SOURCE_MAP = new HashMap<>();
    private Connection connection = null;


    public Connection getConnection(String connectionId) {
        HikariDataSource dataSource = DATA_SOURCE_MAP.get(connectionId);
        if(dataSource == null) {
            Map<String,Object> databaseConfig = LoaderConfig.INSTANCE.getEnvConfig().getConnection("source");
            HikariConfig jdbcConfig = new HikariConfig();
            jdbcConfig.setPoolName((String)databaseConfig.get("name"));
            jdbcConfig.setJdbcUrl((String) databaseConfig.get("jdbcUrl"));
            jdbcConfig.setUsername((String) databaseConfig.get("username"));
            jdbcConfig.setPassword((String) databaseConfig.get("password"));
            jdbcConfig.setMaximumPoolSize((Integer)databaseConfig.getOrDefault("maximumPoolSize", 10));
            jdbcConfig.setMinimumIdle((Integer)databaseConfig.getOrDefault("minimumIdle", 2));
            if (((String) databaseConfig.get("jdbcUrl")).contains("postgresql"))
                jdbcConfig.addDataSourceProperty("stringtype", "unspecified");
            dataSource = new HikariDataSource(jdbcConfig);
            DATA_SOURCE_MAP.put(connectionId,dataSource);
        }
        try {
            connection = dataSource.getConnection();
        } catch (SQLException e) {
            e.printStackTrace();
            log.error(e.getMessage());
        }
        return connection;
    }
}
