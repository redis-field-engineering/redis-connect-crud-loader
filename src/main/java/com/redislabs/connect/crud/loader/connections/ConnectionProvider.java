package com.redislabs.connect.crud.loader.connections;

import java.util.Properties;

/**
 *
 * @author Virag Tripathi
 *
 */

public interface ConnectionProvider<Connection> {
    Connection getConnection(String connectionId);
    default Connection getConnection(String connectionId, Properties properties) {
        throw new RuntimeException("getConnection(connectionId,properties) implementation not available in " + this.getClass().getName());
    }
}