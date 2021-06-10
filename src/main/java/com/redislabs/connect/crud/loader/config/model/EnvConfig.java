package com.redislabs.connect.crud.loader.config.model;

import lombok.Getter;
import lombok.Setter;

import java.util.HashMap;
import java.util.Map;

/**
 *
 * @author Virag Tripathi
 *
 */

@Getter
@Setter
public class EnvConfig {
    private Map<String,Map<String,Object>> connections = new HashMap<>();

    public Map<String,Object> getConnection(String connectionId) {
        return connections.get(connectionId);
    }

}