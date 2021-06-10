package com.redislabs.connect.crud.loader.core;

import lombok.Getter;
import lombok.Setter;

/**
 *
 * @author Virag Tripathi
 *
 */

@Getter
@Setter
public class CoreConfig {
    private String providerId;
    private String connectionId;
    private String source;
    private int batchSize;
}