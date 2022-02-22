package com.redis.connect.crud.loader.core;

import lombok.Getter;

import java.text.SimpleDateFormat;

/**
 *
 * @author Virag Tripathi
 *
 */


@Getter
public enum DateTimeUtil {
    dd_MM_yyyy(new SimpleDateFormat("dd-MM-yyyy"), "dd-MM-yyyy"),
    yyyy_mm_dd(new SimpleDateFormat("yyyy-mm-dd"), "yyyy-mm-dd"),
    yyyy_MM_dd(new SimpleDateFormat("yyyy-MM-dd"), "yyyy-MM-dd"),
    yyyyMMdd(new SimpleDateFormat("yyyyMMdd"), "yyyyMMdd"),
    yyyy_MM_dd_HH_mm_ss(new SimpleDateFormat("yyyy-MM-dd HH:mm:ss"), "yyyy-MM-dd HH:mm:ss"),
    yyyy_MM_ddTHH_mm_ss(new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss"), "yyyy-MM-dd'T'HH:mm:ss"),
    yyyy_MM_ddTHH_mm_ssZ(new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ssZ"), "yyyy-MM-dd'T'HH:mm:ssZ");

    final SimpleDateFormat parser;
    private final String displayName;

    DateTimeUtil(SimpleDateFormat parser, String displayName) {
        this.parser = parser;
        this.displayName = displayName;
    }

    @Override
    public String toString() {
        return displayName;
    }

}