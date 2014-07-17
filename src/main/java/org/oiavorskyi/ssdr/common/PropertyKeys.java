package org.oiavorskyi.ssdr.common;

/**
 * User: denys.kovalenko
 * Date: 5/23/14
 * Time: 9:14 AM
 */
public enum PropertyKeys {
    REDIS_LOCAL_HOST("redis.local.host"),
    REDIS_LOCAL_PORT("redis.local.port"),

    REDIS_SENTINEL1_HOST("redis.sentinel1.host"),
    REDIS_SENTINEL1_PORT("redis.sentinel1.port"),
    REDIS_MASTER("redis.master.name"),
    REDIS_PASSWORD("redis.password");

    private String propertyKey;

    PropertyKeys(String propertyKey){
        this.propertyKey = propertyKey;
    }

    public String getKey(){
        return propertyKey;
    }

}
