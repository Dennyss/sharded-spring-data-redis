package org.oiavorskyi.ssdr;

/**
 * Value type that contains specification of shard instance
 */
public final class RedisShardSpec {


    private String masterName;
    private String host;
    private int    port;
    private int    db;

    private RedisShardSpec( String masterName, int db ) {
        this.masterName = masterName;
        this.db = db;
    }

    private RedisShardSpec( String host, int port, int db ) {
        this.host = host;
        this.port = port;
        this.db = db;
    }

    public static RedisShardSpec fromHostAndPort( String host, int port, int db ) {
        return new RedisShardSpec(host, port, db);
    }

    public static RedisShardSpec fromMasterName( String masterName, int db ) {
        return new RedisShardSpec(masterName, db);
    }

    public String getMasterName() {
        return masterName;
    }

    public String getHost() {
        return host;
    }

    public int getPort() {
        return port;
    }

    public int getDb() {
        return db;
    }

}
