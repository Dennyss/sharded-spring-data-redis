package org.oiavorskyi.ssdr.specification;

import java.util.HashSet;
import java.util.Set;

/**
 * Created by Denys Kovalenko on 7/16/2014.
 */
public class RedisSentinelSpec {
    private String masterName;
    private Set<String> sentinels;
    private String password;
    private int databaseIndex;

    private RedisSentinelSpec(String masterName, Set<String> sentinels, String password, int databaseIndex) {
        this.masterName = masterName;
        this.sentinels = sentinels;
        this.password = password;
        this.databaseIndex = databaseIndex;
    }

    public RedisSentinelSpec fromSentinelDetails(String masterName, Set<String> sentinels, String password, int databaseIndex){
        return new RedisSentinelSpec(masterName, sentinels, password, databaseIndex);
    }

    public static RedisSentinelSpec fromSentinelDetails(String masterName, String sentinel, String password, int databaseIndex){
        Set<String> sentinels = new HashSet<>();
        sentinels.add(sentinel);
        return new RedisSentinelSpec(masterName, sentinels, password, databaseIndex);
    }

    public RedisSentinelSpec addSentinel(String sentinel){
        sentinels.add(sentinel);
        return this;
    }

    public String getMasterName() {
        return masterName;
    }

    public Set<String> getSentinels() {
        return sentinels;
    }

    public String getPassword() {
        return password;
    }

    public int getDatabaseIndex() {
        return databaseIndex;
    }
}
