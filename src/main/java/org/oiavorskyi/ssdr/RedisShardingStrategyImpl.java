package org.oiavorskyi.ssdr;

import org.springframework.util.Assert;
import redis.clients.util.MurmurHash;


/**
 * Created by Denys Kovalenko on 6/18/2014.
 */
public class RedisShardingStrategyImpl implements RedisShardingStrategy<String> {
    private final int totalShardsNumber;

    public RedisShardingStrategyImpl(int totalShardsNumber) {
        Assert.isTrue(totalShardsNumber > 0, "totalShardsNumber should be positive");
        this.totalShardsNumber = totalShardsNumber;
    }

    @Override
    public int getShardsCount() {
        return totalShardsNumber;
    }

    @Override
    public int getShardIdByKey(String key) {
        Assert.notNull(key, "Key should not be null");
        Assert.isTrue(!key.trim().isEmpty(), "Key should not be empty");

        // Get positive murmur hash of key
        MurmurHash murmurHash = new MurmurHash();
        long vinHash = Math.abs(murmurHash.hash(key));

        // Calculate shard id
        return (int) vinHash % totalShardsNumber;
    }
}
