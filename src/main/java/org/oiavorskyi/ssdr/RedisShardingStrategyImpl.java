package org.oiavorskyi.ssdr;

import redis.clients.util.MurmurHash;


/**
 * Created by Denys Kovalenko on 6/18/2014.
 */
public class RedisShardingStrategyImpl implements RedisShardingStrategy<String> {
    private final int totalShardsNumber;

    public RedisShardingStrategyImpl(int totalShardsNumber) {
        if (totalShardsNumber <= 0) {
            throw new IllegalArgumentException("totalShardsNumber should be positive");
        }

        this.totalShardsNumber = totalShardsNumber;
    }

    @Override
    public int getShardsCount() {
        return totalShardsNumber;
    }

    @Override
    public int getShardIdByKey(String key) {
        if (key == null) {
            throw new NullPointerException("Key should not be null");
        }

        if (key.trim().isEmpty()) {
            throw new IllegalArgumentException("Key should not be empty");
        }

        // Get positive murmur hash of key
        MurmurHash murmurHash = new MurmurHash();
        long vinHash = Math.abs(murmurHash.hash(key));

        // Calculate shard id
        return (int) vinHash % totalShardsNumber;
    }
}
