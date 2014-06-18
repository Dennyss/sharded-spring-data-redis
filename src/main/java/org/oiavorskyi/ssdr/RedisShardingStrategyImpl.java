package org.oiavorskyi.ssdr;

import redis.clients.jedis.Protocol;
import redis.clients.util.MurmurHash;

import java.io.UnsupportedEncodingException;
import java.util.SortedMap;
import java.util.TreeMap;

/**
 * Created by Denys Kovalenko on 6/18/2014.
 */
public class RedisShardingStrategyImpl implements RedisShardingStrategy<String> {
    private final int totalShardsNumber;
    private TreeMap<Long, Integer> shardsMap;

    public RedisShardingStrategyImpl(int totalShardsNumber){
        this.totalShardsNumber = totalShardsNumber;

        this.shardsMap = new TreeMap<>();
        Long initialHash = Long.MIN_VALUE/(totalShardsNumber/2);
        for(int i = 0; i < totalShardsNumber; i++){
            shardsMap.put(initialHash * i, i);
        }
    }

    @Override
    public int getShardsCount() {
        return totalShardsNumber;
    }

    @Override
    public int getShardIdByKey(String key) {
        // 1. Get bytes
        byte[] vinBytes;
        try {
            vinBytes = key.getBytes(Protocol.CHARSET);
        } catch (UnsupportedEncodingException e) {
            throw new RuntimeException(e.getMessage());
        }

        // 2. Get VIN hash
        MurmurHash murmurHash = new MurmurHash();
        long vinHash = murmurHash.hash(vinBytes);

        // 3. Get shard id from tail map of shardsMap by vinHash
        SortedMap<Long, Integer> tailMap = shardsMap.tailMap(vinHash);
        if (tailMap.isEmpty()) {
            return shardsMap.get(shardsMap.firstKey());
        }

        return tailMap.get(tailMap.firstKey());
    }
}
