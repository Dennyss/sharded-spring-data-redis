package org.oiavorskyi.ssdr;

import org.springframework.util.Assert;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

// TODO: Refactor to contain lenght of registry
// TODO: Add iterator
public class DefaultRedisShardSpecRegistry implements RedisShardSpecRegistry {

    private final Map<Integer, RedisShardSpec> specs = new ConcurrentHashMap<>();

    @Override
    public RedisShardSpec getShardSpecById( int shardId ) {
        Assert.isTrue(shardId >= 0, "shardId should be >= 0");

        RedisShardSpec spec = specs.get(shardId);

        Assert.state(spec != null, "specification for shard " + shardId + " could not " +
                "be found");

        return spec;
    }

    public void addSpecForShard( int shardId, RedisShardSpec spec ) {
        Assert.isTrue(shardId >= 0, "shardId should be >= 0");
        Assert.notNull(spec, "spec cannot be null");

        specs.put(shardId, spec);
    }
}
