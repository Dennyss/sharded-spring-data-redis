package org.oiavorskyi.ssdr;

/**
 * Translates shard ID into physical parameters such as host/port,
 * master name (for Sentinel), credentials, etc.
 */
public interface RedisShardSpecRegistry {

    public RedisShardSpec getShardSpecById(int shardId);

}
