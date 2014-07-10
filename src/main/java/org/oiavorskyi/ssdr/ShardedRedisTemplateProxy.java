package org.oiavorskyi.ssdr;

import org.springframework.beans.factory.InitializingBean;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.serializer.RedisSerializer;
import org.springframework.util.Assert;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Provides abstraction on top of RedisTemplate that supports transparent sharding of operations
 */
public class ShardedRedisTemplateProxy<K, V> implements InitializingBean {

    private RedisShardingStrategy<K>         shardingStrategy;
    private ShardedConnectionFactoryProvider connectionFactoryProvider;
    private RedisSerializer<?>               keySerializer;

    private RedisSerializer<?> valueSerializer;

    private Map<Integer, RedisTemplate<K, V>> templates;

    public int getShardIdForKey( K key ) {
        return shardingStrategy.getShardIdByKey(key);
    }

    public RedisTemplate<K, V> template( K key ) {
        return template(getShardIdForKey(key));
    }

    public RedisTemplate<K, V> template( int shardId ) {
        Assert.notNull(templates, "ensure that method afterPropertiesSet was called before this " +
                "one");

        return templates.get(shardId);
    }

    @Override
    public void afterPropertiesSet() throws Exception {
        Assert.notNull(shardingStrategy, "Sharding strategy should be set prior to usage of the " +
                "instance");
        Assert.notNull(connectionFactoryProvider, "Connection factory provider should be set " +
                "prior to usage of the instance");

        templates = new ConcurrentHashMap<>(shardingStrategy.getShardsCount());

        for ( int i = 0; i < shardingStrategy.getShardsCount(); i++ ) {
            RedisTemplate<K, V> template = new RedisTemplate<>();
            template.setConnectionFactory(connectionFactoryProvider.getConnectionFactory(i));
            if ( keySerializer != null ) {
                template.setKeySerializer(keySerializer);
            }
            if ( valueSerializer != null ) {
                template.setValueSerializer(valueSerializer);
            }
            // Additional parameters should be added as pass-through
            template.afterPropertiesSet();
            templates.put(i, template);
        }

    }

    public void setShardingStrategy( RedisShardingStrategy<K> shardingStrategy ) {
        this.shardingStrategy = shardingStrategy;
    }

    public void setConnectionFactoryProvider(
            ShardedConnectionFactoryProvider connectionFactoryProvider ) {
        this.connectionFactoryProvider = connectionFactoryProvider;
    }

    public void setKeySerializer( RedisSerializer<?> keySerializer ) {
        this.keySerializer = keySerializer;
    }

    public void setValueSerializer( RedisSerializer<?> valueSerializer ) {
        this.valueSerializer = valueSerializer;
    }
}
