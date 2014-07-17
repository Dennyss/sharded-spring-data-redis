package org.oiavorskyi.ssdr.proxy;

import org.oiavorskyi.ssdr.provider.JedisConnectionFactoryProvider;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.serializer.RedisSerializer;
import org.springframework.util.Assert;


/**
 * Created by Denys Kovalenko on 7/16/2014.
 */
public class RedisTemplateProxy<K, V> implements InitializingBean {
    private JedisConnectionFactoryProvider connectionFactoryProvider;
    private RedisSerializer<?> keySerializer;
    private RedisSerializer<?> valueSerializer;
    private RedisTemplate<K, V> template;


    public RedisTemplate<K, V> getTemplate() {
        Assert.notNull(template, "ensure that method afterPropertiesSet was called before this one");

        return template;
    }

    @Override
    public void afterPropertiesSet() throws Exception {
        Assert.notNull(connectionFactoryProvider, "Connection factory provider should be set prior to usage of the instance");

        template = new RedisTemplate<>();
        template.setConnectionFactory(connectionFactoryProvider.createConnectionFactory());
        if (keySerializer != null) {
            template.setKeySerializer(keySerializer);
        }
        if (valueSerializer != null) {
            template.setValueSerializer(valueSerializer);
        }
        // Additional parameters should be added as pass-through
        template.afterPropertiesSet();
    }


    public void setConnectionFactoryProvider(JedisConnectionFactoryProvider connectionFactoryProvider) {
        this.connectionFactoryProvider = connectionFactoryProvider;
    }

    public void setKeySerializer( RedisSerializer<?> keySerializer ) {
        this.keySerializer = keySerializer;
    }

    public void setValueSerializer( RedisSerializer<?> valueSerializer ) {
        this.valueSerializer = valueSerializer;
    }
}
