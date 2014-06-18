package org.oiavorskyi.ssdr;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

/**
 * Created by Denys Kovalenko on 6/18/2014.
 */
public class RedisShardingStrategyTest {


    @Test
    public void shardingLogicTest(){
        String[] keys = {
                "1C4RJFDJ2EC169760",
                "1C4RJFDJ2EC169761",
                "1C4RJFDJ2EC169762",
                "1C4RJFDJ2EC169763",
                "1C4RJFDJ2EC169764",
                "1C4RJFDJ2EC169765",
                "1C4RJFDJ2EC169766",
                "1C4RJFDJ2EC169767",
                "1C4RJFDJ2EC169768",
                "1C4RJFDJ2EC169769"
        };

        RedisShardingStrategy redisShardingStrategy = new RedisShardingStrategyImpl(keys.length);

        // Null check
        for(String key : keys){
            assertNotNull(redisShardingStrategy.getShardIdByKey(key));
        }

        // Consistency check, invoke the same method with the same parameter 20 times
        int firstResult = redisShardingStrategy.getShardIdByKey(keys[0]);
        for(int i = 0; i < 20; i++){
            assertEquals(firstResult, redisShardingStrategy.getShardIdByKey(keys[0]));
        }

    }
}
