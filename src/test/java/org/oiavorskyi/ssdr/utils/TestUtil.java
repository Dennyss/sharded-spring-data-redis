package org.oiavorskyi.ssdr.utils;

import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPubSub;

import java.util.concurrent.atomic.AtomicReference;

/**
 * Created by Denys Kovalenko on 7/10/2014.
 */
public class TestUtil {

    public static HostAndPort waitForNewPromotedMaster(Jedis sentinel) throws InterruptedException {

        final AtomicReference<String> newMaster = new AtomicReference<String>("");

        sentinel.psubscribe(new JedisPubSub() {

            @Override
            public void onMessage(String channel, String message) {
            }

            @Override
            public void onPMessage(String pattern, String channel,
                                   String message) {
                if (channel.equals("+switch-master")) {
                    newMaster.set(message);
                    punsubscribe();
                } else if (channel.startsWith("-failover-abort")) {
                    punsubscribe();
                    throw new RuntimeException("Unfortunately sentinel cannot failover... reason(channel) : " +
                            channel + " / message : " + message);
                }
            }

            @Override
            public void onSubscribe(String channel, int subscribedChannels) {
            }

            @Override
            public void onUnsubscribe(String channel, int subscribedChannels) {
            }

            @Override
            public void onPUnsubscribe(String pattern, int subscribedChannels) {
            }

            @Override
            public void onPSubscribe(String pattern, int subscribedChannels) {
            }
        }, "*");

        String[] chunks = newMaster.get().split(" ");
        HostAndPort newMaster2 = new HostAndPort(chunks[3],
                Integer.parseInt(chunks[4]));

        return newMaster2;
    }


}
