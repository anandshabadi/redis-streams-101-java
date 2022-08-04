package com.kanibl.redis.streams.simple;

import io.lettuce.core.*;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.sync.RedisCommands;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import redis.clients.jedis.Connection;
import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.JedisPooled;
import redis.clients.jedis.StreamEntryID;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public class RedisStreams101Producer {

    public final static String STREAMS_KEY = "weather_sensor:wind";
    private static final HostAndPort hnp = HostAndPorts.getRedisServers().get(0);

    public static void main(String[] args) {

        int nbOfMessageToSend = 5;

        if (args != null && args.length != 0 ) {
            nbOfMessageToSend = Integer.valueOf(args[0]);
        }
        GenericObjectPoolConfig<Connection> config = new GenericObjectPoolConfig<>();
        config.setMaxTotal(1);
        config.setBlockWhenExhausted(false);

        System.out.println( String.format("\n Sending %s message(s)", nbOfMessageToSend));
        JedisPooled jedis = new JedisPooled(hnp,config);

        //RedisClient redisClient = RedisClient.create("redis://localhost:6379"); // change to reflect your environment
        //StatefulRedisConnection<String, String> connection = redisClient.connect();
        //RedisCommands<String, String> syncCommands = connection.sync();

        for (int i = 0 ; i < nbOfMessageToSend ; i++) {

            Map<String, String> messageBody = new HashMap<>();
            messageBody.put("speed", "15");
            messageBody.put("direction", "270");
            messageBody.put("sensor_ts", String.valueOf(System.currentTimeMillis()));
            messageBody.put("loop_info", String.valueOf( i ));

            //String messageId = syncCommands.xadd(
                    //STREAMS_KEY,
                    //messageBody);

            StreamEntryID messageId = jedis.xadd(
                    STREAMS_KEY,
                    new StreamEntryID(System.nanoTime()),
                    messageBody);
            System.out.println(String.format("\tMessage %s : %s posted", messageId.toString(), messageBody));
        }

        System.out.println("\n");
        jedis.close();
        //connection.close();
        //redisClient.shutdown();

    }




}
