package com.travel.common;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.DeserializationFeature;
import org.apache.commons.lang3.StringUtils;
import org.apache.log4j.Logger;
import redis.clients.jedis.*;
import redis.clients.jedis.params.SortingParams;
import redis.clients.jedis.util.SafeEncoder;

import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class JedisUtil {

    private final static Logger log = Logger.getLogger(JedisUtil.class);

    private final int expire = 60000;

    public Keys KEYS;
    public Strings STRINGS;
    public Lists LISTS;
    public Sets SETS;
    public Hash HASH;
    public SortSet SORTSET;

    private static JedisPool jedisPool = null;
    private static final JedisUtil jedisUtil = new JedisUtil();
    private static final ObjectMapper mapper = new ObjectMapper();

    static {
        mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
    }

    static {
        JedisPoolConfig config = new JedisPoolConfig();
        config.setMaxTotal(200);
        config.setMaxIdle(50);
        config.setMinIdle(10);
        config.setMaxWaitMillis(30000);
        config.setTestOnBorrow(true);
        config.setTestOnReturn(true);

        String jedisHost = ConfigUtil.getConfig(Constants.JEDIS_HOST);
        int jedisPort = Integer.parseInt(ConfigUtil.getConfig(Constants.JEDIS_PORT));
        String jedisPassword = ConfigUtil.getConfig(Constants.JEDIS_PASS);
        int connectTimeOut = 30000;

        System.out.println("=== JedisUtil 初始化 ===");
        System.out.println("JEDIS_HOST: " + jedisHost);
        System.out.println("JEDIS_PORT: " + jedisPort);
        System.out.println("JEDIS_PASS: " + jedisPassword);
        System.out.println("======================");

        jedisPool = new JedisPool(config, jedisHost, jedisPort, connectTimeOut, jedisPassword);
    }

    public static JedisPool getJedisPool() {
        return jedisPool;
    }

    public static Jedis getJedis() {
        return jedisPool.getResource();
    }

    public static void returnJedis(Jedis jedis) {
        if (jedis != null) {
            jedis.close();
        }
    }

    public static void saveChengDuJedis(String line) throws Exception {
        Jedis jedis = null;
        try {
            jedis = getJedis();
            String[] split = line.split(",");
            if (split.length < 2) {
                return;
            }
            String orderId = split[1];

            if (line.startsWith("end") && line.contains(",")) {
                jedis.lpush(Constants.CITY_CODE_CHENG_DU + "_" + orderId, "end");
                jedis.srem(Constants.REALTIME_ORDERS, Constants.CITY_CODE_CHENG_DU + "_" + orderId);
            } else {
                if (split.length < 5) {
                    return;
                }
                String driverId = split[0];
                String timestamp = split[2];
                String lng = split[3];
                String lat = split[4];

                jedis.sadd(Constants.REALTIME_ORDERS, Constants.CITY_CODE_CHENG_DU + "_" + orderId);
                jedis.lpush(Constants.CITY_CODE_CHENG_DU + "_" + orderId, lng + "," + lat);

                Order order = new Order();
                String hget = jedis.hget(Constants.ORDER_START_ENT_TIME, orderId);

                if (StringUtils.isNotEmpty(hget)) {
                    Order parseOrder = mapper.readValue(hget, Order.class);

                    if (Long.parseLong(timestamp) * 1000 > parseOrder.getEndTime()) {
                        parseOrder.setEndTime(Long.parseLong(timestamp) * 1000);
                        parseOrder.setGetOfLat(lat);
                        parseOrder.setGetOfLng(lng);
                        jedis.hset(Constants.ORDER_START_ENT_TIME, orderId, mapper.writeValueAsString(parseOrder));
                    } else if (Long.parseLong(timestamp) * 1000 < parseOrder.getStartTime()) {
                        parseOrder.setStartTime(Long.parseLong(timestamp) * 1000);
                        parseOrder.setGetOnLat(lat);
                        parseOrder.setGetOnLng(lng);
                        jedis.hset(Constants.ORDER_START_ENT_TIME, orderId, mapper.writeValueAsString(parseOrder));
                    }
                } else {
                    order.setGetOnLat(lat);
                    order.setGetOnLng(lng);
                    order.setCityCode(Constants.CITY_CODE_CHENG_DU);
                    order.setGetOfLng(lng);
                    order.setGetOfLat(lat);
                    order.setEndTime(Long.parseLong(timestamp + "000"));
                    order.setStartTime(Long.parseLong(timestamp + "000"));
                    order.setOrderId(orderId);
                    jedis.hset(Constants.ORDER_START_ENT_TIME, orderId, mapper.writeValueAsString(order));
                }
                hourOrderCount(orderId, timestamp);
            }
        } finally {
            returnJedis(jedis);
        }
    }

    private static void hourOrderCount(String orderId, String timestamp) {
        String hourOrder = Constants.CITY_CODE_CHENG_DU + "_order";
        String hourOrderCountTab = Constants.CITY_CODE_CHENG_DU + "_" + DateUtils.formateDate(timestamp, "yyyy-MM-dd") + "_hour_order_count";
        String hourOrderField = Constants.CITY_CODE_CHENG_DU + "_" + DateUtils.formateDate(timestamp, "yyyy-MM-dd") + "_" + DateUtils.getHour(timestamp);

        Jedis jedis = null;
        try {
            jedis = getJedis();
            if (!jedis.sismember(hourOrder, orderId)) {
                jedis.sadd(hourOrder, orderId);
                String hourOrdernum = jedis.hget(hourOrderCountTab, hourOrderField);
                int hourOrderCount = 1;
                if (StringUtils.isNotEmpty(hourOrdernum)) {
                    hourOrderCount = Integer.parseInt(hourOrdernum) + 1;
                }
                jedis.hset(hourOrderCountTab, hourOrderField, String.valueOf(hourOrderCount));
            }
        } finally {
            returnJedis(jedis);
        }
    }

    public static JedisUtil getInstance() {
        return jedisUtil;
    }

    public void expire(String key, int seconds) {
        if (seconds <= 0) {
            return;
        }
        try (Jedis jedis = getJedis()) {
            jedis.expire(key, seconds);
        }
    }

    public void expire(String key) {
        expire(key, expire);
    }

    // ==================== Keys ====================
    public class Keys {
        public String flushAll() {
            try (Jedis jedis = getJedis()) {
                return jedis.flushAll();
            }
        }

        public String rename(String oldkey, String newkey) {
            try (Jedis jedis = getJedis()) {
                return jedis.rename(oldkey, newkey);
            }
        }

        public long renamenx(String oldkey, String newkey) {
            try (Jedis jedis = getJedis()) {
                return jedis.renamenx(oldkey, newkey);
            }
        }

        public long expired(String key, int seconds) {
            try (Jedis jedis = getJedis()) {
                return jedis.expire(key, seconds);
            }
        }

        public long expireAt(String key, long timestamp) {
            try (Jedis jedis = getJedis()) {
                return jedis.expireAt(key, timestamp);
            }
        }

        public long ttl(String key) {
            try (Jedis jedis = getJedis()) {
                return jedis.ttl(key);
            }
        }

        public long persist(String key) {
            try (Jedis jedis = getJedis()) {
                return jedis.persist(key);
            }
        }

        public long del(String... keys) {
            try (Jedis jedis = getJedis()) {
                return jedis.del(keys);
            }
        }

        public boolean exists(String key) {
            try (Jedis jedis = getJedis()) {
                return jedis.exists(key);
            }
        }

        public List<String> sort(String key) {
            try (Jedis jedis = getJedis()) {
                return jedis.sort(key);
            }
        }

        public List<String> sort(String key, SortingParams parame) {
            try (Jedis jedis = getJedis()) {
                return jedis.sort(key, parame);
            }
        }

        public String type(String key) {
            try (Jedis jedis = getJedis()) {
                return jedis.type(key);
            }
        }

        public Set<String> keys(String pattern) {
            try (Jedis jedis = getJedis()) {
                return jedis.keys(pattern);
            }
        }
    }

    // ==================== Sets ====================
    public class Sets {
        public long sadd(String key, String member) {
            try (Jedis jedis = getJedis()) {
                return jedis.sadd(key, member);
            }
        }

        public long scard(String key) {
            try (Jedis jedis = getJedis()) {
                return jedis.scard(key);
            }
        }

        public Set<String> sdiff(String... keys) {
            try (Jedis jedis = getJedis()) {
                return jedis.sdiff(keys);
            }
        }

        public long sdiffstore(String newkey, String... keys) {
            try (Jedis jedis = getJedis()) {
                return jedis.sdiffstore(newkey, keys);
            }
        }

        public Set<String> sinter(String... keys) {
            try (Jedis jedis = getJedis()) {
                return jedis.sinter(keys);
            }
        }

        public long sinterstore(String newkey, String... keys) {
            try (Jedis jedis = getJedis()) {
                return jedis.sinterstore(newkey, keys);
            }
        }

        public boolean sismember(String key, String member) {
            try (Jedis jedis = getJedis()) {
                return jedis.sismember(key, member);
            }
        }

        public Set<String> smembers(String key) {
            try (Jedis jedis = getJedis()) {
                return jedis.smembers(key);
            }
        }

        public long smove(String srckey, String dstkey, String member) {
            try (Jedis jedis = getJedis()) {
                return jedis.smove(srckey, dstkey, member);
            }
        }

        public String spop(String key) {
            try (Jedis jedis = getJedis()) {
                return jedis.spop(key);
            }
        }

        public long srem(String key, String member) {
            try (Jedis jedis = getJedis()) {
                return jedis.srem(key, member);
            }
        }

        public Set<String> sunion(String... keys) {
            try (Jedis jedis = getJedis()) {
                return jedis.sunion(keys);
            }
        }

        public long sunionstore(String newkey, String... keys) {
            try (Jedis jedis = getJedis()) {
                return jedis.sunionstore(newkey, keys);
            }
        }
    }

    // ==================== SortSet ====================
    public class SortSet {
        public long zadd(String key, double score, String member) {
            try (Jedis jedis = getJedis()) {
                return jedis.zadd(key, score, member);
            }
        }

        public long zadd(String key, Map<String, Double> scoreMembers) {
            try (Jedis jedis = getJedis()) {
                return jedis.zadd(key, scoreMembers);
            }
        }

        public long zcard(String key) {
            try (Jedis jedis = getJedis()) {
                return jedis.zcard(key);
            }
        }

        public long zcount(String key, double min, double max) {
            try (Jedis jedis = getJedis()) {
                return jedis.zcount(key, min, max);
            }
        }

        public long zlength(String key) {
            Set<String> set = zrange(key, 0, -1);
            return set.size();
        }

        public double zincrby(String key, double score, String member) {
            try (Jedis jedis = getJedis()) {
                return jedis.zincrby(key, score, member);
            }
        }

        public Set<String> zrange(String key, int start, int end) {
            try (Jedis jedis = getJedis()) {
                List<String> list = jedis.zrange(key, start, end);
                return new LinkedHashSet<>(list);
            }
        }

        public Set<String> zrangeByScore(String key, double min, double max) {
            try (Jedis jedis = getJedis()) {
                List<String> resultList = jedis.zrangeByScore(key, min, max);
                return new LinkedHashSet<>(resultList);
            }
        }

        public long zrank(String key, String member) {
            try (Jedis jedis = getJedis()) {
                return jedis.zrank(key, member);
            }
        }

        public long zrevrank(String key, String member) {
            try (Jedis jedis = getJedis()) {
                return jedis.zrevrank(key, member);
            }
        }

        public long zrem(String key, String member) {
            try (Jedis jedis = getJedis()) {
                return jedis.zrem(key, member);
            }
        }

        public long zrem(String key) {
            try (Jedis jedis = getJedis()) {
                return jedis.del(key);
            }
        }

        public long zremrangeByRank(String key, int start, int end) {
            try (Jedis jedis = getJedis()) {
                return jedis.zremrangeByRank(key, start, end);
            }
        }

        public long zremrangeByScore(String key, double min, double max) {
            try (Jedis jedis = getJedis()) {
                return jedis.zremrangeByScore(key, min, max);
            }
        }

        public Set<String> zrevrange(String key, int start, int end) {
            try (Jedis jedis = getJedis()) {
                List<String> list = jedis.zrevrange(key, start, end);
                return new LinkedHashSet<>(list);
            }
        }

        public double zscore(String key, String memebr) {
            try (Jedis jedis = getJedis()) {
                Double score = jedis.zscore(key, memebr);
                return score != null ? score : 0;
            }
        }
    }

    // ==================== Hash ====================
    public class Hash {
        public long hdel(String key, String fieid) {
            try (Jedis jedis = getJedis()) {
                return jedis.hdel(key, fieid);
            }
        }

        public long hdel(String key) {
            try (Jedis jedis = getJedis()) {
                return jedis.del(key);
            }
        }

        public boolean hexists(String key, String fieid) {
            try (Jedis jedis = getJedis()) {
                return jedis.hexists(key, fieid);
            }
        }

        public String hget(String key, String fieid) {
            try (Jedis jedis = getJedis()) {
                return jedis.hget(key, fieid);
            }
        }

        public Map<String, String> hgetAll(String key) {
            try (Jedis jedis = getJedis()) {
                return jedis.hgetAll(key);
            }
        }

        public long hset(String key, String fieid, String value) {
            try (Jedis jedis = getJedis()) {
                return jedis.hset(key, fieid, value);
            }
        }

        public long hsetnx(String key, String fieid, String value) {
            try (Jedis jedis = getJedis()) {
                return jedis.hsetnx(key, fieid, value);
            }
        }

        public List<String> hvals(String key) {
            try (Jedis jedis = getJedis()) {
                return jedis.hvals(key);
            }
        }

        public long hincrby(String key, String fieid, long value) {
            try (Jedis jedis = getJedis()) {
                return jedis.hincrBy(key, fieid, value);
            }
        }

        public Set<String> hkeys(String key) {
            try (Jedis jedis = getJedis()) {
                return jedis.hkeys(key);
            }
        }

        public long hlen(String key) {
            try (Jedis jedis = getJedis()) {
                return jedis.hlen(key);
            }
        }

        public List<String> hmget(String key, String... fieids) {
            try (Jedis jedis = getJedis()) {
                return jedis.hmget(key, fieids);
            }
        }

        public String hmset(String key, Map<String, String> map) {
            try (Jedis jedis = getJedis()) {
                return jedis.hmset(key, map);
            }
        }
    }

    // ==================== Strings ====================
    public class Strings {
        public String get(String key) {
            try (Jedis jedis = getJedis()) {
                return jedis.get(key);
            }
        }

        public String setEx(String key, int seconds, String value) {
            try (Jedis jedis = getJedis()) {
                return jedis.setex(key, seconds, value);
            }
        }

        public long setnx(String key, String value) {
            try (Jedis jedis = getJedis()) {
                return jedis.setnx(key, value);
            }
        }

        public String set(String key, String value) {
            try (Jedis jedis = getJedis()) {
                return jedis.set(key, value);
            }
        }

        public long setRange(String key, long offset, String value) {
            try (Jedis jedis = getJedis()) {
                return jedis.setrange(key, offset, value);
            }
        }

        public long append(String key, String value) {
            try (Jedis jedis = getJedis()) {
                return jedis.append(key, value);
            }
        }

        public long decrBy(String key, long number) {
            try (Jedis jedis = getJedis()) {
                return jedis.decrBy(key, number);
            }
        }

        public long incrBy(String key, long number) {
            try (Jedis jedis = getJedis()) {
                return jedis.incrBy(key, number);
            }
        }

        public String getrange(String key, long startOffset, long endOffset) {
            try (Jedis jedis = getJedis()) {
                return jedis.getrange(key, startOffset, endOffset);
            }
        }

        public String getSet(String key, String value) {
            try (Jedis jedis = getJedis()) {
                return jedis.getSet(key, value);
            }
        }

        public List<String> mget(String... keys) {
            try (Jedis jedis = getJedis()) {
                return jedis.mget(keys);
            }
        }

        public String mset(String... keysvalues) {
            try (Jedis jedis = getJedis()) {
                return jedis.mset(keysvalues);
            }
        }

        public long strlen(String key) {
            try (Jedis jedis = getJedis()) {
                return jedis.strlen(key);
            }
        }
    }

    // ==================== Lists ====================
    public class Lists {
        public long llen(String key) {
            try (Jedis jedis = getJedis()) {
                return jedis.llen(key);
            }
        }

        public String lset(String key, int index, String value) {
            try (Jedis jedis = getJedis()) {
                return jedis.lset(key, index, value);
            }
        }

        public String lindex(String key, int index) {
            try (Jedis jedis = getJedis()) {
                return jedis.lindex(key, index);
            }
        }

        public String lpop(String key) {
            try (Jedis jedis = getJedis()) {
                return jedis.lpop(key);
            }
        }

        public String rpop(String key) {
            try (Jedis jedis = getJedis()) {
                return jedis.rpop(key);
            }
        }

        public long lpush(String key, String value) {
            try (Jedis jedis = getJedis()) {
                return jedis.lpush(key, value);
            }
        }

        public long rpush(String key, String value) {
            try (Jedis jedis = getJedis()) {
                return jedis.rpush(key, value);
            }
        }

        public List<String> lrange(String key, long start, long end) {
            try (Jedis jedis = getJedis()) {
                return jedis.lrange(key, start, end);
            }
        }

        public long lrem(String key, long count, String value) {
            try (Jedis jedis = getJedis()) {
                return jedis.lrem(key, count, value);
            }
        }

        public String ltrim(String key, int start, int end) {
            try (Jedis jedis = getJedis()) {
                return jedis.ltrim(key, start, end);
            }
        }
    }

    public static void main(String[] args) {
        Jedis jedis = JedisUtil.getInstance().getJedis();
        jedis.set("Abc", "abc");
        byte[] hget = jedis.hget(Constants.ORDER_START_ENT_TIME.getBytes(),
                "001e4acd0f513b796745340142f38b11".getBytes());
        System.out.println(new String(hget));
        jedis.close();
    }
}