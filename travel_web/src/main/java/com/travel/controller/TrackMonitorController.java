package com.travel.controller;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.travel.common.*;
import com.travel.modules.*;
import com.travel.service.OrderService;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.web.bind.annotation.*;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.ScanParams;
import redis.clients.jedis.ScanResult;

import javax.servlet.http.HttpServletRequest;
import java.util.*;

/**
 * 轨迹相关控制器
 */
@SpringBootApplication
@RestController
@RequestMapping("/track")
public class TrackMonitorController {
    private static final ObjectMapper mapper = new ObjectMapper();
    static {
        mapper.configure(com.fasterxml.jackson.databind.DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
    }

    private final static Logger logger = LoggerFactory.getLogger(TrackMonitorController.class);

    @Autowired
    OrderService orderService;

    // ==================== 原有接口 ====================

    /**
     * 实时订单
     */
    @PostMapping("/realtimeOrder")
    public ResultModel<List<String>> realtimeOrder(@RequestBody QueryWrapper wrapper, HttpServletRequest request) {
        long startTime = System.currentTimeMillis();
        logger.info("【查询实时轨迹...】");
        ResultModel<List<String>> result = new ResultModel<List<String>>();
        try {
            JedisUtil jedisUtil = JedisUtil.getInstance();
            Jedis jedis = JedisUtil.getJedis();
            ScanParams scanParams = new ScanParams();
            String result222 = wrapper.getCityCode() + "_";
            System.out.println(result222);
            result222 = "075";
            scanParams.match(result222 + "_*");
            ScanResult<String> sscan = jedis.sscan(Constants.REALTIME_ORDERS, "0", scanParams);
            List<String> orders = sscan.getResult();
            List<String> frontOrders = new ArrayList<String>();
            for (String order : orders) {
                List<String> lrange = jedis.lrange(order, 0, -1);
                if(!lrange.isEmpty()){
                    frontOrders.add(order);
                }
            }
            result.setSuccess(true);
            result.setData(frontOrders);
            JedisUtil.returnJedis(jedis);
        } catch (Exception e) {
            result.setMsg(e.getMessage());
        }
        logger.info("【查询实时轨迹】msg:{},time:{}", result.getMsg(),
                System.currentTimeMillis() - startTime);
        return result;
    }

    /**
     * 查询订单实时轨迹点
     */
    @PostMapping("/realTimeTrack")
    public ResultModel<String> realTimeTrack(@RequestBody QueryWrapper wrapper) {
        long startTime = System.currentTimeMillis();
        logger.info("【查询实时轨迹】");
        ResultModel<String> result = new ResultModel<String>();
        try {
            JedisUtil.getInstance();
            Jedis jedis = JedisUtil.getJedis();

            String orderId = wrapper.getOrderId();
            logger.info("原始订单ID: {}", orderId);

            // 自动补全前缀：如果是32位纯订单号，自动添加 "075_"
            String redisKey = orderId;
            if (orderId != null && orderId.length() == 32 && !orderId.startsWith("075_")) {
                redisKey = "075_" + orderId;
                logger.info("自动补全前缀: {}", redisKey);
            }

            List<String> points = jedis.lrange(redisKey, -1, -1);
            String lngAndlat = points.isEmpty() ? null : points.get(0);

            result.setSuccess(true);
            result.setData(lngAndlat);
            JedisUtil.returnJedis(jedis);
        } catch (Exception e) {
            logger.error("查询实时轨迹失败: {}", e.getMessage());
            result.setMsg(e.getMessage());
        }
        logger.info("【查询实时轨迹】msg:{},time:{}", result.getMsg(),
                System.currentTimeMillis() - startTime);
        return result;
    }

    @PostMapping("/historyTrackPoints")
    public ResultModel<List<TrackPoint>> historyTrackPoints(@RequestBody QueryWrapper wrapper) {
        long startTime = System.currentTimeMillis();
        logger.info("【查询图形(点线面)】");
        ResultModel<List<TrackPoint>> result = new ResultModel<List<TrackPoint>>();
        List<TrackPoint> list = null;
        try {
            String orderId = wrapper.getOrderId();

            if (StringUtils.isEmpty(orderId)) {
                result.setSuccess(true);
                result.setData(null);
                return result;
            }

            // 提取纯订单ID（去掉前缀）
            String pureOrderId = orderId;
            if (orderId.startsWith("075_")) {
                pureOrderId = orderId.substring(4);
            }
            logger.info("原始订单ID: {}, 纯订单ID: {}", orderId, pureOrderId);

            JedisUtil instance = JedisUtil.getInstance();
            Jedis jedis = instance.getJedis();

            // 用纯订单ID从Redis获取订单起止时间
            String hget = jedis.hget(Constants.ORDER_START_ENT_TIME, pureOrderId);
            System.out.println(hget);

            if(StringUtils.isNotEmpty(hget)){
                Order order = mapper.readValue(hget, Order.class);
                String starttime = order.getStartTime() + "";
                String enttime = order.getEndTime() + "";

                String tableName = Constants.HTAB_GPS;
                // 用纯订单ID查询HBase
                list = HBaseUtil.getRest(tableName, pureOrderId, starttime, enttime, TrackPoint.class);
                logger.info("查询到轨迹点数量: {}", list != null ? list.size() : 0);
            }

            result.setSuccess(true);
            result.setData(list);
            jedis.close();

        } catch (Exception e) {
            logger.error("查询历史轨迹失败: {}", e.getMessage());
            result.setMsg(e.getMessage());
        }
        logger.info("【查询订单历史轨迹点】msg:{},time:{}", result.getMsg(),
                System.currentTimeMillis() - startTime);
        return result;
    }

    /**
     * 订单概况
     */
    @PostMapping("/orderOverview")
    public ResultModel<OrderOverview> orderOverview(@RequestBody QueryWrapper wrapper) {
        long startTime = System.currentTimeMillis();
        logger.info("【查询订单概况信息】");
        ResultModel<OrderOverview> result = new ResultModel<OrderOverview>();

        try {
            OrderOverview orderOverview = new OrderOverview();
            Jedis jedis = JedisUtil.getInstance().getJedis();

            String day = wrapper.getStartTime().split(" ")[0];
            String orderCountStr = jedis.hget(Constants.ORDER_COUNT,
                    wrapper.getCityCode() + "_" + day);

            String passengerCountStr = jedis.hget(Constants.PASSENGER_COUNT,
                    wrapper.getCityCode() + "_" + day);

            String hourOrderCountTab = wrapper.getCityCode() + "_" + day + "hourOrderCount";
            hourOrderCountTab = "075_2016-10-10_hour_order_count";
            Map<String, String> ordercountMap = jedis.hgetAll("075_2016-10-10_hour_order_count");
            Collection<String> values = ordercountMap.values();
            int ordercount = 0;
            for (String str : values) {
                ordercount += Integer.parseInt(str);
            }

            if (ordercount > 0) {
                orderOverview.setOrderCount(ordercount);
            } else {
                orderOverview.setOrderCount(Integer.parseInt(orderCountStr));
            }
            if (null != passengerCountStr) {
                orderOverview.setOrderPassengerCount(Integer.parseInt(passengerCountStr));
                orderOverview.setOrderPassengerCount(283);
            }
            JedisUtil.returnJedis(jedis);
            result.setSuccess(true);
            result.setData(orderOverview);
        } catch (Exception e) {
            result.setMsg(e.getMessage());
        }
        logger.info("【查询订单概况信息】msg:{},time:{}", result.getMsg(),
                System.currentTimeMillis() - startTime);
        return result;
    }

    /**
     * 小时订单统计
     */
    @PostMapping("/hourOrderCount")
    public ResultModel<Map<String, String>> hourOrderCount(@RequestBody QueryWrapper wrapper) {
        long startTime = System.currentTimeMillis();
        logger.info("【查询订单概况信息】");
        ResultModel<Map<String, String>> result = new ResultModel<Map<String, String>>();

        try {
            String day = wrapper.getStartTime().substring(0, wrapper.getStartTime().indexOf(" "));
            String hourOrderCountTab = wrapper.getCityCode() + "_" + day + "_hour_order_count";
            hourOrderCountTab = "075_2016-10-10_hour_order_count";
            JedisUtil.getInstance();
            Jedis jedis = JedisUtil.getJedis();
            Map<String, String> stringStringMap = jedis.hgetAll(hourOrderCountTab);

            result.setSuccess(true);
            result.setData(stringStringMap);
            JedisUtil.returnJedis(jedis);
        } catch (Exception e) {
            e.printStackTrace();
            result.setMsg(e.getMessage());
        }
        logger.info("【查询订单概况信息】msg:{},time:{}", result.getMsg(),
                System.currentTimeMillis() - startTime);
        return result;
    }

    /**
     * 虚拟车站
     */
    @PostMapping("/virtualStations")
    public ResultModel<List<VirtualStationsVo>> virtualStations(@RequestBody StationWrapper wrapper) {
        long startTime = System.currentTimeMillis();
        logger.info("【查询订单概况信息】");
        ResultModel<List<VirtualStationsVo>> result = new ResultModel<List<VirtualStationsVo>>();
        try {
            List<VirtualStationsVo> virtualStationsVos = orderService.queryVirtualStations(wrapper);
            result.setSuccess(true);
            result.setData(virtualStationsVos);
        } catch (Exception e) {
            result.setMsg(e.getMessage());
        }
        logger.info("【查询虚拟车站信息】msg:{},time:{}", result.getMsg(),
                System.currentTimeMillis() - startTime);
        return result;
    }

    /**
     * 查询虚拟车站统计信息
     */
    @PostMapping("/virtualStationCount")
    public ResultModel<List<VirtualStationCountVo>> virtualStationCount(@RequestBody StationWrapper wrapper) {
        long startTime = System.currentTimeMillis();
        logger.info("【查询订单概况信息】");
        ResultModel<List<VirtualStationCountVo>> result = new ResultModel<List<VirtualStationCountVo>>();
        try {
            String city_code = wrapper.getCityCode();
            System.out.println(city_code);
            List<VirtualStationCountVo> list = orderService.virtualStationCount(wrapper.getCityCode());
            result.setSuccess(true);
            result.setData(list);
        } catch (Exception e) {
            result.setMsg(e.getMessage());
        }

        logger.info("【查询虚拟车站统计信息】msg:{},time:{}", result.getMsg(),
                System.currentTimeMillis() - startTime);
        return result;
    }
    //=====新增接口=====
    @GetMapping("/{_:.*}")
    public String redirect() {
        return "forward:/index.html";
    }
}