package com.travel.controller;

import com.travel.common.JedisUtil;
import org.json.JSONObject;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.ResponseBody;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.text.NumberFormat;
import java.text.SimpleDateFormat;
import java.util.*;

@SpringBootApplication
@Controller
@RequestMapping("didiRedis")
public class SearchRedisController {

    @ResponseBody
    @RequestMapping(value = "/monitoring", method = RequestMethod.POST)
    public String monitoring(HttpServletRequest request, HttpServletResponse response) throws Exception {
        // 获取连接redis
        JedisUtil jedisUtil = JedisUtil.getInstance();
        JedisUtil.Keys keysMethod = jedisUtil.new Keys();
        JedisUtil.Strings strings = jedisUtil.new Strings();

        // keys方法
        Set<String> keys = keysMethod.keys("MONITOR_DP*");
        List<Long> timeStampList = new ArrayList<>();

        if (keys == null || keys.isEmpty()) {
            return null;
        }

        for (String str : keys) {
            String monitor_dp = str.replace("MONITOR_DP", "");
            long timeStamp = Long.parseLong(monitor_dp);
            timeStampList.add(timeStamp);
        }

        // 降序
        Collections.sort(timeStampList);
        Collections.reverse(timeStampList);

        Map<String, List<Object>> taskDetailMap = new HashMap<>();
        List<Object> listDetails = new ArrayList<>();

        int len = Math.min(timeStampList.size(), 5);

        for (int i = 0; i < len; i++) {
            String timeStamp = timeStampList.get(i).toString();
            String key = "MONITOR_DP" + timeStamp;
            String value = strings.get(key);

            JSONObject jsonObject = new JSONObject(value);

            Object activeJobs = jsonObject.opt("activeJobs");
            Object allJobs = jsonObject.opt("allJobs");
            Object runningStages = jsonObject.opt("runningStages");
            Object waitingStages = jsonObject.opt("waitingStages");
            Object runningBatches = jsonObject.opt("runningBatches");
            Object totalCompletedBatches = jsonObject.opt("totalCompletedBatches");
            Object totalReceivedRecords = jsonObject.opt("totalReceivedRecords");
            Object unprocessedBatches = jsonObject.opt("unprocessedBatches");

            listDetails.add(activeJobs);
            listDetails.add(allJobs);
            listDetails.add(runningStages);
            listDetails.add(waitingStages);
            listDetails.add(runningBatches);
            listDetails.add(totalCompletedBatches);
            listDetails.add(totalReceivedRecords);
            listDetails.add(unprocessedBatches);

            long currentTime = Long.parseLong(timeStamp);
            SimpleDateFormat formatter = new SimpleDateFormat("yyyy年-MM月dd日-HH时mm分ss秒");
            Date date = new Date(currentTime);
            String timeFormat = formatter.format(date);
            System.out.println(timeFormat);
            taskDetailMap.put(timeFormat, new ArrayList<>(listDetails));
        }

        JSONObject resultJson = new JSONObject();
        resultJson.put("任务详情", taskDetailMap);

        // 最新数据
        String latestData = strings.get("MONITOR_DP" + timeStampList.get(0).toString());
        JSONObject latestJson = new JSONObject(latestData);

        int failedStages = latestJson.optInt("failedStages", 0);
        int runningStagesVal = latestJson.optInt("runningStages", 0);
        int waitingStagesVal = latestJson.optInt("waitingStages", 0);
        int runningBatchesVal = latestJson.optInt("runningBatches", 0);
        int waitingBatchesVal = latestJson.optInt("waitingBatches", 0);
        int totalCompletedBatchesVal = latestJson.optInt("totalCompletedBatches", 0);
        int maxMemMB = latestJson.optInt("maxMem_MB", 0);
        int memUsedMB = latestJson.optInt("memUsed_MB", 0);
        int remainingMemMB = latestJson.optInt("remainingMem_MB", 0);

        NumberFormat numberFormat = NumberFormat.getInstance();
        numberFormat.setMaximumFractionDigits(2);

        // stage 情况
        float errorRate;
        float operatingRate;
        float waitingRate;

        int totalStages = failedStages + runningStagesVal + waitingStagesVal;
        if (totalStages == 0) {
            errorRate = 0f;
            operatingRate = 0f;
            waitingRate = 0f;
        } else {
            errorRate = Float.parseFloat(numberFormat.format((float) failedStages / totalStages * 100));
            operatingRate = Float.parseFloat(numberFormat.format((float) runningStagesVal / totalStages * 100));
            waitingRate = Float.parseFloat(numberFormat.format((float) waitingStagesVal / totalStages * 100));
        }

        // 批次情况
        float proportionOfBatchesInOperation;
        float waitingBatchProportion;
        float ratioOfCompletedBatches;

        int totalBatches = runningBatchesVal + waitingBatchesVal + totalCompletedBatchesVal;
        if (totalBatches == 0) {
            proportionOfBatchesInOperation = 0f;
            waitingBatchProportion = 0f;
            ratioOfCompletedBatches = 0f;
        } else {
            proportionOfBatchesInOperation = Float.parseFloat(numberFormat.format((float) runningBatchesVal / totalBatches * 100));
            waitingBatchProportion = Float.parseFloat(numberFormat.format((float) waitingBatchesVal / totalBatches * 100));
            ratioOfCompletedBatches = Float.parseFloat(numberFormat.format((float) totalCompletedBatchesVal / totalBatches * 100));
        }

        // 内存情况
        float memUsedMBProportion;
        float remainingMemMBProportion;
        float maxMemMBProportion;

        int totalMem = memUsedMB + remainingMemMB;
        if (totalMem == 0) {
            memUsedMBProportion = 0f;
            remainingMemMBProportion = 0f;
            maxMemMBProportion = 0f;
        } else {
            memUsedMBProportion = Float.parseFloat(numberFormat.format((float) memUsedMB / totalMem * 100));
            remainingMemMBProportion = Float.parseFloat(numberFormat.format((float) remainingMemMB / totalMem * 100));
            maxMemMBProportion = Float.parseFloat(numberFormat.format((float) maxMemMB / totalMem * 100));
        }

        // 构建返回数据
        List<Float> stageSituation = new ArrayList<>();
        stageSituation.add(errorRate);
        stageSituation.add(operatingRate);
        stageSituation.add(waitingRate);

        List<Float> batchSituation = new ArrayList<>();
        batchSituation.add(proportionOfBatchesInOperation);
        batchSituation.add(waitingBatchProportion);
        batchSituation.add(ratioOfCompletedBatches);

        List<Float> memSituation = new ArrayList<>();
        memSituation.add(memUsedMBProportion);
        memSituation.add(remainingMemMBProportion);
        memSituation.add(maxMemMBProportion);

        List<Object> overview = new ArrayList<>();
        overview.add(failedStages);
        overview.add(runningStagesVal);
        overview.add(waitingStagesVal);
        overview.add(runningBatchesVal);
        overview.add(waitingBatchesVal);
        overview.add(totalCompletedBatchesVal);
        overview.add(maxMemMB);
        overview.add(remainingMemMBProportion);
        overview.add(remainingMemMB);

        resultJson.put("stageSituation", stageSituation);
        resultJson.put("batchSituation", batchSituation);
        resultJson.put("mBSituation", memSituation);
        resultJson.put("概况", overview);

        return resultJson.toString();
    }
}