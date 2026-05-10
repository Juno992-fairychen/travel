package com.travel.common;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.net.HttpURLConnection;
import java.net.URL;
import java.net.URLConnection;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

/**
 * 地图相关操作工具方法
 */
public class MapUtil {
    private static final ObjectMapper mapper = new ObjectMapper();

    static {
        mapper.configure(com.fasterxml.jackson.databind.DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
    }

    private final static Logger logger = LoggerFactory.getLogger(MapUtil.class);

    // 高德地图开发者key（需要在高德地图开发者中心注册获取）
    private static String key = "83c27aa19fbaa78cc0336efaf2118bfe";

    /**
     * 高德api 根据经纬度获取地址
     */
    public static String getAdd(String lng, String lat) {
        String res = sendPost("https://restapi.amap.com/v3/geocode/regeo", "key=" + key + "&location=" + lng + "," + lat);
        logger.info(res);
        try {
            JsonNode jsonNode = mapper.readTree(res);
            JsonNode regeocodeNode = jsonNode.get("regeocode");
            return regeocodeNode.get("formatted_address").asText();
        } catch (Exception e) {
            logger.error("解析JSON失败", e);
            return "";
        }
    }

    /**
     * 高德api 获取城市县区边界
     */
    public static ArrayNode getDistricts(String districtName, String districtCode) {
        String s = "key=" + key + "&keywords=" + districtName + "&subdistrict=1&extensions=all";
        String res = sendGet("https://restapi.amap.com/v3/config/district", s);
        logger.info(res);
        try {
            JsonNode jsonNode = mapper.readTree(res);
            return (ArrayNode) jsonNode.get("districts");
        } catch (Exception e) {
            logger.error("解析JSON失败", e);
            return mapper.createArrayNode();
        }
    }

    public static void parseDistrictInfo(ArrayNode districts, String targeCitycode, List<District> districtList) {
        if (null != districts && !districts.isEmpty()) {
            Iterator<JsonNode> iterator = districts.iterator();
            while (iterator.hasNext()) {
                JsonNode next = iterator.next();

                String name = next.get("name").asText();
                String citycode = next.has("citycode") ? next.get("citycode").asText() : null;
                String polyline = next.has("polyline") ? next.get("polyline").asText() : null;
                String level = next.get("level").asText();
                logger.warn("name:{}", name);
                logger.warn("polyline:{}", polyline);
                logger.warn("citycode:{}", citycode);

                if (level.equals("city")) {
                    ArrayNode district = (ArrayNode) next.get("districts");
                    parseDistrictInfo(district, null, districtList);
                } else if (level.equals("district")) {
                    if (null != targeCitycode && targeCitycode.equals(citycode)) {
                        districtList.add(new District(citycode, name, polyline));
                        return;
                    }

                    if (null == polyline) {
                        ArrayNode cityDistrict = getDistricts(name, null);
                        parseDistrictInfo(cityDistrict, citycode, districtList);
                    }
                }
            }
        }
    }

    /**
     * 高德api 根据经纬度获取所在城市（替换原阿里云API）
     */
    public static String getCity(String lng, String lat) {
        String res = sendGet("https://restapi.amap.com/v3/geocode/regeo", "key=" + key + "&location=" + lng + "," + lat);
        logger.info(res);
        try {
            JsonNode jsonNode = mapper.readTree(res);
            JsonNode regeocodeNode = jsonNode.get("regeocode");
            JsonNode addressComponent = regeocodeNode.get("addressComponent");
            String city = addressComponent.get("city").asText();
            // 如果city为空（可能是直辖市），则使用province
            if (city == null || city.isEmpty()) {
                city = addressComponent.get("province").asText();
            }
            return city;
        } catch (Exception e) {
            logger.error("解析JSON失败", e);
            return "";
        }
    }

    /**
     * 高德api 根据地址获取经纬度
     */
    public static String getLatAndLogByName(String name) {
        String res = sendPost("https://restapi.amap.com/v3/geocode/geo", "key=" + key + "&address=" + name);
        logger.info(res);
        try {
            JsonNode jsonNode = mapper.readTree(res);
            ArrayNode geocodes = (ArrayNode) jsonNode.get("geocodes");
            if (geocodes == null || geocodes.isEmpty()) {
                return "";
            }
            JsonNode location = geocodes.get(0);
            return location.get("location").asText();
        } catch (Exception e) {
            logger.error("解析JSON失败", e);
            return "";
        }
    }

    /**
     * 高德api 根据经纬度获取地址（逆地理编码）
     */
    public static String getAddByAMAP(String lng, String lat) {
        String res = sendPost("https://restapi.amap.com/v3/geocode/regeo", "key=" + key + "&location=" + lng + "," + lat);
        logger.info(res);
        try {
            JsonNode jsonNode = mapper.readTree(res);
            JsonNode regeocodeNode = jsonNode.get("regeocode");
            return regeocodeNode.get("formatted_address").asText();
        } catch (Exception e) {
            logger.error("解析JSON失败", e);
            return "";
        }
    }

    /**
     * 高德api 坐标转换
     */
    public static String convertLocations(String lng, String lat, String type) {
        StringBuilder s = new StringBuilder();
        s.append("key=").append(key).append("&locations=").append(lng).append(",").append(lat).append("&coordsys=");
        if (type == null) {
            s.append("gps");
        } else {
            s.append(type);
        }
        String res = sendPost("https://restapi.amap.com/v3/assistant/coordinate/convert", s.toString());
        logger.info(res);
        try {
            JsonNode jsonNode = mapper.readTree(res);
            return jsonNode.get("locations").asText();
        } catch (Exception e) {
            logger.error("解析JSON失败", e);
            return "";
        }
    }

    /**
     * 高德api 根据地址获取附近位置（替换原阿里云API）
     */
    public static String getAddByName(String name) {
        // 先通过地理编码获取经纬度
        String latAndLng = getLatAndLogByName(name);
        if (latAndLng == null || latAndLng.isEmpty()) {
            return "";
        }
        String[] parts = latAndLng.split(",");
        if (parts.length < 2) {
            return "";
        }
        String lng = parts[0];
        String lat = parts[1];
        return getNearbyAdd(lng, lat);
    }

    public static String getNearbyAdd(String lng, String lat) {
        String res = sendGet("https://restapi.amap.com/v3/geocode/regeo", "key=" + key + "&location=" + lng + "," + lat);
        logger.info(res);
        try {
            JsonNode jsonNode = mapper.readTree(res);
            JsonNode regeocodeNode = jsonNode.get("regeocode");
            return regeocodeNode.get("formatted_address").asText();
        } catch (Exception e) {
            logger.error("解析JSON失败", e);
            return "";
        }
    }

    /**
     * 高德api 关键字模糊查询
     */
    public static String getKeywordsAddByLbs(String keyWord, String city) {
        StringBuilder s = new StringBuilder();
        s.append("key=").append(key).append("&keywords=");
        if (keyWord.contains(" ")) {
            String[] str = keyWord.split(" ");
            for (int i = 0; i < str.length; i++) {
                if (i == 0) {
                    s.append(str[i]);
                } else {
                    s.append("+").append(str[i]);
                }
            }
        } else {
            s.append(keyWord);
        }
        s.append("&city=").append(city);
        s.append("&offset=10&page=1");
        String around = sendPost("https://restapi.amap.com/v3/place/text", s.toString());
        logger.info(around);
        return around;
    }

    /**
     * 高德api 周边搜索
     */
    public static String getAroundAddByLbs(String lng, String lat, String keyWord) {
        String around = sendPost("https://restapi.amap.com/v3/place/around",
                "key=" + key + "&location=" + lng + "," + lat + "&keywords=" + keyWord +
                        "&radius=2000&offset=10&page=1");
        logger.info(around);
        return around;
    }

    public static String sendGet(String url, String param) {
        String result = "";
        BufferedReader in = null;
        try {
            String urlNameString = url + "?" + param;
            URL realUrl = new URL(urlNameString);
            URLConnection connection = realUrl.openConnection();
            connection.setRequestProperty("accept", "*/*");
            connection.setRequestProperty("connection", "Keep-Alive");
            connection.setRequestProperty("user-agent", "Mozilla/4.0 (compatible; MSIE 6.0; Windows NT 5.1;SV1)");
            connection.connect();

            // 遍历响应头字段（调试用）
            Map<String, List<String>> map = connection.getHeaderFields();
            for (String key : map.keySet()) {
                logger.info(key + "--->" + map.get(key));
            }

            in = new BufferedReader(new InputStreamReader(connection.getInputStream()));
            String line;
            StringBuilder sb = new StringBuilder();
            while ((line = in.readLine()) != null) {
                sb.append(line);
            }
            result = sb.toString();
        } catch (Exception e) {
            logger.info("发送GET请求出现异常！" + e);
            e.printStackTrace();
        } finally {
            try {
                if (in != null) {
                    in.close();
                }
            } catch (Exception e2) {
                e2.printStackTrace();
            }
        }
        return result;
    }

    /**
     * 向指定 URL 发送POST方法的请求
     */
    public static String sendPost(String url, String param) {
        PrintWriter out = null;
        BufferedReader in = null;
        String result = "";
        try {
            URL realUrl = new URL(url);
            URLConnection conn = realUrl.openConnection();
            conn.setRequestProperty("accept", "*/*");
            conn.setRequestProperty("connection", "Keep-Alive");
            conn.setRequestProperty("user-agent", "Mozilla/4.0 (compatible; MSIE 6.0; Windows NT 5.1;SV1)");
            conn.setDoOutput(true);
            conn.setDoInput(true);
            out = new PrintWriter(conn.getOutputStream());
            out.print(param);
            out.flush();
            in = new BufferedReader(new InputStreamReader(conn.getInputStream()));
            String line;
            StringBuilder sb = new StringBuilder();
            while ((line = in.readLine()) != null) {
                sb.append(line);
            }
            result = sb.toString();
        } catch (Exception e) {
            logger.info("发送 POST 请求出现异常！" + e);
            e.printStackTrace();
        } finally {
            try {
                if (out != null) {
                    out.close();
                }
                if (in != null) {
                    in.close();
                }
            } catch (IOException ex) {
                ex.printStackTrace();
            }
        }
        return result;
    }

    /**
     * GET请求数据
     */
    public static String sendGetData(String get_url, String content) throws Exception {
        String result = "";
        URL getUrl = null;
        BufferedReader reader = null;
        String lines = "";
        HttpURLConnection connection = null;
        try {
            if (content != null && !content.isEmpty())
                get_url = get_url + "?" + content;
            getUrl = new URL(get_url);
            connection = (HttpURLConnection) getUrl.openConnection();
            connection.connect();
            reader = new BufferedReader(new InputStreamReader(connection.getInputStream(), "utf-8"));
            StringBuilder sb = new StringBuilder();
            while ((lines = reader.readLine()) != null) {
                sb.append(lines);
            }
            result = sb.toString();
            return result;
        } finally {
            if (reader != null) {
                reader.close();
            }
            if (connection != null) {
                connection.disconnect();
            }
        }
    }

    /**
     * POST请求数据
     */
    public static String sendPostData(String POST_URL, String content) throws Exception {
        HttpURLConnection connection = null;
        DataOutputStream out = null;
        BufferedReader reader = null;
        String line = "";
        String result = "";
        try {
            URL postUrl = new URL(POST_URL);
            connection = (HttpURLConnection) postUrl.openConnection();
            connection.setDoOutput(true);
            connection.setDoInput(true);
            connection.setRequestMethod("POST");
            connection.setUseCaches(false);
            connection.setInstanceFollowRedirects(true);
            connection.setRequestProperty("Content-Type", "application/x-www-form-urlencoded");
            connection.connect();

            out = new DataOutputStream(connection.getOutputStream());
            out.writeBytes(content);
            out.flush();
            out.close();
            reader = new BufferedReader(new InputStreamReader(connection.getInputStream(), "utf-8"));
            StringBuilder sb = new StringBuilder();
            while ((line = reader.readLine()) != null) {
                sb.append(line);
            }
            result = sb.toString();
            return result;
        } finally {
            if (out != null) {
                out.close();
            }
            if (reader != null) {
                reader.close();
            }
            if (connection != null) {
                connection.disconnect();
            }
        }
    }

    /**
     * 过滤HTML不安全标签
     */
    public static String htmlFilter(String inputString) {
        String htmlStr = inputString;
        String textStr = "";

        try {
            String regEx_script = "<[\\s]*?(script|style)[^>]*?>[\\s\\S]*?<[\\s]*?\\/[\\s]*?(script|style)[\\s]*?>";
            String regEx_onevent = "on[^\\s]+=\\s*";
            String regEx_hrefjs = "href=javascript:";
            String regEx_iframe = "<[\\s]*?(iframe|frameset)[^>]*?>[\\s\\S]*?<[\\s]*?\\/[\\s]*?(iframe|frameset)" +
                    "[\\s]*?>";
            String regEx_link = "<[\\s]*?link[^>]*?/>";

            htmlStr = Pattern.compile(regEx_script, Pattern.CASE_INSENSITIVE).matcher(htmlStr).replaceAll("");
            htmlStr = Pattern.compile(regEx_onevent, Pattern.CASE_INSENSITIVE).matcher(htmlStr).replaceAll("");
            htmlStr = Pattern.compile(regEx_hrefjs, Pattern.CASE_INSENSITIVE).matcher(htmlStr).replaceAll("");
            htmlStr = Pattern.compile(regEx_iframe, Pattern.CASE_INSENSITIVE).matcher(htmlStr).replaceAll("");
            htmlStr = Pattern.compile(regEx_link, Pattern.CASE_INSENSITIVE).matcher(htmlStr).replaceAll("");

            textStr = htmlStr;

        } catch (Exception e) {
            System.err.println("Html2Text: " + e.getMessage());
        }

        return textStr;
    }

    public static void main(String[] args) {
        List<District> districtList = new ArrayList<>();
        ArrayNode districts = getDistricts("海口市", null);
        parseDistrictInfo(districts, null, districtList);
        try {
            logger.warn(mapper.writeValueAsString(districtList));
        } catch (Exception e) {
            logger.error("序列化失败", e);
        }
    }
}