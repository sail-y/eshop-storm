package com.roncoo.eshop.storm.bolt;

import com.alibaba.fastjson.JSON;
import com.roncoo.eshop.storm.http.HttpClientUtils;
import com.roncoo.eshop.storm.zk.ZooKeeperSession;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.trident.util.LRUMap;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.stream.Collectors;

/**
 * 商品访问次数统计bolt
 *
 * @author yangfan
 * @date 2018/03/02
 */
public class ProductCountBolt extends BaseRichBolt {
    private static final Logger LOGGER = LoggerFactory.getLogger(ProductCountBolt.class);

    private LRUMap<Long, Long> productCountMap = new LRUMap<>(1000);

    private ZooKeeperSession zkSession;

    private int taskId;

    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        new Thread(new ProductCountThread()).start();
        taskId = context.getThisTaskId();
        zkSession = ZooKeeperSession.getInstance();


        // 1. 将自己的taskId写入一个zookeeper node中
        // 2. 然后每次都将自己的热门商品列表，写入自己的taskId对应的zookeeper节点
        // 3. 这样并行的预热程序才能从第一步知道有哪些taskid
        // 4. 然后并行预热程序根据每个taskId去获取一个锁，然后再从对应的znode拿到热门商品列表
        initTaskId(context.getThisTaskId());
    }

    private void initTaskId(int taskId) {


        zkSession.acquireDistributedLock();

        String taskIdList = zkSession.getNodeData();

        LOGGER.info("【ProductCountBolt获取到taskid list】taskidList=" + taskIdList);

        if(!"".equals(taskIdList)) {
            taskIdList += "," + taskId;
        } else {
            taskIdList += taskId;
        }

        zkSession.createNode("/taskid-list");
        zkSession.setNodeData("/taskid-list", taskIdList);
        LOGGER.info("【ProductCountBolt设置taskid list】taskidList=" + taskIdList);

        zkSession.releaseDistributedLock();

    }


    private class HotProductFindThread implements Runnable {

        List<Map.Entry<Long, Long>> productCountList = new ArrayList<>();
        List<Long> lastTimeHotProductList = new ArrayList<>();
        List<Long> hotProductIdList = new ArrayList<>();


        @Override
        public void run() {
            while (true) {
                // 1. 将LRUMap中的数据按照访问次数进行全局的排序
                // 2. 计算95%的商品访问次数的平均值
                // 3. 遍历排序后的商品访问次数，降序
                // 4. 如果某个商品是平均访问量的10倍以上，就认为是缓存的热点
                if (productCountMap.size() == 0) {
                    Utils.sleep(100);
                    continue;
                }

                LOGGER.info("【ProductCountThread打印productCountMap的长度】size=" + productCountMap.size());

                Comparator<Map.Entry<Long, Long>> comparator = Comparator.comparingLong(Map.Entry::getValue);
                productCountList = productCountMap.entrySet().stream()
                        .sorted(comparator.reversed())
                        .collect(Collectors.toList());

                String productCountListJSON = JSON.toJSONString(productCountList);
                LOGGER.info("【HotProductFindThread计算出一份排序后的商品访问次数列表】 productCountListJSON=" + productCountListJSON);


                int percent5 = (int)Math.floor(productCountList.size() * 0.05);

                // 计算95%商品访问次数的平均值
                double avgCount = productCountList.stream().skip(percent5).mapToLong(Map.Entry::getValue).average().orElse(0.0);






                hotProductIdList = productCountList.stream()
                        .filter(m -> m.getValue() > 10 * avgCount)
                        .map(Map.Entry::getKey)
                        .peek(pId -> {
                            // 将缓存热点数据推送到流量分发的nginx中
                            String distributeURL = "http://192.168.2.203/hot?productId=" + pId;
                            HttpClientUtils.sendGetRequest(distributeURL);

                            // 将缓存热点对应商品发送到缓存服务去获取
                            String cacheServiceURL = "http://localhost:8080/getProductInfo?productId=" + pId;
                            String response = HttpClientUtils.sendGetRequest(cacheServiceURL);

                            String[] appNginxUrls = new String[]{
                                    "http://192.168.2.201/hot?productId=" + pId + "&productInfo=" + response,
                                    "http://192.168.2.202/hot?productId=" + pId + "&productInfo=" + response
                            };
                            // 将获取到的换成你数据推送到nginx服务上去
                            for (String appNginxUrl : appNginxUrls) {
                                HttpClientUtils.sendGetRequest(appNginxUrl);
                            }
                        })
                        .collect(Collectors.toList());



                if (lastTimeHotProductList.size() > 0) {
                    for (Long productId : lastTimeHotProductList) {
                        if (!hotProductIdList.contains(productId)) {
                            // 缓存热点消失，发送一个一个http请求到nginx取消热点缓存的标识
                            String url = "http://192.168.2.203/cancelHot?productId=" + productId;
                            HttpClientUtils.sendGetRequest(url);

                        }
                    }
                }



                lastTimeHotProductList = hotProductIdList;

            }
        }
    }


    /**
     * 每隔一分钟统计一次热点数据，放入zookeeper
     */
    private class ProductCountThread implements Runnable {

        List<Long> topnProductList = new ArrayList<>();


        @Override
        public void run() {


            while (true) {
                topnProductList.clear();

                if (productCountMap.size() == 0) {
                    Utils.sleep(100);
                    continue;
                }

                LOGGER.info("【ProductCountThread打印productCountMap的长度】size=" + productCountMap.size());

                Comparator<Map.Entry<Long, Long>> comparator = Comparator.comparingLong(Map.Entry::getValue);
                topnProductList = productCountMap.entrySet().stream()
                        .sorted(comparator.reversed())
                        .peek(e -> {
                            LOGGER.info("【productCountMap 排序后】" + e.getKey() + ":" + e.getValue());
                        })
                        .map(Map.Entry::getKey)
                        .limit(3).collect(Collectors.toList());


                String topnProductListJSON = JSON.toJSONString(topnProductList);


                zkSession.createNode("/task-hot-product-list-" + taskId);
                zkSession.setNodeData("/task-hot-product-list-" + taskId, topnProductListJSON);

                LOGGER.info("【ProductCountThread计算出一份top3热门商品列表】zk path=" + ("/task-hot-product-list-" + taskId) + ", topnProductListJSON=" + topnProductListJSON);

                Utils.sleep(5000);
            }
        }
    }

    public void execute(Tuple input) {

        Long productId = input.getLongByField("productId");
        LOGGER.info("【ProductCountBolt接收到一个商品id】 productId=" + productId);

        Long count = Optional.ofNullable(productCountMap.get(productId)).orElse(0L);

        productCountMap.put(productId, ++count);

        LOGGER.info("【ProductCountBolt完成商品访问次数统计】productId=" + productId + ", count=" + count);
    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {

    }
}
