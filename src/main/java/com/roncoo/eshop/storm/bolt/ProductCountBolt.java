package com.roncoo.eshop.storm.bolt;

import com.alibaba.fastjson.JSON;
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
