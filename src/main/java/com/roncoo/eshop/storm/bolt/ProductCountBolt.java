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

import java.util.*;
import java.util.stream.Collectors;

/**
 * 商品访问次数统计bolt
 *
 * @author yangfan
 * @date 2018/03/02
 */
public class ProductCountBolt extends BaseRichBolt {

    private LRUMap<Long, Long> productCountMap = new LRUMap<>(1000);

    private ZooKeeperSession zkSession;

    private int taskId;

    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        new Thread(new ProductCountThread());
        taskId = context.getThisTaskId();

        // 1. 将自己的taskId写入一个zookeeper node中
        // 2. 然后每次都将自己的热门商品列表，写入自己的taskId对应的zookeeper节点
        // 3. 这样并行的预热程序才能从第一步知道有哪些taskid
        // 4. 然后并行预热程序根据每个taskId去获取一个锁，然后再从对应的znode拿到热门商品列表
        initTaskId(context.getThisTaskId());
        zkSession = ZooKeeperSession.getInstance();
    }

    private void initTaskId(int taskId) {
        String path = "/taskid-list";


        zkSession.acquireDistributedLock(path);

        String taskIdList = zkSession.getNodeData(path);

        if (!"".equals(taskIdList)) {
            taskIdList += "," + taskId;
        } else {
            taskIdList += taskIdList;
        }

        zkSession.setNodeData(path, taskIdList);


        zkSession.releaseDistributedLock(path);

    }

    /**
     * 每隔一分钟统计一次热点数据，放入zookeeper
     */
    private class ProductCountThread implements Runnable {

        List<Map.Entry> topnProductList = new ArrayList<>();


        @Override
        public void run() {


            while (true) {
                topnProductList.clear();

                if (productCountMap.size() == 0) {
                    Utils.sleep(100);
                    continue;
                }

                topnProductList = productCountMap.entrySet().stream()
                        .sorted(Comparator.comparingLong(Map.Entry::getValue))
                        .limit(3).collect(Collectors.toList());


                zkSession.setNodeData("/task-hot-product-list-" + taskId, JSON.toJSONString(topnProductList));

                Utils.sleep(5000);
            }
        }
    }

    public void execute(Tuple input) {
        Long productId = input.getLongByField("productId");

        Long count = Optional.ofNullable(productCountMap.get(productId)).orElse(0L);

        productCountMap.put(productId, ++count);
    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {

    }
}
