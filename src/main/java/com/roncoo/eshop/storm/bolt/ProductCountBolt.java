package com.roncoo.eshop.storm.bolt;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.trident.util.LRUMap;
import org.apache.storm.tuple.Tuple;

import java.util.Map;
import java.util.Optional;

/**
 * 商品访问次数统计bolt
 *
 * @author yangfan
 * @date 2018/03/02
 */
public class ProductCountBolt extends BaseRichBolt {

    private LRUMap<Long, Long> productCountMap = new LRUMap<Long, Long>(1000);

    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {

    }

    public void execute(Tuple input) {
        Long productId = input.getLongByField("productId");

        Long count = Optional.ofNullable(productCountMap.get(productId)).orElse(0L);

        productCountMap.put(productId, ++count);
    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {

    }
}
