package com.roncoo.eshop.storm.spout;

import kafka.consumer.Consumer;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ArrayBlockingQueue;

/**
 *
 * kafka接受数据的spout
 *
 * @author yangfan
 * @date 2018/03/02
 */
public class AccessLogKafkaSpout extends BaseRichSpout {

    private static final Logger LOGGER = LoggerFactory.getLogger(AccessLogKafkaSpout.class);

    private SpoutOutputCollector collector;

    private ArrayBlockingQueue<String> queue = new ArrayBlockingQueue<String>(1000);

    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
        this.collector = collector;
        startKafkaConsumer();
    }

    private void startKafkaConsumer() {
        String topic = "access-log";

        Properties props = new Properties();
        props.put("zookeeper.connect", "192.168.2.201:2181,192.168.2.202:2181,192.168.2.203:2181");
        props.put("group.id", "eshop-cache-group");
        props.put("zookeeper.session.timeout.ms", "4000");
        props.put("zookeeper.sync.time.ms", "200");
        props.put("auto.commit.interval.ms", "1000");
        ConsumerConfig consumerConfig = new ConsumerConfig(props);

        ConsumerConnector consumerConnector = Consumer.createJavaConsumerConnector(consumerConfig);


        Map<String, Integer> topicCountMap = new HashMap<>();
        topicCountMap.put(topic, 1);

        Map<String, List<KafkaStream<byte[], byte[]>>> consumerMap = consumerConnector.createMessageStreams(topicCountMap);
        List<KafkaStream<byte[], byte[]>> streams = consumerMap.get(topic);

        for (final KafkaStream stream : streams) {
            new Thread(new KafkaMessageProcessor(stream)).start();
        }
    }


    private class KafkaMessageProcessor implements Runnable {

        private KafkaStream kafkaStream;

        public KafkaMessageProcessor(KafkaStream kafkaStream) {
            this.kafkaStream = kafkaStream;
        }

        public void run() {
            ConsumerIterator<byte[], byte[]> it = kafkaStream.iterator();
            while (it.hasNext()) {
                String message = new String(it.next().message());
                LOGGER.info("【AccessLogKafkaSpout中的Kafka消费者接收到一条日志】message=" + message);
                try {
                    queue.put(message);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }

            }
        }
    }


    public void nextTuple() {
        if (queue.size() > 0) {
            try {
                String message = queue.take();
                LOGGER.info("【AccessLogKafkaSpout发射出去一条日志】message=" + message);
                collector.emit(new Values(message));
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        } else {
            Utils.sleep(100);
        }
    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("message"));
    }
}
