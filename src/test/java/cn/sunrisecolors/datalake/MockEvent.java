//package com.dobest.datalake;
//
//import com.dobest.datalake.common.Event;
//import com.fasterxml.jackson.core.JsonProcessingException;
//import com.fasterxml.jackson.databind.ObjectMapper;
//import org.apache.kafka.clients.producer.KafkaProducer;
//import org.apache.kafka.clients.producer.ProducerConfig;
//import org.apache.kafka.clients.producer.ProducerRecord;
//import org.apache.kudu.ColumnSchema;
//import org.apache.kudu.Type;
//import org.junit.Test;
//
//import java.util.*;
//import java.util.stream.Collectors;
//
///**
// * @author :hujiansong
// * @date :2019/8/7 10:01
// * @since :1.8
// */
//public class MockEvent {
//
//
//    private KafkaProducer<String, String> kafkaProducer;
//
//    {
//        Properties props = new Properties();
//        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
//        props.put(ProducerConfig.RETRIES_CONFIG, "3");
//        props.put(ProducerConfig.ACKS_CONFIG, "all");
//        props.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "gzip");
//        props.put(ProducerConfig.BATCH_SIZE_CONFIG, 6000);
//        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
//        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
//        kafkaProducer = new KafkaProducer<>(props);
//    }
//
//
//    private ObjectMapper mapper = new ObjectMapper();
//
//    private String[] eventName = {"startApp", "signUP", "createPart", "loginGame"};
//
//    private String[] appIds = {"yk01", "yk02", "yk03"};
//
//    private long[] timeCache = {1566403200262L, 1566316800262L,
//            1566230400262L, 1566144000262L,
//            1566057600262L, 1565971200262L,
//            1566057600262L, 1565971200262L,
//            1566037600262L, 1565951200262L,
//            1566027600262L, 1565911200262L,
//            1565884800262L};
//
//    private List<Map<String, Object>> properties() {
//        List<Map<String, Object>> properties = new ArrayList<>();
//        // properties1
//        Map<String, Object> map1 = new HashMap<>(16);
//        map1.put("$appVersion", "2.11.11");
//        map1.put("$country", "韩国");
//        map1.put("item_id", "123" + r.nextInt(10));
//        map1.put("$c1", 1980);
//        map1.put("c2", 1980);
//        map1.put("$ip", "192.168.1.1");
//        map1.put("$device", "device-" + new Random().nextInt(10));
//        map1.put("user-agent", "Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/76.0.3809.100 Safari/537.36");
//
//
//        // properties1
//        Map<String, Object> map2 = new HashMap<>(16);
//        map2.put("$city", "上海");
//        map2.put("$ip", "192.168.1.1");
//        map2.put("$c1", 1980);
//        map2.put("c2", 1980);
//        map2.put("age", 12);
//        map2.put("$screenheight88", 1980);
//        map2.put("$device", "device-" + new Random().nextInt(10));
//        map2.put("$screenheight99", 1980);
//        map2.put("user-agent", "Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/76.0.3809.100 Safari/537.36");
//
//
//        // properties1
//        Map<String, Object> map3 = new HashMap<>(16);
//        map3.put("recharge", 12.112);
//        map3.put("$screenwidth", 920);
//        map3.put("$screenheight", 1980);
//        map3.put("$screenheight88", 1980);
//        map3.put("$screenheight99", 1980);
//        map3.put("$c1", 1980);
//        map3.put("c2", 1980);
//        map3.put("$device", "device-" + new Random().nextInt(10));
//        map3.put("$ip", "192.168.1.1");
//
//        properties.add(map1);
//        properties.add(map2);
//        properties.add(map3);
//        return properties;
//    }
//
//
//    private static final Random r = new Random();
//
//
//    @Test
//    public void mockItemEvent() throws JsonProcessingException {
//        int num = 1000;
//        for (int i = 0; i < num; i++) {
//            Event e = initItemEvent();
//            System.out.println("item: " + mapper.writeValueAsString(e));
//            kafkaProducer.send(new ProducerRecord<>("event-topic", UUID.randomUUID().toString(), mapper.writeValueAsString(e)));
//        }
//    }
//
//    private Event initItemEvent() {
//        Event event = new Event();
//        event.setApp_id(appIds[1]);
//        event.setType("item_set");
//        event.setEvent(eventName[r.nextInt(eventName.length)]);
//        event.setUuid(System.currentTimeMillis() + r.nextInt(20000));
//        event.setTime(System.currentTimeMillis() + r.nextInt(20000));
//        event.setTrusted_time(false);
//        event.setProperties(properties().get(r.nextInt(3)));
//        return event;
//    }
//
//
//    @Test
//    public void mockEvent2() throws JsonProcessingException {
//        int num = 1000;
//        for (int i = 0; i < num; i++) {
//            Event e = initEvent2();
//            kafkaProducer.send(new ProducerRecord<>("event-topic", UUID.randomUUID().toString(), mapper.writeValueAsString(e)));
//        }
//    }
//
//    private Event initEvent2() {
//        Event event = new Event();
//        event.setApp_id(appIds[1]);
//        event.setEvent(eventName[r.nextInt(eventName.length)]);
//        event.setUuid(System.currentTimeMillis() + r.nextInt(20000));
//        event.setTime(System.currentTimeMillis() + r.nextInt(20000));
//        event.setTrusted_time(false);
//        event.setProperties(properties().get(r.nextInt(3)));
//        return event;
//    }
//
//    @Test
//    public void mockEvent() throws JsonProcessingException {
//        // send num
//        int num = 1000;
//        List<Event> events = new ArrayList<>();
//        for (int i = 0; i < num; i++) {
//            Event e = initEvent();
//            events.add(e);
//            System.out.println(mapper.writeValueAsString(e));
//            kafkaProducer.send(new ProducerRecord<>("event-topic", UUID.randomUUID().toString(), mapper.writeValueAsString(e)));
//        }
//
//        Map<String, List<ColumnSchema>> kuduSchemaMap = new HashMap<>(16);
//
//        for (Event e : events) {
//            String appId = e.getApp_id();
//            List<ColumnSchema> columns;
//            if (!kuduSchemaMap.keySet().contains(appId)) {
//                columns = new ArrayList<>();
//                columns.add(new ColumnSchema.ColumnSchemaBuilder("uuid", Type.STRING)
//                        .key(true)
//                        .build());
//                columns.add(new ColumnSchema.ColumnSchemaBuilder("time", Type.INT64)
//                        .build());
//                columns.add(new ColumnSchema.ColumnSchemaBuilder("event", Type.STRING)
//                        .build());
//                processKuduSchema(columns, e.getProperties());
//
//            } else {
//                columns = kuduSchemaMap.get(appId);
//                processKuduSchema(columns, e.getProperties());
//            }
//            kuduSchemaMap.put(appId, columns);
//        }
//
//        System.out.println(kuduSchemaMap);
//    }
//
//
//    private void processKuduSchema(List<ColumnSchema> columns, Map<String, Object> properties) {
//        // 当存在相同的key的时候，丢弃掉后面的key
//        Set<String> colSets = columns.stream().map(ColumnSchema::getName).collect(Collectors.toSet());
//        properties.forEach((k, v) -> {
//            if (!colSets.contains(k)) {
//                columns.add(new ColumnSchema.ColumnSchemaBuilder(k, asKuduType(v))
//                        .build());
//            }
//        });
//    }
//
//
//    private Event initEvent() {
//        Event event = new Event();
//        event.setApp_id(appIds[r.nextInt(appIds.length)]);
//        event.setEvent(eventName[r.nextInt(eventName.length)]);
//        event.setUuid(System.currentTimeMillis() + r.nextInt(20000));
//        event.setTime(timeCache[r.nextInt(timeCache.length)]);
//        event.setTrusted_time(false);
//        event.setType("track");
//        event.setProperties(properties().get(r.nextInt(3)));
//        return event;
//    }
//
//    private Type asKuduType(Object v) {
//        Type kuduType;
//        if (v instanceof String) {
//            kuduType = Type.STRING;
//        } else if (v instanceof Integer) {
//            kuduType = Type.INT32;
//        } else if (v instanceof Long) {
//            kuduType = Type.INT64;
//        } else if (v instanceof Double) {
//            kuduType = Type.DOUBLE;
//        } else if (v instanceof Float) {
//            kuduType = Type.FLOAT;
//        } else if (v instanceof Boolean) {
//            kuduType = Type.BOOL;
//        } else {
//            throw new IllegalArgumentException("Type cannot convert!");
//        }
//        return kuduType;
//    }
//
//}
