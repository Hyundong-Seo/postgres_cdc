package net.bitnine.graphizer.controller.kafka;

import java.sql.Connection;

import javax.sql.DataSource;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Controller;

import com.google.gson.Gson;
import com.google.gson.JsonObject;

@Controller
public class KafkaController {
    @Autowired
    DataSource dataSource;

    @Autowired
    JdbcTemplate jdbcTemplate;

    private static final String topicName = "fulfillment.public.sample";

    // listen(@Headers MessageHeaders headers, @Payload String payload)
    @KafkaListener(topics = topicName, groupId = "3")
    public void listen(String message) {

        Gson gson = new Gson();
        JsonObject payloadObj = gson.fromJson(message, JsonObject.class).get("payload").getAsJsonObject();
        String tableName = payloadObj.get("source").getAsJsonObject().get("table").getAsString();
        String updateType = payloadObj.get("op").getAsString();

        try (Connection connection = dataSource.getConnection()) {
            String sql = " WITH oidb AS ( "
                + "SELECT t.table_catalog AS dbname, t.table_name AS tbname, pgc.oid AS oid "
                + "FROM information_schema.tables t "
                + "INNER JOIN pg_catalog.pg_class pgc "
                + "ON t.table_name = pgc.relname "
                + "WHERE t.table_type = 'BASE TABLE' "
                + "AND t.table_name = '" + tableName + "' "
                + ") "
                + "SELECT (SELECT oidb.oid FROM oidb) AS oid_no, a.ctid as ctid_no, '" + updateType + "' as update_type "
                + "FROM " + tableName + " a "
                + "order by ctid_no desc "
                + "limit 1;";
            jdbcTemplate.execute("INSERT INTO tb_sync" + sql);
        } catch (Exception e) {
            System.out.println(e.getMessage());
        }

        /*
        KafkaConsumerConfig consumerConfig = new KafkaConsumerConfig();

        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(props);
        consumer.subscribe(Collections.singletonList(topicName));

        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(2000));

            for (ConsumerRecord<String, String> record : records){
                Gson gson = new Gson();
                JsonObject payloadObj = gson.fromJson(record.value(), JsonObject.class).get("payload").getAsJsonObject();
                String tableName = payloadObj.get("source").getAsJsonObject().get("table").getAsString();
                String updateType = payloadObj.get("op").getAsString();
                System.out.println("tableName : " + tableName);
                System.out.println("updateType : " + updateType);
            }
            consumer.commitSync();
        }
         */
    }
}