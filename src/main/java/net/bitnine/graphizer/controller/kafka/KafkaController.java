package net.bitnine.graphizer.controller.kafka;

import java.sql.Connection;

import javax.sql.DataSource;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Controller;

import com.google.gson.Gson;
import com.google.gson.JsonObject;

import net.bitnine.graphizer.model.entity.SyncEntity;
import net.bitnine.graphizer.service.KafkaConsumerService;

@Controller
public class KafkaController {
    @Autowired
    DataSource dataSource;

    @Autowired
    JdbcTemplate jdbcTemplate;

    @Autowired
    KafkaConsumerService kafkaConsumerService;

    private static final String topicName = "postgres.ag_catalog.tb_sample1";
    private static final String groupId = "age";

    @KafkaListener(topics = topicName, groupId = groupId)
    public void listen(String message) {
        Gson gson = new Gson();
        JsonObject payloadObj = gson.fromJson(message, JsonObject.class).get("payload").getAsJsonObject();
        String before = payloadObj.get("before").getAsJsonObject().toString();
        String after = payloadObj.get("after").getAsJsonObject().toString();
        String tableName = payloadObj.get("source").getAsJsonObject().get("table").getAsString();
        String updateType = payloadObj.get("op").getAsString();
        System.out.println("message: " + message);
        System.out.println("before: " + before);
        System.out.println("after: " + after);

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

        try (Connection connection = dataSource.getConnection()) {
            kafkaConsumerService.insertSyncData(tableName, updateType, sql);

            try {
                SyncEntity entity = kafkaConsumerService.syncData(sql);
            } catch (Exception e) {
                System.out.println(e.getMessage());
            }
        } catch (Exception e) {
            System.out.println(e.getMessage());
        }
    }
}