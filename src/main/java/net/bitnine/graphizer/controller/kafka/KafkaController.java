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

    @Deprecated
    @KafkaListener(topics = topicName, groupId = groupId)
    public void listen(String message) {
        Gson gson = new Gson();
        JsonObject payloadObj = gson.fromJson(message, JsonObject.class).get("payload").getAsJsonObject();
        // String before = payloadObj.get("before").getAsJsonObject().toString();
        String schemaName = payloadObj.get("source").getAsJsonObject().get("schema").getAsString();
        String tableName = payloadObj.get("source").getAsJsonObject().get("table").getAsString();
        String updateType = payloadObj.get("op").getAsString();

        try (Connection connection = dataSource.getConnection()) {
            // vertex일 경우와 edge일 경우 다르게 동작
            try {
                if (updateType.equals("c")) {
                    JsonObject after = payloadObj.get("after").getAsJsonObject();
                    SyncEntity syncEntity = kafkaConsumerService.syncData(schemaName, tableName);
                    String[] pkCol = syncEntity.getRdb_pk_columns().split(",");
                    String conditions = "";
                    for(int i=0; i<pkCol.length; i++) {
                        conditions = conditions + " and " + pkCol[i] + " = " + after.getAsJsonObject().get(pkCol[i]);
                    }

                    kafkaConsumerService.insertVertexData(conditions, syncEntity);
                } else if (updateType.equals("u")) {
                    System.out.println("todo update");
                } else if (updateType.equals("d")) {
                    System.out.println("todo delete");
                }
            } catch (Exception e) {
                System.out.println(e.getMessage());
            }
        } catch (Exception e) {
            System.out.println(e.getMessage());
        }
    }
}