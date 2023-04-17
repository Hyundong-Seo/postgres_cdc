package net.bitnine.graphizer.controller.kafka;

import java.sql.Connection;
import java.util.HashMap;
import java.util.Map;

import javax.sql.DataSource;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Controller;

import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;

import net.bitnine.graphizer.model.entity.LabelInfoEntity;
import net.bitnine.graphizer.service.KafkaConsumerService;

@Controller
public class KafkaController {
    @Autowired
    DataSource dataSource;

    @Autowired
    JdbcTemplate jdbcTemplate;

    @Autowired
    KafkaConsumerService kafkaConsumerService;

    private static final String topicPattern = "postgres.ag_catalog.*";
    private static final String groupId = "age";

    @Deprecated
    @KafkaListener(topicPattern = topicPattern, groupId = groupId)
    public void listen(String message) {
        Gson gson = new Gson();
        JsonObject payloadObj = gson.fromJson(message, JsonObject.class).get("payload").getAsJsonObject();
        String schemaName = payloadObj.get("source").getAsJsonObject().get("schema").getAsString();
        String tableName = payloadObj.get("source").getAsJsonObject().get("table").getAsString();
        String updateType = payloadObj.get("op").getAsString();
        LabelInfoEntity labelInfoEntity = kafkaConsumerService.labelData(schemaName, tableName);

        try (Connection connection = dataSource.getConnection()) {
            System.out.println("===== payload =====");
            System.out.println(gson.fromJson(message, JsonObject.class).get("payload").toString());
            if(labelInfoEntity.getLabel_type().equals("v")) {
                if (updateType.equals("c")) {
                    JsonObject after = payloadObj.get("after").getAsJsonObject();
                    kafkaConsumerService.insertVertexData(after, labelInfoEntity);
                } else if (updateType.equals("u")) {
                    JsonArray schemaObj = gson.fromJson(message, JsonObject.class).get("schema").getAsJsonObject().get("fields").getAsJsonArray().get(0).getAsJsonObject().get("fields").getAsJsonArray();
                    Map<String, String> map = new HashMap<String, String>();
                    for(int i=0; i<schemaObj.size(); i++) {
                        if(schemaObj.get(i).getAsJsonObject().get("name") != null) {
                            map.put(schemaObj.get(i).getAsJsonObject().get("field").toString(), schemaObj.get(i).getAsJsonObject().get("name").toString());
                        }
                    }
                    JsonObject after = payloadObj.get("after").getAsJsonObject();
                    kafkaConsumerService.updateVertexData(after, labelInfoEntity, map);
                } else if (updateType.equals("d")) {
                    JsonObject before = payloadObj.get("before").getAsJsonObject();
                    kafkaConsumerService.deleteVertexData(before, labelInfoEntity);
                }
            } else if(labelInfoEntity.getLabel_type().equals("e")) {
                if (updateType.equals("c")) {
                    JsonObject after = payloadObj.get("after").getAsJsonObject();
                    kafkaConsumerService.insertEdgeData(after, labelInfoEntity);
                }
            }
        } catch (Exception e) {
            System.out.println(e.getMessage());
        }
    }
}