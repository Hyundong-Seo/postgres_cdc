package net.bitnine.graphizer.controller.kafka;

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
    // private static final String topicPattern = "postgres.*";
    private static final String groupId = "grp_age";

    @Deprecated
    @KafkaListener(topicPattern = topicPattern, groupId = groupId)
    public void listen(String message) {
        Gson gson = new Gson();
        JsonObject payloadObj = gson.fromJson(message, JsonObject.class).get("payload").getAsJsonObject();
        String schemaName = payloadObj.get("source").getAsJsonObject().get("schema").getAsString();
        String tableName = payloadObj.get("source").getAsJsonObject().get("table").getAsString();
        String updateType = payloadObj.get("op").getAsString();

        // updateType을 기준으로 c / u / d로 나누어서 진행
        // V, E인지는 service에서 진행
        // 그렇게되면 labelData는 필요없고 서비스에서 한번에 진행
        if (updateType.equals("c")
                && (tableName != "tb_source_info" || tableName != "tb_column_info" || tableName != "tb_property_info" || tableName != "tb_meta_info" || tableName != "tb_label_info")) {
            JsonObject after = payloadObj.get("after").getAsJsonObject();
            kafkaConsumerService.insertData(after, schemaName, tableName);
        } else if (updateType.equals("u")) {
            JsonArray schemaObj = gson.fromJson(message, JsonObject.class).get("schema").getAsJsonObject().get("fields").getAsJsonArray().get(0).getAsJsonObject().get("fields").getAsJsonArray();
            Map<String, String> map = new HashMap<String, String>();
            for(int i=0; i<schemaObj.size(); i++) {
                if(schemaObj.get(i).getAsJsonObject().get("name") != null) {
                    map.put(schemaObj.get(i).getAsJsonObject().get("field").toString(), schemaObj.get(i).getAsJsonObject().get("name").toString());
                }
            }
            JsonObject after = payloadObj.get("after").getAsJsonObject();
            kafkaConsumerService.updateData(after, map, schemaName, tableName);
        } else if (updateType.equals("d")) {
            JsonObject before = payloadObj.get("before").getAsJsonObject();
            kafkaConsumerService.deleteData(before, schemaName, tableName);
        }
    }
}