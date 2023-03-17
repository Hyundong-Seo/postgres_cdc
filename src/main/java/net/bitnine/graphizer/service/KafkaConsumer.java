package net.bitnine.graphizer.service;

import java.io.IOException;
import java.sql.Connection;
import java.sql.Statement;

import javax.sql.DataSource;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

import com.google.gson.Gson;
import com.google.gson.JsonObject;

@Service
public class KafkaConsumer {
    @Autowired
    DataSource dataSource;

    @Autowired
    JdbcTemplate jdbcTemplate;
    
    @KafkaListener(topics = "fulfillment.public.sample", groupId = "3")
    public void consume(String message) throws IOException {
        System.out.println("===== consumer sendMessage =====");

        Gson gson = new Gson();
        JsonObject payloadObj = gson.fromJson(message, JsonObject.class).get("payload").getAsJsonObject();
        String tableName = payloadObj.get("source").getAsJsonObject().get("table").getAsString();
        String updateType = payloadObj.get("op").getAsString();
        
        try (Connection connection = dataSource.getConnection()) {
//            Statement statement = connection.createStatement();
            String sql = " WITH oidb AS ( "
                + "SELECT t.table_catalog AS dbname, t.table_name AS tbname, pgc.oid AS oid "
                + "FROM information_schema.tables t "
                + "INNER JOIN pg_catalog.pg_class pgc "
                + "ON t.table_name = pgc.relname "
                + "WHERE t.table_type = 'BASE TABLE' "
                + "AND t.table_name = '" + tableName + "' "
                + ") "
                + "SELECT (SELECT oidb.oid FROM oidb) AS oid_no, a.ctid as ctid_no, '" + updateType + "' as update_type "
                + "FROM " + tableName + " a;";
            jdbcTemplate.execute("INSERT INTO tb_sync" + sql);
        } catch (Exception e) {
            System.out.println(e.getMessage());
        }
    }
}