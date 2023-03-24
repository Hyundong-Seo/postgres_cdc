package net.bitnine.graphizer.service;

import javax.sql.DataSource;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;

import net.bitnine.graphizer.model.entity.SyncEntity;

@Service
public class KafkaConsumerService {
    @Autowired
    DataSource dataSource;

    @Autowired
    JdbcTemplate jdbcTemplate;

    public void insertSyncData(String tableName, String updateType, String sql) {
        try {
            jdbcTemplate.execute("INSERT INTO tb_sync" + sql);
        } catch (Exception e) {
            System.out.println(e.getMessage());
        }
    }

    public SyncEntity syncData(String sql) {
            return jdbcTemplate.queryForObject(sql, new Object[] {}, (rs, rowNum) ->
                new SyncEntity(
                    rs.getInt("oid_no"),
                    rs.getString("ctid_no"),
                    rs.getString("update_type")
                )
            );
    }
}