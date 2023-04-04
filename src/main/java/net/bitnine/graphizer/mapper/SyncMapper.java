package net.bitnine.graphizer.mapper;

import java.sql.ResultSet;
import java.sql.SQLException;

import org.springframework.jdbc.core.RowMapper;

import net.bitnine.graphizer.model.entity.SyncEntity;

public class SyncMapper implements RowMapper<SyncEntity> {

    @Override
    public SyncEntity mapRow(ResultSet rs, int rowNum) throws SQLException {
        SyncEntity syncEntity = new SyncEntity();
        syncEntity.setRdb_schema(rs.getString("rdb_schema"));
        syncEntity.setRdb_table_name(rs.getString("rdb_table_name"));
        syncEntity.setRdb_table_oid(rs.getInt("rdb_table_oid"));
        syncEntity.setRdb_columns(rs.getString("rdb_columns"));
        syncEntity.setRdb_pk_columns(rs.getString("rdb_pk_columns"));
        syncEntity.setGraph_name(rs.getString("graph_name"));
        syncEntity.setLabel_name(rs.getString("label_name"));
        syncEntity.setLabel_oid(rs.getInt("label_oid"));
        syncEntity.setLabel_properties(rs.getString("label_properties"));
        return syncEntity;
    }
}