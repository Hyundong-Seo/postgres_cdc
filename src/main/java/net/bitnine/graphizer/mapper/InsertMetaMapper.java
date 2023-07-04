package net.bitnine.graphizer.mapper;

import java.sql.ResultSet;
import java.sql.SQLException;

import org.springframework.jdbc.core.RowMapper;

import net.bitnine.graphizer.model.entity.InsertMetaEntity;

public class InsertMetaMapper implements RowMapper<InsertMetaEntity> {

    @Override
    public InsertMetaEntity mapRow(ResultSet rs, int rowNum) throws SQLException {
        InsertMetaEntity insertMetaEntity = new InsertMetaEntity();
        insertMetaEntity.setMeta_id(rs.getLong("meta_id"));
        insertMetaEntity.setMeta_schema_name(rs.getString("meta_schema_name"));
        insertMetaEntity.setMeta_table_name(rs.getString("meta_table_name"));
        insertMetaEntity.setMeta_data(rs.getString("meta_data"));
        insertMetaEntity.setMapped_data(rs.getString("mapped_data"));
        insertMetaEntity.setProperty_data(rs.getString("property_data"));
        return insertMetaEntity;
    }
}