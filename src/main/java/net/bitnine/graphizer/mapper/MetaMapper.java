package net.bitnine.graphizer.mapper;

import java.sql.ResultSet;
import java.sql.SQLException;

import org.springframework.jdbc.core.RowMapper;

import net.bitnine.graphizer.model.entity.MetaEntity;

public class MetaMapper implements RowMapper<MetaEntity> {

    @Override
    public MetaEntity mapRow(ResultSet rs, int rowNum) throws SQLException {
        MetaEntity metaEntity = new MetaEntity();
        metaEntity.setMeta_id(rs.getLong("meta_id"));
        metaEntity.setMeta_schema_name(rs.getString("meta_schema_name"));
        metaEntity.setMeta_table_name(rs.getString("meta_table_name"));
        metaEntity.setMeta_data(rs.getString("meta_data"));
        metaEntity.setMapped_data(rs.getString("mapped_data"));
        metaEntity.setProperty_data(rs.getString("property_data"));
        return metaEntity;
    }
}