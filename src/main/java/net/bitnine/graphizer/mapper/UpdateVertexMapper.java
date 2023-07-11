package net.bitnine.graphizer.mapper;

import java.sql.ResultSet;
import java.sql.SQLException;

import org.springframework.jdbc.core.RowMapper;

import net.bitnine.graphizer.model.entity.UpdateVertexEntity;

public class UpdateVertexMapper implements RowMapper<UpdateVertexEntity> {

    @Override
    public UpdateVertexEntity mapRow(ResultSet rs, int rowNum) throws SQLException {
        UpdateVertexEntity updateVertexEntity = new UpdateVertexEntity();
        updateVertexEntity.setMeta_schema_name(rs.getString("meta_schema_name"));
        updateVertexEntity.setMeta_table_name(rs.getString("meta_table_name"));
        updateVertexEntity.setGraph_name(rs.getString("graph_name"));
        updateVertexEntity.setTarget_label_name(rs.getString("target_label_name"));
        updateVertexEntity.setKey_property(rs.getString("key_property"));
        return updateVertexEntity;
    }
}