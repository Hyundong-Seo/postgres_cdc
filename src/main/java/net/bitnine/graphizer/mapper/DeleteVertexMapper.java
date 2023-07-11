package net.bitnine.graphizer.mapper;

import java.sql.ResultSet;
import java.sql.SQLException;

import org.springframework.jdbc.core.RowMapper;

import net.bitnine.graphizer.model.entity.DeleteVertexEntity;

public class DeleteVertexMapper implements RowMapper<DeleteVertexEntity> {

    @Override
    public DeleteVertexEntity mapRow(ResultSet rs, int rowNum) throws SQLException {
        DeleteVertexEntity deleteVertexEntity = new DeleteVertexEntity();
        deleteVertexEntity.setLabel_id(rs.getLong("label_id"));
        deleteVertexEntity.setLabel_type(rs.getString("label_type"));
        deleteVertexEntity.setGraph_name(rs.getString("graph_name"));
        deleteVertexEntity.setTarget_label_name(rs.getString("target_label_name"));
        deleteVertexEntity.setMeta_pk_column(rs.getString("meta_pk_column"));
        return deleteVertexEntity;
    }
}