package net.bitnine.graphizer.mapper;

import java.sql.ResultSet;
import java.sql.SQLException;

import org.springframework.jdbc.core.RowMapper;

import net.bitnine.graphizer.model.entity.InsertVertexEntity;

public class InsertVertexMapper implements RowMapper<InsertVertexEntity> {

    @Override
    public InsertVertexEntity mapRow(ResultSet rs, int rowNum) throws SQLException {
        InsertVertexEntity insertVertexEntity = new InsertVertexEntity();
        insertVertexEntity.setGraph_name(rs.getString("graph_name"));
        insertVertexEntity.setTarget_label_name(rs.getString("target_label_name"));
        insertVertexEntity.setSource_pk_column(rs.getString("source_pk_column"));
        insertVertexEntity.setMeta_pk_column(rs.getString("meta_pk_column"));
        insertVertexEntity.setSource_column_name(rs.getString("source_column_name"));
        insertVertexEntity.setProperty_name(rs.getString("property_name"));
        return insertVertexEntity;
    }
}