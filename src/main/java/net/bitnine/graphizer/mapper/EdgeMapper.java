package net.bitnine.graphizer.mapper;

import java.sql.ResultSet;
import java.sql.SQLException;

import org.springframework.jdbc.core.RowMapper;

import net.bitnine.graphizer.model.entity.EdgeEntity;

public class EdgeMapper implements RowMapper<EdgeEntity> {

    @Override
    public EdgeEntity mapRow(ResultSet rs, int rowNum) throws SQLException {
        EdgeEntity edgeEntity = new EdgeEntity();
        edgeEntity.setGraph_name(rs.getString("graph_name"));
        edgeEntity.setStart_label_name(rs.getString("start_label_name"));
        edgeEntity.setStart_label_id_name(rs.getString("start_label_id_name"));
        edgeEntity.setEnd_label_name(rs.getString("end_label_name"));
        edgeEntity.setEnd_label_id_name(rs.getString("end_label_id_name"));
        edgeEntity.setTarget_label_name(rs.getString("target_label_name"));
        edgeEntity.setTarget_start_label_id_name(rs.getString("target_start_label_id_name"));
        edgeEntity.setTarget_end_label_id_name(rs.getString("target_end_label_id_name"));
        edgeEntity.setProperty_name(rs.getString("property_name"));
        edgeEntity.setMeta_schema_name(rs.getString("meta_schema_name"));
        edgeEntity.setMeta_table_name(rs.getString("meta_table_name"));
        edgeEntity.setSource_schema_name(rs.getString("source_schema_name"));
        edgeEntity.setSource_table_name(rs.getString("source_table_name"));
        edgeEntity.setColumn_name(rs.getString("column_name"));
        return edgeEntity;
    }
}