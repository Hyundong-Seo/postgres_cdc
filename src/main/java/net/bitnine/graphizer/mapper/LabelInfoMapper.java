package net.bitnine.graphizer.mapper;

import java.sql.ResultSet;
import java.sql.SQLException;

import org.springframework.jdbc.core.RowMapper;

import net.bitnine.graphizer.model.entity.LabelInfoEntity;

public class LabelInfoMapper implements RowMapper<LabelInfoEntity> {

    @Override
    public LabelInfoEntity mapRow(ResultSet rs, int rowNum) throws SQLException {
        LabelInfoEntity labelInfoEntity = new LabelInfoEntity();
        labelInfoEntity.setLabel_type(rs.getString("label_type"));
        labelInfoEntity.setSource_schema(rs.getString("source_schema"));
        labelInfoEntity.setSource_table_name(rs.getString("source_table_name"));
        labelInfoEntity.setSource_table_oid(rs.getInt("source_table_oid"));
        labelInfoEntity.setSource_columns(rs.getString("source_columns"));
        labelInfoEntity.setSource_pk_columns(rs.getString("source_pk_columns"));
        labelInfoEntity.setGraph_name(rs.getString("graph_name"));
        labelInfoEntity.setStart_label_name(rs.getString("start_label_name"));
        labelInfoEntity.setStart_label_oid(rs.getInt("start_label_oid"));
        labelInfoEntity.setStart_label_id(rs.getString("start_label_id"));
        labelInfoEntity.setEnd_label_name(rs.getString("end_label_name"));
        labelInfoEntity.setEnd_label_oid(rs.getInt("end_label_oid"));
        labelInfoEntity.setEnd_label_id(rs.getString("end_label_id"));
        labelInfoEntity.setTarget_label_name(rs.getString("target_label_name"));
        labelInfoEntity.setTarget_label_oid(rs.getInt("target_label_oid"));
        labelInfoEntity.setTarget_start_label_id(rs.getString("target_start_label_id"));
        labelInfoEntity.setTarget_end_label_id(rs.getString("target_end_label_id"));
        labelInfoEntity.setTarget_label_properties(rs.getString("target_label_properties"));
        return labelInfoEntity;
    }
}