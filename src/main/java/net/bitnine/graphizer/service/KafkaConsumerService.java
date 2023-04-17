package net.bitnine.graphizer.service;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Map;

import javax.sql.DataSource;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;

import com.google.gson.JsonObject;

import net.bitnine.graphizer.model.entity.LabelInfoEntity;

@Service
public class KafkaConsumerService {
    @Autowired
    DataSource dataSource;

    @Autowired
    JdbcTemplate jdbcTemplate;

    @Deprecated
    public LabelInfoEntity labelData(String schemaName, String tableName) {
        String sql = "select label_type, source_schema, source_table_name, source_table_oid, source_columns, source_pk_columns, graph_name, start_label_name, start_label_oid, start_label_id, end_label_name, end_label_oid, end_label_id, target_label_name, target_label_oid, target_start_label_id, target_end_label_id, target_label_properties "
        + "from tb_label_info "
        + "where source_schema = ? " 
        + "and source_table_name = ?";

        return jdbcTemplate.queryForObject(sql, new Object[] {schemaName, tableName}, (rs, rowNum) ->
            new LabelInfoEntity(
                rs.getString("label_type"),
                rs.getString("source_schema"),
                rs.getString("source_table_name"),
                rs.getInt("source_table_oid"),
                rs.getString("source_columns"),
                rs.getString("source_pk_columns"),
                rs.getString("graph_name"),
                rs.getString("start_label_name"),
                rs.getInt("start_label_oid"),
                rs.getString("start_label_id"),
                rs.getString("end_label_name"),
                rs.getInt("end_label_oid"),
                rs.getString("end_label_id"),
                rs.getString("target_label_name"),
                rs.getInt("target_label_oid"),
                rs.getString("target_start_label_id"),
                rs.getString("target_end_label_id"),
                rs.getString("target_label_properties")
            )
        );
    }

    @Deprecated
    public void insertVertexData(JsonObject after, LabelInfoEntity labelInfoEntity) {
        String[] pkCol = labelInfoEntity.getSource_pk_columns().split(",");
        String conditions = "";
        for(int i=0; i<pkCol.length; i++) {
            conditions = conditions + " and " + pkCol[i] + " = " + after.getAsJsonObject().get(pkCol[i]);
        }

        String[] labelProp = labelInfoEntity.getTarget_label_properties().split(",");
        String buildmap = "";
        for(int i=0; i<labelProp.length; i++) {
            if(i > 0) {
                buildmap = buildmap + ", '" + labelProp[i] + "', a." + labelProp[i];
            } else {
                buildmap = "'" + labelProp[i] + "', a." + labelProp[i];
            }
        }
        String graphName = labelInfoEntity.getGraph_name();
        String labelName = labelInfoEntity.getTarget_label_name();
        String colms = labelInfoEntity.getSource_columns();
        String rschem = labelInfoEntity.getSource_schema();
        String tbname = labelInfoEntity.getSource_table_name();

        String sql = 
            "INSERT INTO " + graphName + "." + labelName
            + " SELECT _graphid((_label_id('" + graphName + "'::name, '" + labelName + "'::name))::integer, nextval('" + graphName + "." + labelName + "_id_seq'::regclass)), "
            + "        agtype_build_map ("+ buildmap +") "
            + "FROM "
            + "( "
            + "        SELECT "+ colms
            + "        FROM  "+ rschem +"."+ tbname
            + "        WHERE 1 = 1 " + conditions
            + ") as a;";
        jdbcTemplate.execute(sql);
    }

    @Deprecated
    public void updateVertexData(JsonObject after, LabelInfoEntity labelInfoEntity, Map<String, String> map) {
        String[] pkCol = labelInfoEntity.getSource_pk_columns().split(",");
        String conditions = "";
        for(int i=0; i<pkCol.length; i++) {
            conditions = conditions + " and properties ->> '" + pkCol[i] + "' = '" + after.getAsJsonObject().get(pkCol[i]) + "'::text";
        }

        String[] labelProp = labelInfoEntity.getTarget_label_properties().split(",");
        String[] tbColumn = labelInfoEntity.getSource_columns().split(",");
        String setclaus = "";
        for(int i=0; i<labelProp.length; i++) {
            /*
            String afterCol = after.getAsJsonObject().get(tbColumn[i]).getAsString();
            if(map.get("\"" + tbColumn[i] + "\"") != null) {
                String val = map.get("\"" + tbColumn[i] + "\"");
                if(val.contains("Timestamp")) {
                    afterCol = getTimestampToDate(afterCol);
                }
            }
            */

            if(i > 0) {
                setclaus = setclaus + ", \"" + labelProp[i] + "\":" + after.getAsJsonObject().get(tbColumn[i]);
            } else {
                setclaus = "\"" + labelProp[i] + "\":" + after.getAsJsonObject().get(tbColumn[i]);
            }
        }
        String graphName = labelInfoEntity.getGraph_name();
        String labelName = labelInfoEntity.getTarget_label_name();

        String sql = 
            "update " + graphName + "." + labelName
            + " set properties = '{" + setclaus + "}'"
            + " where 1 = 1 " + conditions;
        jdbcTemplate.execute(sql);
    }

    @Deprecated
    public void deleteVertexData(JsonObject before, LabelInfoEntity labelInfoEntity) {
        String[] pkCol = labelInfoEntity.getSource_pk_columns().split(",");
        String conditions = "";
        for(int i=0; i<pkCol.length; i++) {
            conditions = conditions + " and properties ->> '" + pkCol[i] + "' = '" + before.getAsJsonObject().get(pkCol[i]) + "'::text";
        }
        
        String graphName = labelInfoEntity.getGraph_name();
        String labelName = labelInfoEntity.getTarget_label_name();

        String sql = 
            "delete from " + graphName + "." + labelName
            + " where 1 = 1 " + conditions;
        jdbcTemplate.execute(sql);
    }

    @Deprecated
    public void insertEdgeData(JsonObject after, LabelInfoEntity labelInfoEntity) {
        String startLb = after.getAsJsonObject().get("start_id").getAsString();
        String endLb = after.getAsJsonObject().get("end_id").getAsString();
        String conditions = " and start_id = " + startLb + " and end_id = " + endLb;
        
        String[] labelProp = labelInfoEntity.getTarget_label_properties().split(",");
        String buildmap = "";
        for(int i=0; i<labelProp.length; i++) {
            if(i > 0) {
                buildmap = buildmap + ", '" + labelProp[i] + "', a." + labelProp[i];
            } else {
                buildmap = "'" + labelProp[i] + "', data." + labelProp[i];
            }
        }
        String graphName = labelInfoEntity.getGraph_name();
        String targetLabelName = labelInfoEntity.getTarget_label_name();
        String startLabelName = labelInfoEntity.getStart_label_name();
        String startLabelId = labelInfoEntity.getStart_label_id();
        String endLabelName = labelInfoEntity.getEnd_label_name();
        String endLabelId = labelInfoEntity.getEnd_label_id();
        String targetStartLabelId = labelInfoEntity.getTarget_start_label_id();
        String targetEndLabelId = labelInfoEntity.getTarget_end_label_id();
        String colms = (labelInfoEntity.getSource_columns() != null ? ", " + labelInfoEntity.getSource_columns() : "");
        String rschem = labelInfoEntity.getSource_schema();
        String tbname = labelInfoEntity.getSource_table_name();

        String sql = 
            "INSERT INTO " + graphName + "." + targetLabelName
            + " select "
            + "  _graphid((_label_id('" + graphName + "'::name, '" + targetLabelName + "'::name))::integer, nextval('" + graphName + "." + targetLabelName + "_id_seq'::regclass)), "
            + "  data.startid::text::graphid, "
            + "  data.endid::text::graphid, "
            + "  agtype_build_map("+ buildmap +") "
            + "from "
            + "( "
            + "  SELECT startid, endid " + colms
            + "  FROM  "+ rschem +"."+ tbname + " tb "
            + "  JOIN cypher('" + graphName + "', $$ "
            + "    MATCH(v:" + startLabelName + ") RETURN id(v), v." + startLabelId
            + "  $$) as a (startid agtype, " + startLabelId +" agtype) "
            + "  ON tb." + targetStartLabelId + " = a." + startLabelId
            + "  JOIN cypher('" + graphName + "', $$ "
            + "    MATCH(v:" + endLabelName + ") RETURN id(v), v." + endLabelId
            + "  $$) as b (endid agtype, " + endLabelId +" agtype) "
            + "  ON tb." + targetEndLabelId + " = b." + endLabelId
            + "  WHERE 1 = 1 " + conditions
            + ") as data;";
        jdbcTemplate.execute(sql);
    }

    private String getTimestampToDate(String timestampStr) {
        long timestamp = Long.parseLong(timestampStr);
        Date date = new java.util.Date(timestamp*1000L);
        SimpleDateFormat sdf = new java.text.SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        sdf.setTimeZone(java.util.TimeZone.getTimeZone("GMT+9"));
        return sdf.format(date);
    }
}