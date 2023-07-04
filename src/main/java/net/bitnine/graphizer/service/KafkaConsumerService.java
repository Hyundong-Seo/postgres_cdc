package net.bitnine.graphizer.service;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.Map;

import javax.sql.DataSource;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;

import com.google.gson.JsonObject;

import net.bitnine.graphizer.model.entity.InsertVertexEntity;
import net.bitnine.graphizer.model.entity.MetaEntity;

@Service
public class KafkaConsumerService {
    @Autowired
    DataSource dataSource;

    @Autowired
    JdbcTemplate jdbcTemplate;
    // Todo
    // auto commit false로 하고 아래 쿼리들 진행
    // 1. spring.datasource.hikari.auto-commit: false
    // 2. getJdbcTemplate().getDataSource().getConnection().setAutoCommit(false);

    /*
    @Deprecated
    public List<ColumnInfoEntity> labelData(String schemaName, String tableName) {
        String sql = "SELECT li.label_type, li.label_id, mi.meta_id, si.source_id, ci.column_id "
            + "FROM tb_label_info li "
            + "JOIN tb_meta_info mi ON li.meta_id = mi.meta_id "
            + "JOIN tb_source_info si ON mi.meta_id = si.meta_id "
            + "JOIN tb_column_info ci ON si.column_id = ci.column_id "
            + "WHERE ci.schema_name = ? "
            + "AND ci.table_name = ? "
            + "AND si.meta_key_yn = 'Y' "
            + "ORDER BY li.label_id, mi.meta_id, si.source_id, ci.column_id";

        // List<Map<String, Object>> list = jdbcTemplate.query(sql, new Object[] {schemaName, tableName}, String.class, String.class);
        // List<ColumnInfoEntity> clist = new ArrayList<>();
        // list.forEach(m -> {
        //     ColumnInfoEntity columnInfoEntity = new ColumnInfoEntity(
        //         (String)m.get("label_type"),
        //         (long)m.get("label_id"),
        //         (long)m.get("meta_id"),
        //         (long)m.get("source_id"),
        //         (long)m.get("column_id"));
        //         clist.add(columnInfoEntity);
        // });
        // return clist;

        return jdbcTemplate.query(sql, new Object[] {schemaName, tableName}, (rs, rowNum) ->
            new ColumnInfoEntity(
                rs.getString("label_type"),
                rs.getLong("label_id"),
                rs.getLong("meta_id"),
                rs.getLong("source_id"),
                rs.getLong("column_id")
            )
        );
    } */

    @Deprecated
    public void insertData(JsonObject after, String schemaName, String tableName) {
        List<MetaEntity> metaEntityList = selectMetaQuery(schemaName, tableName);
        if(metaEntityList.size() > 0) {
            for(int i=0; i<metaEntityList.size(); i++) {
                String[] metaData = metaEntityList.get(i).getMeta_data().split(",");
                String pkColumn = metaData[2];
                String[] mappedData = metaEntityList.get(i).getMapped_data().split("!");
                String[] mappedDataDetail = {};
                String[] propertyData = metaEntityList.get(i).getProperty_data().split(",");

                String metaColumns = "";
                for(int j=1; j<propertyData.length; j++) {
                    if("".equals(metaColumns)) {
                        metaColumns = propertyData[j];
                    } else {
                        metaColumns = metaColumns + "," + propertyData[j];
                    }
                }
                String columns = "";
                String JOINQuery = "";
                for(int j=3; j<metaData.length; j++) {
                    if("".equals(columns)) {
                        columns = "a." + metaData[j];
                    } else {
                        columns = columns + ", a." + metaData[j];
                    }
                }
                for(int j=0; j<mappedData.length; j++) {
                    mappedDataDetail = mappedData[j].split(",");
                    String alias = " b" + j;
                    JOINQuery = JOINQuery +  " LEFT JOIN " + mappedDataDetail[0] + "." + mappedDataDetail[1] + alias
                        + " ON a." + pkColumn + " =" + alias + "." + mappedDataDetail[2];
                    for(int k=3; k<mappedDataDetail.length; k++) {
                        if("".equals(columns)) {
                            columns = alias + "." + mappedDataDetail[k];
                        } else {
                            columns = columns + ", " + alias + "." + mappedDataDetail[k];
                        }
                    }
                }
                String metaSchema = metaEntityList.get(i).getMeta_schema_name();
                String metaTable = metaEntityList.get(i).getMeta_table_name();
                String insertForMetaSql = "INSERT INTO " + metaSchema + "." + metaTable
                    + " (" + metaColumns + ")"
                    + " SELECT " + columns
                    + " FROM " + metaData[0] + "." + metaData[1] + " a"
                    + JOINQuery
                    + " WHERE a." + metaData[2] + " = " + after.getAsJsonObject().get(metaData[2]).getAsString() + "::text";
                try {
                    jdbcTemplate.execute(insertForMetaSql);
                } catch(Exception e) {
                    System.out.println(e.getMessage());
                }

                InsertVertexEntity insertVertexEntity = insertVertexQuery(metaEntityList.get(i).getMeta_id());
                if(insertVertexEntity != null) {
                    String graphName = insertVertexEntity.getGraph_name();
                    String labelName = insertVertexEntity.getTarget_label_name();
                    String[] colms = insertVertexEntity.getSource_column_name().split(",");
                    String[] pops = insertVertexEntity.getProperty_name().split(",");

                    String conditions = " AND " + insertVertexEntity.getMeta_pk_column() + " = " + after.getAsJsonObject().get(pkColumn).getAsString() + "::text";

                    String buildmap = "";
                    for(int j=0; j<pops.length; j++) {
                        if(j > 0) {
                            buildmap = buildmap + ", '" + pops[j] + "', a." + colms[j];
                        } else {
                            buildmap = "'" + pops[j] + "', a." + pops[j];
                        }
                    }

                    String insertSql = 
                        "INSERT INTO " + graphName + "." + labelName
                        + " SELECT _graphid((_label_id('" + graphName + "'::name, '" + labelName + "'::name))::integer, nextval('" + graphName + "." + labelName + "_id_seq'::regclass)), "
                        + "     agtype_build_map ("+ buildmap +") "
                        + "FROM "
                        + "( "
                        + "     SELECT " + insertVertexEntity.getProperty_name().toString()
                        + "     FROM  " + metaSchema + "." + metaTable
                        + "     WHERE 1 = 1 " + conditions
                        + ") AS a;";
                    jdbcTemplate.execute(insertSql);
                }
            }
        }
    }

    @Deprecated
    private List<MetaEntity> selectMetaQuery(String schemaName, String tableName) {
        try {
            String sql = " WITH sc AS ("
                + "     SELECT si.meta_id"
                + "     FROM tb_column_info ci"
                + "     JOIN tb_source_info si ON ci.column_id = si.column_id"
                + "     WHERE ci.schema_name = ?"
                + "     AND ci.table_name = ?"
                + "     GROUP BY si.meta_id"
                + " ), tb AS ("
                + " 	SELECT sc.meta_id, ci.schema_name AS meta_key_schema_name, ci.table_name AS meta_key_table_name"
                + " 	FROM tb_meta_info mi"
                + " 	JOIN tb_source_info si ON si.meta_id = mi.meta_id"
                + " 	JOIN tb_column_info ci ON ci.column_id = si.column_id"
                + " 	JOIN sc ON sc.meta_id = mi.meta_id AND sc.meta_id = si.meta_id"
                + " 	WHERE si.meta_key_yn = 'Y'"
                + " 	AND mi.use_yn = 'Y'"
                + " 	GROUP BY sc.meta_id, meta_key_schema_name, meta_key_table_name"
                + " ), mt AS ("
                + " 	SELECT a.meta_id, a.meta_schema_name, a.meta_table_name, a.meta_key_schema_name, a.meta_key_table_name, a.meta_key_schema_name || ',' || a.meta_key_table_name || ',' || a.meta_pk_column_name || ',' || string_agg(a.column_name, ',') AS meta_data, (select pi.property_name from tb_property_info pi join tb_source_info si on pi.source_id = si.source_id join tb_column_info ci on ci.column_id = si.column_id where si.meta_id = a.meta_id and si.meta_key_yn = 'Y') || ',' || string_agg(a.property_name, ',') AS property_data"
                + " 	FROM ("
                + " 	    SELECT mi.meta_id, mi.meta_schema_name, mi.meta_table_name, ci.schema_name AS meta_key_schema_name, ci.table_name AS meta_key_table_name, (SELECT column_name FROM tb_column_info WHERE column_id = si.source_pk_column_id) AS meta_pk_column_name, ci.column_name, pi.property_name"
                + " 	    FROM tb_meta_info mi"
                + " 	    JOIN tb_source_info si ON si.meta_id = mi.meta_id"
                + " 	    JOIN tb_column_info ci ON ci.column_id = si.column_id"
                + " 	    JOIN tb_property_info pi ON pi.source_id = si.source_id"
                + " 	    JOIN sc ON sc.meta_id = mi.meta_id"
                + " 	    JOIN tb ON tb.meta_key_schema_name = ci.schema_name AND tb.meta_key_table_name = ci.table_name"
                + " 	    WHERE mi.use_yn = 'Y'"
                + " 	    GROUP BY mi.meta_id, mi.meta_schema_name, mi.meta_table_name, ci.schema_name, ci.table_name, meta_pk_column_name, ci.column_name, pi.property_name"
                + " 	) a"
                + " 	GROUP BY a.meta_id, a.meta_schema_name, a.meta_table_name, a.meta_key_schema_name, a.meta_key_table_name, a.meta_pk_column_name"
                + " ), mp AS ("
                + " 	SELECT b.meta_id, b.meta_schema_name, b.meta_table_name, string_agg(b.mapped_data, '!') AS mapped_data, CASE WHEN mt.property_data != '' THEN mt.property_data || ',' || string_agg(b.property_name, ',') ELSE string_agg(b.property_name, ',') END AS property_data"
                + " 	FROM ("
                + " 		SELECT a.meta_id, a.meta_schema_name, a.meta_table_name, a.mapped_key_schema_name || ',' || a.mapped_key_table_name || ',' || a.mapped_pk_column_name || ',' || string_agg(a.column_name, ',') AS mapped_data, a.property_name"
                + " 		FROM ("
                + " 		    SELECT mi.meta_id, mi.meta_schema_name, mi.meta_table_name, ci.schema_name AS mapped_key_schema_name, ci.table_name AS mapped_key_table_name, (SELECT column_name FROM tb_column_info WHERE column_id = si.source_pk_column_id) AS mapped_pk_column_name, ci.column_name, pi.property_name"
                + " 		    FROM tb_meta_info mi"
                + " 		    JOIN tb_source_info si ON si.meta_id = mi.meta_id"
                + " 		    JOIN tb_column_info ci ON ci.column_id = si.column_id"
                + " 		    JOIN tb_property_info pi ON pi.source_id = si.source_id"
                + " 		    JOIN sc ON sc.meta_id = mi.meta_id"
                + " 		    JOIN tb ON (tb.meta_key_schema_name <> ci.schema_name or tb.meta_key_table_name <> ci.table_name) AND tb.meta_id = sc.meta_id"
                + " 		    WHERE mi.use_yn = 'Y'"
                + " 		    GROUP BY mi.meta_id, mi.meta_schema_name, mi.meta_table_name, ci.schema_name, ci.table_name, mapped_pk_column_name, ci.column_name, pi.property_name"
                + " 		) a"
                + " 		GROUP BY a.meta_id, a.meta_schema_name, a.meta_table_name, a.mapped_key_schema_name, a.mapped_key_table_name, a.mapped_pk_column_name, a.property_name"
                + " 	) b"
                + " 	JOIN mt ON mt.meta_id = b.meta_id"
                + " 	GROUP BY b.meta_id, b.meta_schema_name, b.meta_table_name, mt.property_data"
                + " )"
                + " SELECT mt.meta_id, mt.meta_schema_name, mt.meta_table_name, mt.meta_data, mp.mapped_data, mp.property_data"
                + " FROM mt"
                + " JOIN mp ON mt.meta_id = mp.meta_id";
            
            return jdbcTemplate.query(sql, new Object[] {schemaName, tableName}, (rs, rowNum) ->
                new MetaEntity(
                    rs.getLong("meta_id"),
                    rs.getString("meta_schema_name"),
                    rs.getString("meta_table_name"),
                    rs.getString("meta_data"),
                    rs.getString("mapped_data"),
                    rs.getString("property_data")
                )
            );
        } catch(Exception e) {
            System.out.println(e.getMessage());
            return null;
        }
    }

    @Deprecated
    private InsertVertexEntity insertVertexQuery(long meta_id) {
        try {
            String sql = "WITH pk AS ("
                + " SELECT li.label_id, (SELECT column_name FROM tb_column_info WHERE column_id = si.source_pk_column_id) AS source_pk_column, pi.property_name AS meta_pk_column"
                + " FROM tb_label_info li"
                + " JOIN tb_property_info pi ON pi.label_id = li.label_id"
                + " JOIN tb_source_info si ON si.source_id = pi.source_id"
                + " WHERE li.meta_id = ?"
                + " AND li.use_yn = 'Y'"
                + " AND si.meta_key_yn = 'Y'"
                + " GROUP BY li.label_id, si.source_pk_column_id, pi.property_name"
                + " )"
                + " SELECT li.label_id, li.label_type, li.graph_name, li.target_label_name, pk.source_pk_column, pk.meta_pk_column, string_agg((SELECT column_name FROM tb_column_info ci JOIN tb_source_info si ON ci.column_id = si.column_id WHERE si.source_id = pi.source_id), ',') AS source_column_name, string_agg(pi.property_name, ',') AS property_name"
                + " FROM tb_label_info li"
                + " JOIN tb_property_info pi ON pi.label_id = li.label_id"
                + " JOIN pk ON pk.label_id = li.label_id"
                + " GROUP BY li.label_id, li.label_type, li.graph_name, li.target_label_name, pk.source_pk_column, pk.meta_pk_column";
            
            return jdbcTemplate.queryForObject(sql, new Object[] {meta_id}, (rs, rowNum) ->
                new InsertVertexEntity(
                    rs.getLong("label_id"),
                    rs.getString("label_type"),
                    rs.getString("graph_name"),
                    rs.getString("target_label_name"),
                    rs.getString("source_pk_column"),
                    rs.getString("meta_pk_column"),
                    rs.getString("source_column_name"),
                    rs.getString("property_name")
                )
            );
        } catch(Exception e) {
            System.out.println(e.getMessage());
            return null;
        }
    }

    @Deprecated
    public void updateData(JsonObject after, Map<String, String> map, String schemaName, String tableName) {
        List<MetaEntity> metaEntityList = selectMetaQuery(schemaName, tableName);
        if(metaEntityList.size() > 0) {
            for(int i=0; i<metaEntityList.size(); i++) {
                String[] metaData = metaEntityList.get(i).getMeta_data().split(",");
                String pkColumn = metaData[2];
                String[] mappedData = metaEntityList.get(i).getMapped_data().toString().split("!");
                String[] mappedDataDetail = {};
                String[] propertyData = metaEntityList.get(i).getProperty_data().split(",");

                String setClause = "";
                String columns = "";
                String JOINQuery = "";
                for(int j=3; j<metaData.length; j++) {
                    if("".equals(setClause)) {
                        setClause = "b." + metaData[j] + " = " + after.getAsJsonObject().get(metaData[j]) + "'::text";
                    } else {
                        setClause = setClause + ", b." + metaData[j] + " = " + after.getAsJsonObject().get(metaData[j]) + "'::text";
                    }
                }
                for(int j=0; j<mappedData.length; j++) {
                    mappedDataDetail = mappedData[j].split(",");
                    String alias = " b" + j;
                    JOINQuery = JOINQuery +  " LEFT JOIN " + mappedDataDetail[0] + "." + mappedDataDetail[1] + alias
                        + " ON a." + pkColumn + " =" + alias + "." + mappedDataDetail[2];
                    for(int k=3; k<mappedDataDetail.length; k++) {
                        if("".equals(setClause)) {
                            setClause = alias + "." + mappedDataDetail[k] + " = " + after.getAsJsonObject().get(mappedDataDetail[k]) + "'::text";
                        } else {
                            setClause = setClause + ", b." + mappedDataDetail[k] + " = " + after.getAsJsonObject().get(mappedDataDetail[k]) + "'::text";
                        }
                    }
                }
                String metaSchema = metaEntityList.get(i).getMeta_schema_name();
                String metaTable = metaEntityList.get(i).getMeta_table_name();
                String updateForMetaSql = "UPDATE " + metaSchema + "." + metaTable + " a"
                    + " SET " + setClause
                    + " FROM " + metaData[0] + "." + metaData[1] + " b"
                    + JOINQuery
                    + " WHERE a." + propertyData[0] + " = " + after.getAsJsonObject().get(metaData[2]).getAsString() + "::text";
                System.out.println(updateForMetaSql);
                
            }
        }
/*
        String[] pkCol = labelInfoEntity.getSource_pk_columns().split(",");
        String conditions = "";
        for(int i=0; i<pkCol.length; i++) {
            conditions = conditions + " AND properties ->> '" + pkCol[i] + "' = '" + after.getAsJsonObject().get(pkCol[i]) + "'::text";
        }

        String[] labelProp = labelInfoEntity.getTarget_label_properties().split(",");
        String[] tbColumn = labelInfoEntity.getSource_columns().split(",");
        String setclaus = "";
        for(int i=0; i<labelProp.length; i++) {
             */
            /* Todo timestamp같이 age에 없는 데이터 타입 변경해야 함
            String afterCol = after.getAsJsonObject().get(tbColumn[i]).getAsString();
            if(map.get("\"" + tbColumn[i] + "\"") != null) {
                String val = map.get("\"" + tbColumn[i] + "\"");
                if(val.contains("Timestamp")) {
                    afterCol = getTimestampToDate(afterCol);
                }
            }
            */
            /*
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
            + " WHERE 1 = 1 " + conditions;
        jdbcTemplate.execute(sql);
         */
    }

    @Deprecated
    public void deleteData(JsonObject after, String schemaName, String tableName) {

    }

    /*
    @Deprecated
    public void insertVertexData(JsonObject after, ColumnInfoEntity columnInfoEntity) {
        String[] pkCol = columnInfoEntity.getSource_pk_columns().split(",");
        String conditions = "";
        for(int i=0; i<pkCol.length; i++) {
            conditions = conditions + " AND " + pkCol[i] + " = " + after.getAsJsonObject().get(pkCol[i]);
        }

        String[] labelProp = columnInfoEntity.getTarget_label_properties().split(",");
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
            + ") AS a;";
        jdbcTemplate.execute(sql);
    }
    */

    /*
    @Deprecated
    public void updateVertexData(JsonObject after, ColumnInfoEntity columnInfoEntity, Map<String, String> map) {
        String[] pkCol = labelInfoEntity.getSource_pk_columns().split(",");
        String conditions = "";
        for(int i=0; i<pkCol.length; i++) {
            conditions = conditions + " AND properties ->> '" + pkCol[i] + "' = '" + after.getAsJsonObject().get(pkCol[i]) + "'::text";
        }

        String[] labelProp = labelInfoEntity.getTarget_label_properties().split(",");
        String[] tbColumn = labelInfoEntity.getSource_columns().split(",");
        String setclaus = "";
        for(int i=0; i<labelProp.length; i++) {
             */
            /* Todo timestamp같이 age에 없는 데이터 타입 변경해야 함
            String afterCol = after.getAsJsonObject().get(tbColumn[i]).getAsString();
            if(map.get("\"" + tbColumn[i] + "\"") != null) {
                String val = map.get("\"" + tbColumn[i] + "\"");
                if(val.contains("Timestamp")) {
                    afterCol = getTimestampToDate(afterCol);
                }
            }
            */
/*
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
            + " WHERE 1 = 1 " + conditions;
        jdbcTemplate.execute(sql);
    }
    */

    /*
    @Deprecated
    public void deleteVertexData(JsonObject before, ColumnInfoEntity columnInfoEntity) {
        String[] pkCol = labelInfoEntity.getSource_pk_columns().split(",");
        String conditions = "";
        for(int i=0; i<pkCol.length; i++) {
            conditions = conditions + " AND properties ->> '" + pkCol[i] + "' = '" + before.getAsJsonObject().get(pkCol[i]) + "'::text";
        }
        
        String graphName = labelInfoEntity.getGraph_name();
        String labelName = labelInfoEntity.getTarget_label_name();

        String sql = 
            "delete from " + graphName + "." + labelName
            + " WHERE 1 = 1 " + conditions;
        jdbcTemplate.execute(sql);
    }
    */

    /*
    @Deprecated
    public void insertEdgeData(JsonObject after, ColumnInfoEntity columnInfoEntity) {
        String startLb = after.getAsJsonObject().get("start_id").getAsString();
        String endLb = after.getAsJsonObject().get("end_id").getAsString();
        String conditions = " AND start_id = " + startLb + " AND end_id = " + endLb;
        
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
            + "  $$) AS a (startid agtype, " + startLabelId +" agtype) "
            + "  ON tb." + targetStartLabelId + " = a." + startLabelId
            + "  JOIN cypher('" + graphName + "', $$ "
            + "    MATCH(v:" + endLabelName + ") RETURN id(v), v." + endLabelId
            + "  $$) AS b (endid agtype, " + endLabelId +" agtype) "
            + "  ON tb." + targetEndLabelId + " = b." + endLabelId
            + "  WHERE 1 = 1 " + conditions
            + ") AS data;";
        jdbcTemplate.execute(sql);
    }
    */

    /*
    @Deprecated
    public void updateEdgeData(JsonObject after, ColumnInfoEntity columnInfoEntity, Map<String, String> map) {
        String startLb = after.getAsJsonObject().get("start_id").getAsString();
        String endLb = after.getAsJsonObject().get("end_id").getAsString();
        String startLabelId = labelInfoEntity.getStart_label_id();
        String endLabelId = labelInfoEntity.getEnd_label_id();
        String targetStartLabelId = labelInfoEntity.getTarget_start_label_id();
        String targetEndLabelId = labelInfoEntity.getTarget_end_label_id();
        String conditions = " AND a." + targetStartLabelId + " = b." + startLabelId
            + " AND a." + targetEndLabelId + " = " + endLabelId
            + " AND b.egid = c.id";
        
        String graphName = labelInfoEntity.getGraph_name();
        String targetLabelName = labelInfoEntity.getTarget_label_name();
        String sourceSchema = labelInfoEntity.getSource_schema();
        String sourceTableName = labelInfoEntity.getSource_table_name();
        String startLabelName = labelInfoEntity.getStart_label_name();
        String endLabelName = labelInfoEntity.getEnd_label_name();

        String[] labelProp = labelInfoEntity.getTarget_label_properties().split(",");
        String[] tbColumn = labelInfoEntity.getSource_columns().split(",");
        String setclaus = "";
        for(int i=0; i<labelProp.length; i++) {
            if(i > 0) {
                setclaus = setclaus + ", \"" + labelProp[i] + "\":" + after.getAsJsonObject().get(tbColumn[i]);
            } else {
                setclaus = "\"" + labelProp[i] + "\":" + after.getAsJsonObject().get(tbColumn[i]);
            }
        }

        String sql = 
            "update " + graphName + "." + targetLabelName + " c"
            + " set properties = concat('{" + setclaus + "}')::agtype"
            + " from " + sourceSchema + "." + sourceTableName + " a,"
            + "("
            + " select * from cypher('" + graphName + "', $$"
            + " match(v:" + startLabelName + ")-[e:" + targetLabelName + "]->(v2:" + endLabelName + ")"
            + " WHERE v." + startLabelId + " = " + startLb
            + " AND v2." + endLabelId + " = " + endLb
            + " return v." + startLabelId + ", id(e), v2." + endLabelId + ""
            + " $$) AS (" + startLabelId + " agtype, egid agtype, " + endLabelId + " agtype)"
            + ") b"
            + " WHERE 1 = 1 " + conditions;
        jdbcTemplate.execute(sql);
    }
    */

    /*
    @Deprecated
    public void deleteEdgeData(JsonObject before, ColumnInfoEntity columnInfoEntity) {
        String startLb = before.getAsJsonObject().get("start_id").getAsString();
        String endLb = before.getAsJsonObject().get("end_id").getAsString();
        String startLabelId = labelInfoEntity.getStart_label_id();
        String endLabelId = labelInfoEntity.getEnd_label_id();
        String conditions = " AND v." + startLabelId + " = " + startLb + " AND v2." + endLabelId + " = " + endLb;
        
        String graphName = labelInfoEntity.getGraph_name();
        String targetLabelName = labelInfoEntity.getTarget_label_name();
        String startLabelName = labelInfoEntity.getStart_label_name();
        String endLabelName = labelInfoEntity.getEnd_label_name();

        String sql = 
            "select * from cypher('" + graphName + "', $$"
            + "match(v:" + startLabelName + ")-[e:" + targetLabelName + "]->(v2:" + endLabelName + ")"
            + " WHERE 1 = 1 " + conditions
            + " detach delete e"
            + " $$) AS (e agtype)";
             */
        /* 테스트
         * select * from test.acted_in a,
            person b,
            movie c
            WHERE 1=1
            AND a.start_id = b.id
            AND a.end_id = c.id
            AND b.properties ->> 'act_no' = 7::text
            AND c.properties ->> 'movie_no' = 1003::text;
        //jdbcTemplate.execute(sql);
    }
    */

    private String getTimestampToDate(String timestampStr) {
        long timestamp = Long.parseLong(timestampStr);
        Date date = new java.util.Date(timestamp*1000L);
        SimpleDateFormat sdf = new java.text.SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        sdf.setTimeZone(java.util.TimeZone.getTimeZone("GMT+9"));
        return sdf.format(date);
    }
}