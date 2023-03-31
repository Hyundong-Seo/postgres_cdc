package net.bitnine.graphizer.service;

import javax.sql.DataSource;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;

import net.bitnine.graphizer.model.entity.SyncEntity;

@Service
public class KafkaConsumerService {
    @Autowired
    DataSource dataSource;

    @Autowired
    JdbcTemplate jdbcTemplate;

    @Deprecated
    public SyncEntity syncData(String schemaName, String tableName) {
        String sql = "select rdb_schema, rdb_table_name, rdb_table_oid, rdb_columns, rdb_pk_columns, rdb_pk_columns_values, graph_name, label_name, label_oid, label_properties "
        + "from tb_vlabel_info "
        + "where rdb_schema = ? " 
        + "and rdb_table_name = ?";

        return jdbcTemplate.queryForObject(sql, new Object[] {schemaName, tableName}, (rs, rowNum) ->
            new SyncEntity(
                rs.getString("rdb_schema"),
                rs.getString("rdb_table_name"),
                rs.getInt("rdb_table_oid"),
                rs.getString("rdb_columns"),
                rs.getString("rdb_pk_columns"),
                rs.getString("rdb_pk_columns_values"),
                rs.getString("graph_name"),
                rs.getString("label_name"),
                rs.getInt("label_oid"),
                rs.getString("label_properties")
            )
        );
    }

    @Deprecated
    public void insertVertexData(String conditions, SyncEntity syncEntity) {
        String buildmap = "";

        String[] labelProp = syncEntity.getLabel_properties().split(",");
        for(int i=0; i<labelProp.length; i++) {
            if(i > 0) {
                buildmap = buildmap + ", '" + labelProp[i] + "', a." + labelProp[i];
            } else {
                buildmap = "'" + labelProp[i] + "', a." + labelProp[i];
            }
        }
        String graphName = syncEntity.getGraph_name();
        String labelName = syncEntity.getLabel_name();
        String colms = syncEntity.getRdb_columns();
        String rschem = syncEntity.getRdb_schema();
        String tbname = syncEntity.getRdb_table_name();

        String sql2 = 
            "INSERT INTO " + graphName + "." + labelName
            + " SELECT _graphid((_label_id('" + graphName + "'::name, '" + labelName + "'::name))::integer, nextval('" + graphName + "." + labelName + "_id_seq'::regclass)), "
            + "        agtype_build_map ("+ buildmap +") "
            + "FROM "
            + "( "
            + "        SELECT "+ colms
            + "        FROM  "+ rschem +"."+ tbname
            + "        WHERE 1 = 1 " + conditions
            + ") as a;";
        jdbcTemplate.execute(sql2);
    }
}