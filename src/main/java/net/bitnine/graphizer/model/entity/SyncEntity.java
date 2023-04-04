package net.bitnine.graphizer.model.entity;

import java.io.Serializable;

import javax.persistence.Entity;
import javax.persistence.Id;

// import org.springframework.data.relational.core.mapping.Table;

import lombok.Builder;
import lombok.Getter;
import lombok.Setter;

@Entity
@Getter
@Setter
@Builder
public class SyncEntity implements Serializable {
    @Id
    private String rdb_schema;
    @Id
    private String rdb_table_name;
    @Id
    private int rdb_table_oid;
    @Id
    private String rdb_columns;
    @Id
    private String rdb_pk_columns;
    @Id
    private String graph_name;
    @Id
    private String label_name;
    @Id
    private int label_oid;
    @Id
    private String label_properties;

    public SyncEntity() {
	}
    
    @Builder
    public SyncEntity(String rdb_schema, String rdb_table_name, int rdb_table_oid, String rdb_columns, String rdb_pk_columns,
                    String graph_name, String label_name, int label_oid, String label_properties) {
		this.rdb_schema = rdb_schema;
		this.rdb_table_name = rdb_table_name;
		this.rdb_table_oid = rdb_table_oid;
        this.rdb_columns = rdb_columns;
        this.rdb_pk_columns = rdb_pk_columns;
        this.graph_name = graph_name;
        this.label_name = label_name;
        this.label_oid = label_oid;
        this.label_properties = label_properties;
	}
}
