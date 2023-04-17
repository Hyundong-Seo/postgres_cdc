package net.bitnine.graphizer.model.entity;

import java.io.Serializable;

import javax.persistence.Entity;
import javax.persistence.Id;

import lombok.Builder;
import lombok.Getter;
import lombok.Setter;

@Entity
@Getter
@Setter
@Builder
public class LabelInfoEntity implements Serializable {
    @Id
    private String label_type;
    @Id
    private String source_schema;
    @Id
    private String source_table_name;
    @Id
    private int source_table_oid;
    @Id
    private String source_columns;
    @Id
    private String source_pk_columns;
    @Id
    private String graph_name;
    @Id
    private String start_label_name;
    @Id
    private int start_label_oid;
    @Id
    private String start_label_id;
    @Id
    private String end_label_name;
    @Id
    private int end_label_oid;
    @Id
    private String end_label_id;
    @Id
    private String target_label_name;
    @Id
    private int target_label_oid;
    @Id
    private String target_start_label_id;
    @Id
    private String target_end_label_id;
    @Id
    private String target_label_properties;

    public LabelInfoEntity() {
	}
    
    @Builder
    public LabelInfoEntity(String label_type, String source_schema, String source_table_name, int source_table_oid, String source_columns, String source_pk_columns,
                    String graph_name, String start_label_name, int start_label_oid, String start_label_id, String end_label_name, int end_label_oid, String end_label_id, String target_label_name, int target_label_oid, String target_start_label_id, String target_end_label_id, String target_label_properties) {
		this.label_type = label_type;
		this.source_schema = source_schema;
		this.source_table_name = source_table_name;
        this.source_table_oid = source_table_oid;
        this.source_columns = source_columns;
        this.source_pk_columns = source_pk_columns;
        this.graph_name = graph_name;
        this.start_label_name = start_label_name;
        this.start_label_oid = start_label_oid;
        this.start_label_id = start_label_id;
        this.end_label_name = end_label_name;
        this.end_label_oid = end_label_oid;
        this.end_label_id = end_label_id;
        this.target_label_name = target_label_name;
        this.target_label_oid = target_label_oid;
        this.target_start_label_id = target_start_label_id;
        this.target_end_label_id = target_end_label_id;
        this.target_label_properties = target_label_properties;
	}
}
