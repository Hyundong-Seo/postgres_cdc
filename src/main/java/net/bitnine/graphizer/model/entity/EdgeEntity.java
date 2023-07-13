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
public class EdgeEntity implements Serializable {
    @Id
    private String graph_name;
    @Id
    private String start_label_name;
    @Id
    private String start_label_id_name;
    @Id
    private String end_label_name;
    @Id
    private String end_label_id_name;
    @Id
    private String target_label_name;
    @Id
    private String target_start_label_id_name;
    @Id
    private String target_end_label_id_name;
    @Id
    private String property_name;
    @Id
    private String meta_schema_name;
    @Id
    private String meta_table_name;
    @Id
    private String source_schema_name;
    @Id
    private String source_table_name;
    @Id
    private String column_name;

    public EdgeEntity() {
	}
    
    @Builder
    public EdgeEntity(String graph_name, String start_label_name, String start_label_id_name, String end_label_name, String end_label_id_name,
                     String target_label_name, String target_start_label_id_name, String target_end_label_id_name, String property_name,
                     String meta_schema_name, String meta_table_name, String source_schema_name, String source_table_name, String column_name) {
		this.graph_name = graph_name;
        this.start_label_name = start_label_name;
        this.start_label_id_name = start_label_id_name;
		this.end_label_name = end_label_name;
		this.end_label_id_name = end_label_id_name;
        this.target_label_name = target_label_name;
        this.target_start_label_id_name = target_start_label_id_name;
        this.target_end_label_id_name = target_end_label_id_name;
        this.property_name = property_name;
        this.meta_schema_name = meta_schema_name;
        this.meta_table_name = meta_table_name;
        this.source_schema_name = source_schema_name;
        this.source_table_name = source_table_name;
        this.column_name = column_name;
	}
}
