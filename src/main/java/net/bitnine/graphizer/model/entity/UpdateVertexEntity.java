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
public class UpdateVertexEntity implements Serializable {
    @Id
    private String meta_schema_name;
    @Id
    private String meta_table_name;
    @Id
    private String graph_name;
    @Id
    private String target_label_name;
    @Id
    private String key_property;

    public UpdateVertexEntity() {
	}
    
    @Builder
    public UpdateVertexEntity(String meta_schema_name, String meta_table_name, String graph_name, String target_label_name, String key_property) {
		this.meta_schema_name = meta_schema_name;
        this.meta_table_name = meta_table_name;
		this.graph_name = graph_name;
		this.target_label_name = target_label_name;
        this.key_property = key_property;
	}
}
