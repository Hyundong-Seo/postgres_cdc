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
public class InsertVertexEntity implements Serializable {
    @Id
    private long label_id;
    @Id
    private String graph_name;
    @Id
    private String target_label_name;
    @Id
    private String source_pk_column;
    @Id
    private String meta_pk_column;
    @Id
    private String source_column_name;
    @Id
    private String property_name;

    public InsertVertexEntity() {
	}
    
    @Builder
    public InsertVertexEntity(long label_id, String graph_name, String target_label_name, String source_pk_column, String meta_pk_column,
                            String source_column_name, String property_name) {
		this.label_id = label_id;
		this.graph_name = graph_name;
		this.target_label_name = target_label_name;
        this.source_pk_column = source_pk_column;
        this.meta_pk_column = meta_pk_column;
        this.source_column_name = source_column_name;
        this.property_name = property_name;
	}
}
