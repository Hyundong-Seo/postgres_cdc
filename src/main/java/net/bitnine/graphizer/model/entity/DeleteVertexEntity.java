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
public class DeleteVertexEntity implements Serializable {
    @Id
    private long label_id;
    @Id
    private String graph_name;
    @Id
    private String target_label_name;
    @Id
    private String meta_pk_column;

    public DeleteVertexEntity() {
	}
    
    @Builder
    public DeleteVertexEntity(long label_id, String graph_name, String target_label_name, String meta_pk_column) {
		this.label_id = label_id;
		this.graph_name = graph_name;
		this.target_label_name = target_label_name;
        this.meta_pk_column = meta_pk_column;
	}
}
