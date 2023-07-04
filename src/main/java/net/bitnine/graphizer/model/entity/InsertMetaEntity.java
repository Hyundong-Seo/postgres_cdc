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
public class InsertMetaEntity implements Serializable {
    @Id
    private long meta_id;
    @Id
    private String meta_schema_name;
    @Id
    private String meta_table_name;
    @Id
    private String meta_data;
    @Id
    private String mapped_data;
    @Id
    private String property_data;

    public InsertMetaEntity() {
	}
    
    @Builder
    public InsertMetaEntity(long meta_id, String meta_schema_name, String meta_table_name, String meta_data, String mapped_data, String property_data) {
		this.meta_id = meta_id;
        this.meta_schema_name = meta_schema_name;
		this.meta_table_name = meta_table_name;
		this.meta_data = meta_data;
        this.mapped_data = mapped_data;
        this.property_data = property_data;
	}
}
