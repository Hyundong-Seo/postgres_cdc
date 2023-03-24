package net.bitnine.graphizer.model.entity;

import java.io.Serializable;
import java.sql.Time;
import java.sql.Timestamp;

import javax.persistence.Entity;
import javax.persistence.Id;

import org.springframework.data.relational.core.mapping.Table;

import lombok.Builder;
import lombok.Getter;
import lombok.Setter;

@Entity
@Getter
@Setter
@Table(name = "tb_sync")
@Builder
public class SyncEntity implements Serializable {
    @Id
    private int oid_no;
    @Id
    private String ctid_no;
    @Id
    private String update_type;

    public SyncEntity() {
	}
    
    @Builder
    public SyncEntity(int oid_no, String ctid_no, String update_type) {
		this.oid_no = oid_no;
		this.ctid_no = ctid_no;
		this.update_type = update_type;
	}
}
