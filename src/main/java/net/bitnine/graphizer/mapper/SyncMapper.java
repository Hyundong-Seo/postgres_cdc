package net.bitnine.graphizer.mapper;

import java.sql.ResultSet;
import java.sql.SQLException;

import org.springframework.jdbc.core.RowMapper;

import net.bitnine.graphizer.model.entity.SyncEntity;

public class SyncMapper implements RowMapper<SyncEntity> {

    @Override
    public SyncEntity mapRow(ResultSet rs, int rowNum) throws SQLException {
        SyncEntity syncEntity = new SyncEntity();
        syncEntity.setOid_no(rs.getInt("oid_no"));
        syncEntity.setCtid_no(rs.getString("ctid_no"));
        syncEntity.setUpdate_type(rs.getString("update_type"));
        return syncEntity;
    }
}
