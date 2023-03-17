package net.bitnine.graphizer.common;

import java.sql.Connection;
import java.sql.Statement;

import javax.sql.DataSource;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;

@Component
public class PostgreSQLRunner implements ApplicationRunner {
    @Autowired
    DataSource dataSource;

    @Autowired
    JdbcTemplate jdbcTemplate;

    @Override
    public void run(ApplicationArguments args) throws Exception {
        try (Connection connection = dataSource.getConnection()) {
            Statement statement = connection.createStatement();
            String sql = "CREATE TABLE IF NOT EXISTS tb_sync ("
                + "oid_no oid,"
                + "ctid_no tid)";
            statement.executeUpdate(sql);
            
            sql = " WITH oidb AS ( "
            + "SELECT t.table_catalog AS dbname, t.table_name AS tbname, pgc.oid AS oid "
            + "FROM information_schema.tables t "
            + "INNER JOIN pg_catalog.pg_class pgc "
            + "ON t.table_name = pgc.relname "
            + "WHERE t.table_type = 'BASE TABLE' "
            + "AND t.table_name = 'sample' "
            + ") "
            + "SELECT (SELECT oidb.oid FROM oidb) AS oid, a.ctid "
            + "FROM sample a;";
            jdbcTemplate.execute("INSERT INTO tb_sync" + sql);
        } catch (Exception e) {
            System.out.println(e.getMessage());
        }
    }
}
