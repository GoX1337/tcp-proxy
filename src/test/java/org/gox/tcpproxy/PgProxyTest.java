package org.gox.tcpproxy;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;
import org.postgresql.jdbc.PgConnection;

import java.sql.*;
import java.util.Properties;

class PgProxyTest {

    @Test
    public void jdbcConnect() throws SQLException {
        String url = "jdbc:postgresql://localhost:5666/test";
        Properties props = new Properties();
        props.setProperty("user", "test");
        props.setProperty("password", "test");
        props.setProperty("ssl", "false");

        PgConnection connection = DriverManager.getConnection(url, props).unwrap(PgConnection.class);
        String SQL_QUERY = "select now()";
        PreparedStatement pst = connection.prepareStatement(SQL_QUERY);

        ResultSet rs = pst.executeQuery();
        Timestamp timestamp = null;
        while (rs.next()) {
            timestamp = rs.getTimestamp(1);
        }
        rs.close();

        Assertions.assertThat(timestamp).isNotNull();
    }
}