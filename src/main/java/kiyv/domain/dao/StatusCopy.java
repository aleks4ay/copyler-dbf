package kiyv.domain.dao;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.IntStream;

import static kiyv.log.ClassNameUtil.getCurrentClassName;

public class StatusCopy {

    private Connection connPostgresFrom;
    private Connection connPostgresTo;
    private static final Logger log = LoggerFactory.getLogger(getCurrentClassName());

    private static final String SQL_GET_ALL = "SELECT * FROM statuses;";

    public StatusCopy(Connection connFrom, Connection connTo) {
        this.connPostgresFrom = connFrom;
        this.connPostgresTo = connTo;
        log.debug("Get 2 connections to PostgreSQL from {}.", UtilDao.class);
    }

    public List<String> getAll() {
        List<String> result = new ArrayList<>();
        try {
            Statement statement = connPostgresFrom.createStatement();
            ResultSet rs = statement.executeQuery(SQL_GET_ALL);
            log.debug("Select all 'Statuses'. SQL = {}.", SQL_GET_ALL);

            while (rs.next()) {
                String idDoc = rs.getString("iddoc");
                String kod = String.valueOf(rs.getInt("kod"));
                String posString = kod.substring(kod.length()-2, kod.length());
                int posInt = Integer.parseInt(posString);
                String id = idDoc + "-" + posInt;

                String sql = "UPDATE statuses SET ";

                for (int i = 2; i < 25; i++) {
                    Long time = rs.getLong("time_" + i);
                    if (rs.getLong("time_" + i) != 0) {
                        sql += "time_" + i + "=" + rs.getLong("time_" + i) + ", ";
                    }
                }
                sql += "type_index=" + rs.getInt("type_index") + ", ";
                sql += "status_index=" + rs.getInt("status_index") + ", ";
                String designer = rs.getString("designer_name");
                if (designer != null) {
                    sql += "designer_name='" + designer + "', ";
                }
                sql += "is_technologichka=" + rs.getInt("is_technologichka") + ", ";
                sql += "descr_first='" + rs.getString("descr_first") + "', ";
                sql += "is_parsing=" + rs.getInt("is_parsing") + " ";
                sql += " WHERE id='" + id + "';";

                System.out.println("sql=" + sql);
                result.add(sql);

            }
            log.debug("Was read {} Statuses.", result.size());
            return result;
        } catch (SQLException e) {
            log.warn("Exception during reading all 'Statuses'.", e);
        }
        log.debug("Statuses not found.");
        return null;
    }

    public boolean updateState(List<String> sqlList) {
        try {
            int result = 0;
            for (String sql : sqlList) {
                Statement statement = connPostgresTo.createStatement();

                log.debug("SQL = {}", sql);
                statement.addBatch(sql);

                int[] numberOfUpdates = statement.executeBatch();
                result += IntStream.of(numberOfUpdates).sum();
            }

            log.debug("Try commit");
            connPostgresTo.commit();
            log.debug("Commit - OK. {} state Statuses updated.", result);
            return true;

        } catch (SQLException e) {
            log.warn("Exception during save/update {} state 'Statuses'.", sqlList.size() , e);
        }
        return false;
    }

}
