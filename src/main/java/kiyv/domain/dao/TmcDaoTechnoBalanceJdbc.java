package kiyv.domain.dao;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.util.*;
import java.util.stream.IntStream;

import static kiyv.log.ClassNameUtil.getCurrentClassName;

public class TmcDaoTechnoBalanceJdbc {

    private static Connection connPostgres;
    private static final Logger log = LoggerFactory.getLogger(getCurrentClassName());
    private static final String SQL_GET_ALL_BALANCE = "SELECT id, store_c FROM techno_item;";
    private static final String SQL_UPDATE = "UPDATE techno_item SET store_c=? WHERE id = ?;";


    public TmcDaoTechnoBalanceJdbc(Connection conn) {
        connPostgres = conn;
        log.debug("Get connection to PostgreSQL from {}.", UtilDao.class);
    }

    public Map<String, Integer> getAllBalance() {
        Map<String, Integer> result = new HashMap<>();
        try {
            Statement statement = connPostgres.createStatement();
            ResultSet rs = statement.executeQuery(SQL_GET_ALL_BALANCE);
            log.debug("Select all 'Balance of Tmc-techno'. SQL = {}.", SQL_GET_ALL_BALANCE);
            while (rs.next()) {
                result.put(rs.getString("id"), rs.getInt("store_c"));
            }
            log.debug("Was read {} Balance of Tmc-techno from Postgres.", result.size());
            return result;
        } catch (SQLException e) {
            log.warn("Exception during reading all 'Balance of Tmc-techno'.", e);
        }
        log.debug("Balance of Tmc-techno not found.");
        return null;
    }


    public boolean updateAllBalance(Map<String, Integer> tmcList) {
        try {
            int result = 0;
            for (String idTmc : tmcList.keySet()) {
                PreparedStatement ps = connPostgres.prepareStatement(SQL_UPDATE);
                log.debug("Prepared 'Balance of Tmc-techno' to batch. SQL = {}. iDTmc = {}, balance = {}.", SQL_UPDATE,
                        idTmc, tmcList.get(idTmc));

                Integer balance = tmcList.get(idTmc);
                if (balance != null && balance >=0 ) {
                    ps.setInt(1, balance);
                    ps.setString(2, idTmc);

                    ps.addBatch();
                    ps.executeBatch();
                }
            }

            log.debug("Try commit");
            connPostgres.commit();
            log.debug("Commit - OK. {} Balance of Tmc-techno Updated.", result);
            return true;

        } catch (SQLException e) {
            log.warn("Exception during updating {} new 'Balance of Tmc-techno'. SQL = {}.", tmcList.size() , SQL_UPDATE, e);
        }
        return false;
    }

}



