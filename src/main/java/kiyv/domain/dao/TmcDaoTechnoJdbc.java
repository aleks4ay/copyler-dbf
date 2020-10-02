package kiyv.domain.dao;

import kiyv.domain.model.Tmc;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.stream.IntStream;

import static kiyv.log.ClassNameUtil.getCurrentClassName;

public class TmcDaoTechnoJdbc implements TmcDao {

    private static Connection connPostgres;
    private static final Logger log = LoggerFactory.getLogger(getCurrentClassName());
    private static final String SQL_GET_ONE = "SELECT * FROM techno_item WHERE id = ?;";
    private static final String SQL_GET_ALL = "SELECT * FROM techno_item;";
    private static final String SQL_DELETE = "DELETE FROM techno_item WHERE id = ?;";
    private static final String SQL_SAVE = "INSERT INTO techno_item (id_parent, descr, id) VALUES (?, ?, ?);";
    private static final String SQL_UPDATE = "UPDATE techno_item SET id_parent=?, descr=? WHERE id = ?;";


    public TmcDaoTechnoJdbc(Connection conn) {
        connPostgres = conn;
        log.debug("Get connection to PostgreSQL from {}.", UtilDao.class);
    }

    @Override
    public Tmc getById(String id) {
        try {
            PreparedStatement statement = connPostgres.prepareStatement(SQL_GET_ONE);
            statement.setString(1, id);
            log.debug("Select 'Tmc-techno'. SQL = {}. id = {}.", SQL_GET_ONE, id);
            ResultSet rs = statement.executeQuery();

            rs.next();
//            log.debug("return new 'Tmc-techno'. id = {}.", id);
            Tmc tmc = new Tmc(id);
            tmc.setIdParent(rs.getString("id_parent"));
            tmc.setDescr(rs.getString("descr"));

            return tmc;

        } catch (SQLException e) {
            log.warn("Exception during reading 'Tmc-techno' with id = {}.", id, e);
        }
        log.debug("Tmc-techno with id = {} not found.", id);
        return null;
    }

    @Override
    public List<Tmc> getAll() {
        List<Tmc> result = new ArrayList<>();
        try {
            Statement statement = connPostgres.createStatement();
            ResultSet rs = statement.executeQuery(SQL_GET_ALL);
            log.debug("Select all 'Tmc-techno'. SQL = {}.", SQL_GET_ALL);
            while (rs.next()) {
                Tmc tmc = new Tmc(rs.getString("id"));
                tmc.setIdParent(rs.getString("id_parent"));
                tmc.setDescr(rs.getString("descr"));
                result.add(tmc);
            }
            log.debug("Was read {} Tmc-techno from Postgres.", result.size());
            return result;
        } catch (SQLException e) {
            log.warn("Exception during reading all 'Tmc-techno'.", e);
        }
        log.debug("Tmc-techno not found.");
        return null;
    }


    public boolean saveOrUpdateAll(List<Tmc> tmcList, String sql) {
        try {
            int result = 0;
            for (Tmc tmc : tmcList) {
                PreparedStatement ps = connPostgres.prepareStatement(sql);
                log.debug("Prepared 'Tmc-techno' to batch. SQL = {}. Tmc = {}.", sql, tmc);

                ps.setString(1, tmc.getIdParent());
                ps.setString(2, tmc.getDescr());
                ps.setString(3, tmc.getId());

                ps.addBatch();
                int[] numberOfUpdates = ps.executeBatch();
                result += IntStream.of(numberOfUpdates).sum();
            }
            if (result == tmcList.size()) {
                log.debug("Try commit");
                connPostgres.commit();
                log.debug("Commit - OK. {} Tmc-techno Saved/Updated.", result);
                return true;
            }
            else {
                log.debug("Saved/Updated {}, but need to save/update {} Tmc-techno. Not equals!!!", result, tmcList.size());
                connPostgres.rollback();
            }
        } catch (SQLException e) {
            log.warn("Exception during saving/updating {} new 'Tmc'. SQL = {}.", tmcList.size() , sql, e);
        }
        return false;
    }


    @Override
    public boolean saveAll(List<Tmc> tmcList) {
        return saveOrUpdateAll(tmcList, SQL_SAVE);
    }

    @Override
    public boolean updateAll(List<Tmc> tmcList) {
        return saveOrUpdateAll(tmcList, SQL_UPDATE);
    }

    @Override
    public boolean deleteAll(Collection<String> listId) {
        try {
            int result = 0;
            for (String id : listId) {
                log.debug("Prepared old 'Tmc-techno' for delete to batch. SQL = {}. Id = {}.", SQL_DELETE, id);
                PreparedStatement ps = connPostgres.prepareStatement(SQL_DELETE);
                ps.setString(1, id);
                ps.addBatch();
                int[] numberOfUpdates = ps.executeBatch();
                result += IntStream.of(numberOfUpdates).sum();
            }
            if (result == listId.size()) {
                log.debug("Try commit");
                connPostgres.commit();
                log.debug("Commit - OK. {} Tmc-techno deleted.", result);
                return true;
            }
            else {
                connPostgres.rollback();
                log.debug("Deleted {}, but need to delete {} Tmc-techno. Not equals!!!", result,listId.size());
            }
        } catch (SQLException e) {
            log.warn("Exception during delete {} old 'Tmc-techno'. SQL = {}.", listId.size() , SQL_DELETE, e);
        }
        return false;
    }
}



