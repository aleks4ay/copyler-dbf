package kiyv.domain.dao;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import kiyv.domain.model.Tmc;

import java.sql.*;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.stream.IntStream;

import static kiyv.log.ClassNameUtil.getCurrentClassName;

public class TmcDaoJdbc implements TmcDao {

    private Connection connPostgres;
    private static final Logger log = LoggerFactory.getLogger(getCurrentClassName());
    private static final String SQL_GET_ONE = "SELECT * FROM tmc WHERE id = ?;";
    private static final String SQL_GET_ALL = "SELECT * FROM tmc;";
    private static final String SQL_DELETE = "DELETE FROM tmc WHERE id = ?;";
    private static final String SQL_SAVE =
            "INSERT INTO tmc (id_parent, code, descr, is_folder, descr_all, type, id) VALUES (?, ?, ?, ?, ?, ?, ?);";
    private static final String SQL_UPDATE =
            "UPDATE tmc SET id_parent=?, code=?, descr=?, is_folder=?, descr_all=?, type=? WHERE id = ?;";


    public TmcDaoJdbc(Connection conn) {
        this.connPostgres = conn;
        log.debug("Get connection to PostgreSQL from {}.", UtilDao.class);
    }

    @Override
    public Tmc getById(String id) {
        try {
            PreparedStatement statement = connPostgres.prepareStatement(SQL_GET_ONE);
            statement.setString(1, id);
            log.debug("Select 'Tmc'. SQL = {}. id = {}.", SQL_GET_ONE, id);
            ResultSet rs = statement.executeQuery();

            rs.next();
            log.debug("return new 'Tmc'. id = {}.", id);

            return new Tmc(id, rs.getString("id_parent"), rs.getString("code"), rs.getString("descr"),
                    rs.getInt("is_folder"), rs.getString("descr_all"), rs.getString("type"));

        } catch (SQLException e) {
            log.warn("Exception during reading 'Tmc' with id = {}.", id, e);
        }
        log.debug("Tmc with id = {} not found.", id);
        return null;
    }

    @Override
    public List<Tmc> getAll() {
        List<Tmc> result = new ArrayList<>();
        try {
            Statement statement = connPostgres.createStatement();
            ResultSet rs = statement.executeQuery(SQL_GET_ALL);
            log.debug("Select all 'Tmc'. SQL = {}.", SQL_GET_ALL);
            while (rs.next()) {
                result.add(new Tmc(rs.getString("id"), rs.getString("id_parent"), rs.getString("code"), rs.getString("descr"),
                        rs.getInt("is_folder"), rs.getString("descr_all"), rs.getString("type")));
            }
            log.debug("Was read {} Tmc from Postgres.", result.size());
            return result;
        } catch (SQLException e) {
            log.warn("Exception during reading all 'Tmc'.", e);
        }
        log.debug("Tmc not found.");
        return null;
    }


    public boolean saveOrUpdateAll(List<Tmc> tmcList, String sql) {
        try {
            int result = 0;
            for (Tmc tmc : tmcList) {
                PreparedStatement ps = connPostgres.prepareStatement(sql);
                log.debug("Prepared 'Tmc' to batch. SQL = {}. Tmc = {}.", sql, tmc);

                ps.setString(1, tmc.getIdParent());
                ps.setString(2, tmc.getCode());
                ps.setString(3, tmc.getDescr());
                ps.setInt(4, tmc.getIsFolder());
                ps.setString(5, tmc.getDescrAll());
                ps.setString(6, tmc.getType());
                ps.setString(7, tmc.getId());

                ps.addBatch();
                int[] numberOfUpdates = ps.executeBatch();
                result += IntStream.of(numberOfUpdates).sum();
            }
            if (result == tmcList.size()) {
                log.debug("Try commit");
                connPostgres.commit();
                log.debug("Commit - OK. {} Tmc Saved/Updated.", result);
                return true;
            }
            else {
                log.debug("Saved/Updated {}, but need to save/update {} Tmc. Not equals!!!", result, tmcList.size());
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
                log.debug("Prepared old 'Tmc' for delete to batch. SQL = {}. Id = {}.", SQL_DELETE, id);
                PreparedStatement ps = connPostgres.prepareStatement(SQL_DELETE);
                ps.setString(1, id);
                ps.addBatch();
                int[] numberOfUpdates = ps.executeBatch();
                result += IntStream.of(numberOfUpdates).sum();
            }
            if (result == listId.size()) {
                log.debug("Try commit");
                connPostgres.commit();
                log.debug("Commit - OK. {} Tmc deleted.", result);
                return true;
            }
            else {
                connPostgres.rollback();
                log.debug("Deleted {}, but need to delete {} Tmc. Not equals!!!", result,listId.size());
            }
        } catch (SQLException e) {
            log.warn("Exception during delete {} old 'Tmc'. SQL = {}.", listId.size() , SQL_DELETE, e);
        }
        return false;
    }
}



