package kiyv.domain.dao;

import kiyv.domain.model.Worker;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.stream.IntStream;

import static kiyv.log.ClassNameUtil.getCurrentClassName;

public class WorkerDaoJdbc implements WorkerDao {

    private Connection connPostgres;
    private static final Logger log = LoggerFactory.getLogger(getCurrentClassName());
    private static final String SQL_GET_ONE = "SELECT name FROM worker WHERE id = ?;";
    private static final String SQL_GET_ALL = "SELECT id, name FROM worker;";
    private static final String SQL_DELETE = "DELETE FROM worker WHERE id = ?;";
    private static final String SQL_SAVE = "INSERT INTO worker (name, id) VALUES (?, ?);";
    private static final String SQL_UPDATE = "UPDATE worker SET name = ? WHERE id = ?;";


    public WorkerDaoJdbc(Connection conn) {
        this.connPostgres = conn;
        log.debug("Get connection to PostgreSQL from {}.", UtilDao.class);
    }

    @Override
    public Worker getById(String id) {
        try {
            PreparedStatement statement = connPostgres.prepareStatement(SQL_GET_ONE);
            statement.setString(1, id);
            log.debug("Select 'Worker'. SQL = {}. id = {}.", SQL_GET_ONE, id);
            ResultSet rs = statement.executeQuery();

            rs.next();
            log.debug("return new 'Worker'. id = {}.", id);
            return new Worker(id, rs.getString("name"));

        } catch (SQLException e) {
            log.warn("Exception during reading 'Worker' with id = {}.", id, e);
        }
        log.debug("Worker with id = {} not found.", id);
        return null;
    }

    @Override
    public List<Worker> getAll() {
        List<Worker> result = new ArrayList<>();
        try {
            Statement statement = connPostgres.createStatement();
            ResultSet rs = statement.executeQuery(SQL_GET_ALL);
            log.debug("Select all 'Workers'. SQL = {}.", SQL_GET_ALL);
            while (rs.next()) {
                result.add(new Worker( rs.getString("id"), rs.getString("name")));
            }
            log.debug("Was read {} Workers from Postgres.", result.size());
            return result;
        } catch (SQLException e) {
            log.warn("Exception during reading all 'Worker'.", e);
        }
        log.debug("Workers not found.");
        return null;
    }



    private boolean saveOrUpdateAll(List<Worker> workerList, String sql) {
        try {
            int result = 0;
            for (Worker client : workerList) {
                PreparedStatement ps = connPostgres.prepareStatement(sql);
                log.debug("Prepared new 'Worker' to batch. SQL = {}. Worker = {}.", sql, client);
                ps.setString(1, client.getName());
                ps.setString(2, client.getId());
                ps.addBatch();
                int[] numberOfUpdates = ps.executeBatch();
                result += IntStream.of(numberOfUpdates).sum();
            }
            if (result == workerList.size()) {
                log.debug("Try commit");
                connPostgres.commit();
                log.debug("Commit - OK. {} Workers Saved/Updated. SQL = {}.", result, sql);
                return true;
            }
            else {
                log.debug("Saved {}, but need to save/update {} Workers. Not equals!!!", result, workerList.size());
                connPostgres.rollback();
            }
        } catch (SQLException e) {
            log.warn("Exception during saving/updating {} new 'Worker'. SQL = {}.", workerList.size() , sql, e);
        }
        return false;
    }

    @Override
    public boolean saveAll(List<Worker> workerList) {
        return saveOrUpdateAll(workerList, SQL_SAVE);
    }

    @Override
    public boolean updateAll(List<Worker> workerList) {
        return saveOrUpdateAll(workerList, SQL_UPDATE);
    }

    @Override
    public boolean deleteAll(Collection<String> listId) {
        try {
            int result = 0;
            for (String id : listId) {
                log.debug("Prepared old 'Client' for delete to batch. SQL = {}. Id = {}.", SQL_DELETE, id);
                PreparedStatement ps = connPostgres.prepareStatement(SQL_DELETE);
                ps.setString(1, id);
                ps.addBatch();
                int[] numberOfUpdates = ps.executeBatch();
                result += IntStream.of(numberOfUpdates).sum();
            }
            if (result == listId.size()) {
                log.debug("Try commit");
                connPostgres.commit();
                log.debug("Commit - OK. {} Clients deleted. SQL = {}.", result, SQL_DELETE);
                return true;
            }
            else {
                connPostgres.rollback();
                log.debug("Deleted {}, but need to delete {} clients. Not equals!!!", result, listId.size());
            }
        } catch (SQLException e) {
            log.warn("Exception during delete {} old 'Client'. SQL = {}.", listId.size() , SQL_DELETE, e);
        }
        return false;
    }
}



