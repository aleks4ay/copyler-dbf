package kiyv.domain.dao;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import kiyv.domain.model.Description;

import java.sql.*;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.stream.IntStream;

import static kiyv.log.ClassNameUtil.getCurrentClassName;

public class DescriptionDaoJdbc implements DescriptionDao {

    private Connection connPostgres;
    private static final Logger log = LoggerFactory.getLogger(getCurrentClassName());
    private static final String SQL_GET_ONE = "SELECT * FROM descriptions WHERE id = ?;";
    private static final String SQL_GET_ALL = "SELECT * FROM descriptions;";
    private static final String SQL_DELETE = "DELETE FROM descriptions WHERE id = ?;";
    private static final String SQL_SAVE = "INSERT INTO descriptions (iddoc, position, id_tmc, quantity, descr_second, " +
            "size_a, size_b, size_c, embodiment, id) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?);";
    private static final String SQL_UPDATE = "UPDATE descriptions SET iddoc=?, position=?, id_tmc=?, quantity=?, descr_second=?, " +
            "size_a=?, size_b=?, size_c=?, embodiment=? WHERE id = ?;";

    public DescriptionDaoJdbc(Connection conn) {
        this.connPostgres = conn;
        log.debug("Get connection to PostgreSQL from {}.", UtilDao.class);
    }


    @Override
    public Description getById(String id) {
        try {
            PreparedStatement statement = connPostgres.prepareStatement(SQL_GET_ONE);
            statement.setString(1, id);
            log.debug("Select 'Description'. SQL = {}. Id = {}.", SQL_GET_ONE, id);

            ResultSet rs = statement.executeQuery();
            rs.next();

            log.debug("return new 'Description'. Code = {}.", id);
            return new Description(id, rs.getString("iddoc"), rs.getInt("position"), rs.getString("id_tmc"), rs.getInt("quantity"),
                    rs.getString("descr_second"), rs.getInt("size_a"), rs.getInt("size_b"), rs.getInt("size_c"), rs.getString("embodiment"));

        } catch (SQLException e) {
            log.warn("Exception during reading 'Description' with Code = {}.", id, e);
        }
        log.debug("Description with Code = {} not found.", id);
        return null;
    }

    @Override
    public List<Description> getAll() {
        List<Description> result = new ArrayList<>();
        try {
            Statement statement = connPostgres.createStatement();
            ResultSet rs = statement.executeQuery(SQL_GET_ALL);
            log.debug("Select all 'Description'. SQL = {}.", SQL_GET_ALL);

            while (rs.next()) {
                String id = rs.getString("id");
//                log.debug("return new 'Description'. Code = '{}'.", id);

                Description description = new Description(id, rs.getString("iddoc"), rs.getInt("position"),
                        rs.getString("id_tmc"), rs.getInt("quantity"), rs.getString("descr_second"),
                        rs.getInt("size_a"), rs.getInt("size_b"), rs.getInt("size_c"), rs.getString("embodiment"));

                result.add(description);
            }
            log.debug("Was read {} Description from Postgres.", result.size());
            return result;
        } catch (SQLException e) {
            log.warn("Exception during reading all 'Description'.", e);
        }
        log.debug("Description not found.");
        return null;
    }

    private boolean saveOrUpdateAll(List<Description> descriptionList, String sql) {
        try {
            int result = 0;
            for (Description description : descriptionList) {
                PreparedStatement ps = connPostgres.prepareStatement(sql);

                log.debug("Prepared 'Description' to batch. SQL = {}. {}.", sql, description);

                ps.setString(10, description.getId());
                ps.setString(1, description.getIdDoc());
                ps.setInt(2, description.getPosition());
                ps.setString(3, description.getIdTmc());
                ps.setInt(4, description.getQuantity());
                ps.setString(5, description.getDescrSecond());
                ps.setInt(6, description.getSizeA());
                ps.setInt(7, description.getSizeB());
                ps.setInt(8, description.getSizeC());
                ps.setString(9, description.getEmbodiment());

                ps.addBatch();
                int[] numberOfUpdates = ps.executeBatch();
                result += IntStream.of(numberOfUpdates).sum();
            }
            if (result == descriptionList.size()) {
                log.debug("Try commit");
                connPostgres.commit();
                log.debug("Commit - OK. {} Description saved/updated.", result);
                return true;
            }
            else {
                log.debug("Saved/Updated {}, but need to save/update {} Description. Not equals!!!", result, descriptionList.size());
                connPostgres.rollback();
            }
        } catch (SQLException e) {
            log.warn("Exception during save/update {} new 'Description'. SQL = {}.", descriptionList.size() , sql, e);
        }
        return false;
    }

    @Override
    public boolean saveAll(List<Description> descriptionList) {
        return saveOrUpdateAll(descriptionList, SQL_SAVE);
    }

    @Override
    public boolean updateAll(List<Description> descriptionList) {
        return saveOrUpdateAll(descriptionList, SQL_UPDATE);
    }


    @Override
    public boolean deleteAll(Collection<String> listId) {
        try {
            int result = 0;
            for (String id : listId) {
                PreparedStatement ps = connPostgres.prepareStatement(SQL_DELETE);
                ps.setString(1, id);
                ps.addBatch();
                int[] numberOfUpdates = ps.executeBatch();
                result += IntStream.of(numberOfUpdates).sum();
            }
            if (result == listId.size()) {
                log.debug("Try commit");
                connPostgres.commit();
                log.debug("Commit - OK. {} Description deleted.", result);
                return true;
            }
            else {
                connPostgres.rollback();
                log.debug("Deleted {}, but need to delete {} Description. Not equals!!!", result, listId.size());
            }
        } catch (SQLException e) {
            log.warn("Exception during delete {} old 'Description'. SQL = {}.", listId.size() , SQL_DELETE, e);
        }
        return false;
    }
}
