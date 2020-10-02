package kiyv.domain.dao;

import kiyv.domain.model.Manufacture;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.stream.IntStream;

import static kiyv.log.ClassNameUtil.getCurrentClassName;

public class ManufDaoJdbc implements ManufDao {

    private Connection connPostgres;
    private static final Logger log = LoggerFactory.getLogger(getCurrentClassName());
    private static final String SQL_GET_ONE = "SELECT * FROM manufacture WHERE id = ?;";
    private static final String SQL_GET_ALL = "SELECT * FROM manufacture;";
    private static final String SQL_DELETE = "DELETE FROM manufacture WHERE id = ?;";

    private static final String SQL_SAVE = "INSERT INTO manufacture (iddoc, position, docno, id_order, time_manuf, " +
            "time21, quantity, id_tmc, descr_second, size_a, size_b, size_c, embodiment, id) VALUES " +
            "(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);";

    private static final String SQL_UPDATE = "UPDATE manufacture SET iddoc=?, position=?, docno=?, id_order=?, time_manuf=?," +
            " time21=?, quantity=?, id_tmc=?, descr_second=?, size_a=?, size_b=?, size_c=?, embodiment=? WHERE id = ?;";

    public ManufDaoJdbc(Connection conn) {
        this.connPostgres = conn;
        log.debug("Get connection to PostgreSQL from {}.", UtilDao.class);
    }

    @Override
    public Manufacture getById(String id) {
        try {
            PreparedStatement statement = connPostgres.prepareStatement(SQL_GET_ONE);
            statement.setString(1, id);
            log.debug("Select 'Manufacture'. SQL = {}. Id = {}.", SQL_GET_ONE, id);

            ResultSet rs = statement.executeQuery();
            rs.next();

            log.debug("return new 'Manufacture'. Id = {}.", id);

            return new Manufacture(id, rs.getString("iddoc"), rs.getInt("position"),
                    rs.getString("docno"), rs.getString("id_order"), rs.getTimestamp("time_manuf"), rs.getLong("time21"),
                    rs.getInt("quantity"), rs.getString("id_tmc"), rs.getString("descr_second"), rs.getInt("size_a"),
                    rs.getInt("size_b"), rs.getInt("size_c"), rs.getString("embodiment"));

        } catch (SQLException e) {
            log.warn("Exception during reading 'Manufacture' with Id = {}.", id, e);
        }
        log.debug("Manufacture with Id = {} not found.", id);
        return null;
    }

    @Override
    public List<Manufacture> getAll() {
        List<Manufacture> result = new ArrayList<>();
        try {
            Statement statement = connPostgres.createStatement();
            ResultSet rs = statement.executeQuery(SQL_GET_ALL);
            log.debug("Select all 'Manufactures'. SQL = {}.", SQL_GET_ALL);

            while (rs.next()) {
                String id = rs.getString("id");
//                log.debug("return new 'Manufacture'. Id = {}.", id);

                Manufacture manufacture = new Manufacture(id, rs.getString("iddoc"), rs.getInt("position"),
                        rs.getString("docno"), rs.getString("id_order"), rs.getTimestamp("time_manuf"), rs.getLong("time21"),
                        rs.getInt("quantity"), rs.getString("id_tmc"), rs.getString("descr_second"), rs.getInt("size_a"),
                        rs.getInt("size_b"), rs.getInt("size_c"), rs.getString("embodiment"));
                result.add(manufacture);
            }
            log.debug("Was read {} Manufactures from Postgres.", result.size());
            return result;
        } catch (SQLException e) {
            log.warn("Exception during reading all 'Manufactures'.", e);
        }
        log.debug("Manufactures not found.");
        return null;
    }

    private boolean saveOrUpdateAll(List<Manufacture> manufactureList, String sql) {
        try {
            int result = 0;
            for (Manufacture manufacture : manufactureList) {
                PreparedStatement ps = connPostgres.prepareStatement(sql);

                log.debug("Prepared 'Manufacture' to batch. SQL = {}. {}.", sql, manufacture);

                ps.setString(14, manufacture.getId());
                ps.setString(1, manufacture.getIdDoc());
                ps.setInt(2, manufacture.getPosition());
                ps.setString(3, manufacture.getDocNumber());
                ps.setString(4, manufacture.getIdOrder());
                ps.setTimestamp(5, manufacture.getTimeManufacture());
                ps.setLong(6, manufacture.getTime21());
                ps.setInt(7, manufacture.getQuantity());
                ps.setString(8, manufacture.getIdTmc());
                ps.setString(9, manufacture.getDescrSecond());
                ps.setInt(10, manufacture.getSizeA());
                ps.setInt(11, manufacture.getSizeB());
                ps.setInt(12, manufacture.getSizeC());
                ps.setString(13, manufacture.getEmbodiment());

                if (manufacture.getTime21() != null) {
                    ps.setLong(6, manufacture.getTime21());
                } else {
                    ps.setLong(6, 0L);
                }

                ps.addBatch();
                int[] numberOfUpdates = ps.executeBatch();
                result += IntStream.of(numberOfUpdates).sum();
            }
            if (result == manufactureList.size()) {
                log.debug("Try commit");
                connPostgres.commit();
                log.debug("Commit - OK. {} Manufactures saved/updated.", result);
                return true;
            }
            else {
                log.debug("Saved/Updated {}, but need to save/update {} Manufactures. Not equals!!!", result, manufactureList.size());
                connPostgres.rollback();
            }
        } catch (SQLException e) {
            log.warn("Exception during save/update {} new 'Manufactures'. SQL = {}.", manufactureList.size() , sql, e);
        }
        return false;
    }

    @Override
    public boolean saveAll(List<Manufacture> manufactureList) {
        return saveOrUpdateAll(manufactureList, SQL_SAVE);
    }

    @Override
    public boolean updateAll(List<Manufacture> manufactureList) {
        return saveOrUpdateAll(manufactureList, SQL_UPDATE);
    }

    @Override
    public boolean deleteAll(Collection<String> listId) {
        try {
            int result = 0;
            for (String idDoc : listId) {
                PreparedStatement ps = connPostgres.prepareStatement(SQL_DELETE);
                ps.setString(1, idDoc);
                ps.addBatch();
                int[] numberOfUpdates = ps.executeBatch();
                result += IntStream.of(numberOfUpdates).sum();
            }
            if (result == listId.size()) {
                log.debug("Try commit");
                connPostgres.commit();
                log.debug("Commit - OK. {} Manufactures deleted.", result);
                return true;
            }
            else {
                connPostgres.rollback();
                log.debug("Deleted {}, but need to delete {} Manufactures. Not equals!!!", result, listId.size());
            }
        } catch (SQLException e) {
            log.warn("Exception during delete {} old 'Manufactures'. SQL = {}.", listId.size() , SQL_DELETE, e);
        }
        return false;
    }

}
