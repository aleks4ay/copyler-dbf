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

public class ManufTechnoDaoJdbc implements ManufDao {

    private Connection connPostgres;
    private static final Logger log = LoggerFactory.getLogger(getCurrentClassName());
    private static final String SQL_GET_ONE = "SELECT * FROM manufacture_techno WHERE id = ?;";
    private static final String SQL_GET_ALL = "SELECT * FROM manufacture_techno;";
    private static final String SQL_DELETE = "DELETE FROM manufacture_techno WHERE id = ?;";

    private static final String SQL_SAVE = "INSERT INTO manufacture_techno (id_manuf, docno, id_tmc, id_order, " +
            " position, time_manuf, quantity, descr_second, size_a, size_b, size_c, embodiment, id) VALUES " +
            "(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);";

    private static final String SQL_UPDATE = "UPDATE manufacture_techno SET id_manuf=?, docno=?, id_tmc=?, id_order=?, " +
            "position=?, time_manuf=?, quantity=?, descr_second=?, size_a=?, size_b=?, size_c=?, embodiment=? WHERE id = ?;";

    public ManufTechnoDaoJdbc(Connection conn) {
        this.connPostgres = conn;
    }

    @Override
    public Manufacture getById(String id) {
        try {
            PreparedStatement statement = connPostgres.prepareStatement(SQL_GET_ONE);
            statement.setString(1, id);
            ResultSet rs = statement.executeQuery();
            rs.next();
            return new Manufacture(id, rs.getString("id_manuf"), rs.getInt("position"),
                    rs.getString("docno"), rs.getString("id_order"), rs.getTimestamp("time_manuf"), 0L,
                    rs.getInt("quantity"), rs.getString("id_tmc"), rs.getString("descr_second"), rs.getInt("size_a"),
                    rs.getInt("size_b"), rs.getInt("size_c"), rs.getString("embodiment"));
        } catch (SQLException e) {
            log.warn("Exception during reading 'Manufacture' for Techno with Id = {}.", id, e);
        }
        log.debug("Manufacture for Techno with Id = {} not found.", id);
        return null;
    }

    @Override
    public List<Manufacture> getAll() {
        List<Manufacture> result = new ArrayList<>();
        try {
            Statement statement = connPostgres.createStatement();
            ResultSet rs = statement.executeQuery(SQL_GET_ALL);
            while (rs.next()) {
                String id = rs.getString("id");
                Manufacture manufacture = new Manufacture(id, rs.getString("id_manuf"), rs.getInt("position"),
                        rs.getString("docno"), rs.getString("id_order"), rs.getTimestamp("time_manuf"), 0L,
                        rs.getInt("quantity"), rs.getString("id_tmc"), rs.getString("descr_second"), rs.getInt("size_a"),
                        rs.getInt("size_b"), rs.getInt("size_c"), rs.getString("embodiment"));
                result.add(manufacture);
            }
            log.debug("Was read {} Manufactures for Techno from Postgres.", result.size());
            return result;
        } catch (SQLException e) {
            log.warn("Exception during reading all 'Manufactures' for Techno.", e);
        }
        log.debug("Manufactures for Techno not found.");
        return null;
    }

    private boolean saveOrUpdateAll(List<Manufacture> manufactureList, String sql) {
        try {
            int result = 0;
            for (Manufacture manufacture : manufactureList) {
                PreparedStatement ps = connPostgres.prepareStatement(sql);
                ps.setString(13, manufacture.getId());
                ps.setString(1, manufacture.getIdDoc());
                ps.setString(2, manufacture.getDocNumber());
                ps.setString(3, manufacture.getIdTmc());
                ps.setString(4, manufacture.getIdOrder());
                ps.setInt(5, manufacture.getPosition());
                ps.setTimestamp(6, manufacture.getTimeManufacture());
                ps.setInt(7, manufacture.getQuantity());
                ps.setString(8, manufacture.getDescrSecond());
                ps.setInt(9, manufacture.getSizeA());
                ps.setInt(10, manufacture.getSizeB());
                ps.setInt(11, manufacture.getSizeC());
                ps.setString(12, manufacture.getEmbodiment());
                ps.addBatch();
                int[] numberOfUpdates = ps.executeBatch();
                result += IntStream.of(numberOfUpdates).sum();
            }
            if (result == manufactureList.size()) {
                log.debug("Try commit");
                connPostgres.commit();
                log.debug("Commit - OK. {} Manufactures for Techno saved/updated.", result);
                return true;
            }
            else {
                log.debug("Saved/Updated {}, but need to save/update {} Manufactures for Techno. Not equals!!!",
                        result, manufactureList.size());
                connPostgres.rollback();
            }
        } catch (SQLException e) {
            log.warn("Exception during save/update {} new 'Manufactures' for Techno. SQL = {}.",
                    manufactureList.size() , sql, e);
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
        return new DefaultDaoJdbc().deleteAll(listId, connPostgres, SQL_DELETE);
    }
}
