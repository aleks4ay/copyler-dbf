package kiyv.domain.dao;

import kiyv.domain.model.Description;
import kiyv.domain.model.Invoice;
import kiyv.domain.model.Manufacture;
import kiyv.domain.model.Status;
import kiyv.domain.tools.DateConverter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static kiyv.log.ClassNameUtil.getCurrentClassName;

public class StatusDaoJdbc implements StatusDao {

    private Connection connPostgres;
    private static final Logger log = LoggerFactory.getLogger(getCurrentClassName());
    private static final String SQL_INSERT_BEGIN_VALUE = "INSERT INTO statuses (id, iddoc, time_0, time_1, type_index, " +
            "status_index, is_technologichka, descr_first)  VALUES (?, ?, ?, ?, ?, ?, ?, ?);";

    private static final String SQL_UPDATE_BEGIN_VALUE = "UPDATE statuses SET descr_first=?, time_1=? WHERE id=? AND iddoc=?;";

    private static final String SQL_GET_ONE = "SELECT * FROM statuses WHERE id = ?;";
    private static final String SQL_GET_ALL = "SELECT * FROM statuses;";
    private static final String SQL_DELETE = "DELETE FROM statuses WHERE id = ?;";

    private static final String SQL_UPDATE_TIME_FROM_MANUF = "UPDATE statuses SET time_21 =? WHERE iddoc=?;";
    private static final String SQL_UPDATE_STATUS_FROM_MANUF = "UPDATE statuses SET status_index = 21 WHERE time_21 " +
            "NOTNULL and status_index < 21 AND status_index > 6 ;";

    private static final String SQL_UPDATE_TIME_FROM_INVOICE = "UPDATE statuses SET time_22 =? WHERE iddoc=?;";
    private static final String SQL_UPDATE_STATUS_FROM_INVOICE = "UPDATE statuses SET status_index = 22 WHERE " +
            "time_22 NOTNULL and status_index < 22  AND status_index > 6 ;";

    private static final String SQL_SAVE = "INSERT INTO statuses (iddoc, time_0, time_1, time_2, time_3, time_4, " +
            "time_5, time_6, time_7, time_8, time_9, time_10, time_11, time_12, time_13, time_14, time_15, time_16, " +
            "time_17, time_18, time_19, time_20, time_21, time_22, time_23, time_24, type_index, status_index, " +
            "designer_name, is_technologichka, descr_first, is_parsing, id) VALUES " +
            " (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);";

    private static final String SQL_UPDATE = "UPDATE statuses SET iddoc=?, time_0=?, time_1=?, time_2=?, time_3=?, " +
            "time_4=?, time_5=?, time_6=?, time_7=?, time_8=?, time_9=?, time_10=?, time_11=?, time_12=?, time_13=?, " +
            "time_14=?, time_15=?, time_16=?, time_17=?, time_18=?, time_19=?, time_20=?, time_21=?, time_22=?, " +
            "time_23=?, time_24=?, type_index=?, status_index=?, designer_name=?, is_technologichka=?, descr_first=?, " +
            "is_parsing=? WHERE id=?;";

    public StatusDaoJdbc(Connection conn) {
        this.connPostgres = conn;
        log.debug("Get connection to PostgreSQL from {}.", UtilDao.class);
    }


    @Override
    public Status getById(String id) {
        try {
            PreparedStatement statement = connPostgres.prepareStatement(SQL_GET_ONE);
            statement.setString(1, id);
            log.debug("Select 'Status'. SQL = {}. Id = {}.", SQL_GET_ONE, id);

            ResultSet rs = statement.executeQuery();
            rs.next();

            log.debug("return new 'Status'. Id = {}.", id);
            long[] statusTimeList = new long[25];
            for (int i = 0; i < 25; i++) {
                statusTimeList[i] = rs.getLong("time_" + i);
            }
            return new Status(id, rs.getString("iddoc"), rs.getInt("type_index"), rs.getInt("status_index"),
                    rs.getString("designer_name"), rs.getString("descr_first"), rs.getInt("is_technologichka"),
                    rs.getInt("is_parsing"), statusTimeList);

        } catch (SQLException e) {
            log.warn("Exception during reading 'Status' with Code = {}.", id, e);
        }
        log.debug("Status with Id = {} not found.", id);
        return null;
    }

    @Override
    public List<Status> getAll() {
        List<Status> result = new ArrayList<>();
        try {
            Statement statement = connPostgres.createStatement();
            ResultSet rs = statement.executeQuery(SQL_GET_ALL);
            log.debug("Select all 'Statuses'. SQL = {}.", SQL_GET_ALL);

            while (rs.next()) {
                String id = rs.getString("id");

                long[] statusTimeList = new long[25];
                for (int i = 0; i < 25; i++) {
                    statusTimeList[i] = rs.getLong("time_" + i);
                }
                Status status = new Status(id, rs.getString("iddoc"), rs.getInt("type_index"), rs.getInt("status_index"),
                        rs.getString("designer_name"), rs.getString("descr_first"), rs.getInt("is_technologichka"),
                        rs.getInt("is_parsing"), statusTimeList);

                result.add(status);
            }
            log.debug("Was read {} Statuses from Postgres.", result.size());
            return result;
        } catch (SQLException e) {
            log.warn("Exception during reading all 'Statuses'.", e);
        }
        log.debug("Statuses not found.");
        return null;
    }

    @Override
    public boolean saveBeginValues(List<Status> statusList) {
        try {
            int result = 0;
            for (Status status : statusList) {
                PreparedStatement ps = connPostgres.prepareStatement(SQL_INSERT_BEGIN_VALUE);

                log.debug("Prepared 'Status' to batch. SQL = {}. {}.", SQL_INSERT_BEGIN_VALUE, status);

                ps.setString(1, status.getId());
                ps.setString(2, status.getIdDoc());
                ps.setLong(3, status.getStatusTimeList()[0]);
                ps.setLong(4, DateConverter.getNowDate());
                ps.setInt(5, status.getTypeIndex());
                ps.setInt(6, status.getStatusIndex());
                ps.setInt(7, status.getIsTechno());
                ps.setString(8, status.getDescrFirst());

                ps.addBatch();
                int[] numberOfUpdates = ps.executeBatch();
                result += IntStream.of(numberOfUpdates).sum();
            }
            if (result == statusList.size()) {
                log.debug("Try commit");
                connPostgres.commit();
                log.debug("Commit - OK. {} Statuses saved/updated.", result);
                return true;
            }
            else {
                log.debug("Saved/Updated {}, but need to save/update {} Statuses. Not equals!!!", result, statusList.size());
                connPostgres.rollback();
            }
        } catch (SQLException e) {
            log.warn("Exception during save/update {} new 'Statuses'. SQL = {}.", statusList.size() , SQL_INSERT_BEGIN_VALUE, e);
        }
        return false;
    }

    @Override
    public boolean updateBeginValue(List<Status> statusList) {
        try {
            int result = 0;
            for (Status status : statusList) {
                PreparedStatement ps = connPostgres.prepareStatement(SQL_UPDATE_BEGIN_VALUE);

                log.debug("Prepared 'Status' to batch. SQL = {}. {}.", SQL_UPDATE_BEGIN_VALUE, status);

                ps.setString(1, status.getDescrFirst());
                ps.setLong(2, DateConverter.getNowDate());
                ps.setString(3, status.getId());
                ps.setString(4, status.getIdDoc());

                ps.addBatch();
                int[] numberOfUpdates = ps.executeBatch();
                result += IntStream.of(numberOfUpdates).sum();
            }
            if (result == statusList.size()) {
                log.debug("Try commit");
                connPostgres.commit();
                log.debug("Commit - OK. {} Statuses updated.", result);
                return true;
            }
            else {
                log.debug("Updated {}, but need to update {} Statuses. Not equals!!!", result, statusList.size());
                connPostgres.rollback();
            }
        } catch (SQLException e) {
            log.warn("Exception during update {}  'Statuses'. SQL = {}.", statusList.size() , SQL_UPDATE_BEGIN_VALUE, e);
        }
        return false;
    }

    private boolean saveOrUpdateAll(List<Status> statusList, String sql) {
        try {
            int result = 0;
            for (Status status : statusList) {
                PreparedStatement ps = connPostgres.prepareStatement(sql);

                log.debug("Prepared 'Status' to batch. SQL = {}. {}.", sql, status);

                ps.setString(33, status.getId());
                ps.setString(1, status.getIdDoc());
                for (int i=0; i<25; i++) {
                    ps.setLong(2+i, status.getStatusTimeList()[i]);
                }
                ps.setInt(27, status.getTypeIndex());
                ps.setInt(28, status.getStatusIndex());
                ps.setString(29, status.getDesigner());
                ps.setInt(30, status.getIsTechno());
                ps.setString(31, status.getDescrFirst());
                ps.setInt(32, status.getIsParsing());

                ps.addBatch();
                int[] numberOfUpdates = ps.executeBatch();
                result += IntStream.of(numberOfUpdates).sum();
            }
            if (result == statusList.size()) {
                log.debug("Try commit");
                connPostgres.commit();
                log.debug("Commit - OK. {} Statuses saved/updated.", result);
                return true;
            }
            else {
                log.debug("Saved/Updated {}, but need to save/update {} Statuses. Not equals!!!", result, statusList.size());
                connPostgres.rollback();
            }
        } catch (SQLException e) {
            log.warn("Exception during save/update {} new 'Statuses'. SQL = {}.", statusList.size() , sql, e);
        }
        return false;
    }

    @Override
    public boolean saveAll(List<Status > statusList) {
        return saveOrUpdateAll(statusList, SQL_SAVE);
    }

    @Override
    public boolean updateAll(List<Status> statusList) {
        return saveOrUpdateAll(statusList, SQL_UPDATE);
    }

    @Override
    public boolean saveFromManuf(List<Manufacture> manufactureList) {
        List<Manufacture> manufactureListAfterFilter = manufactureList
                .stream()
                .filter(m -> m.getPosition() == 1)
                .collect(Collectors.toList());
        try {
            int result = 0;

            for (Manufacture manufacture : manufactureListAfterFilter) {
                PreparedStatement ps = connPostgres.prepareStatement(SQL_UPDATE_TIME_FROM_MANUF);

                log.debug("Prepared 'Status' to batch. SQL = {}. {}.", SQL_UPDATE_TIME_FROM_MANUF, manufacture);

                ps.setLong(1, manufacture.getTime21());
                ps.setString(2, manufacture.getIdOrder());

                ps.addBatch();
                int[] numberOfUpdates = ps.executeBatch();
                result += IntStream.of(numberOfUpdates).sum();
            }

            Statement statement = connPostgres.createStatement();
            statement.executeUpdate(SQL_UPDATE_STATUS_FROM_MANUF);

            if (result >= manufactureListAfterFilter.size()) {
                log.debug("Try commit");
                connPostgres.commit();
                log.debug("Commit - OK. {} Statuses updated from Manufacture.", result);
                return true;
            }
            else {
                log.debug("Updated {}, but need to update minimum {} Statuses from Manufacture. Not equals!!!",
                        result, manufactureListAfterFilter.size());
                connPostgres.rollback();
            }
        } catch (SQLException e) {
            log.warn("Exception during update {} new 'Statuses' from Manufacture. SQL = {}.",
                    manufactureListAfterFilter.size() , SQL_UPDATE_TIME_FROM_MANUF, e);
        }
        return false;
    }

    @Override
    public boolean saveFromInvoice(List<Invoice> invoiceList) {
        try {
            int result = 0;

            for (Invoice invoice : invoiceList) {
                PreparedStatement ps = connPostgres.prepareStatement(SQL_UPDATE_TIME_FROM_INVOICE);

                log.debug("Prepared 'Status' to batch. SQL = {}. {}.", SQL_UPDATE_TIME_FROM_INVOICE, invoice);

                ps.setLong(1, invoice.getTime22());
                ps.setString(2, invoice.getIdOrder());

                ps.addBatch();
                int[] numberOfUpdates = ps.executeBatch();
                result += IntStream.of(numberOfUpdates).sum();
            }

            Statement statement = connPostgres.createStatement();
            statement.executeUpdate(SQL_UPDATE_STATUS_FROM_INVOICE);

            if (result >= invoiceList.size()) {
                log.debug("Try commit");
                connPostgres.commit();
                log.debug("Commit - OK. {} Statuses updated from Invoice.", result);
                return true;
            }
            else {
                log.debug("Updated {}, but need to update minimum {} Statuses from Invoice. Not equals!!!",
                        result, invoiceList.size());
                connPostgres.rollback();
            }
        } catch (SQLException e) {
            log.warn("Exception during update {} new 'Statuses' from Invoice. SQL = {}.",
                    invoiceList.size() , SQL_UPDATE_TIME_FROM_INVOICE, e);
        }
        return false;
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
                log.debug("Commit - OK. {} Statuses deleted.", result);
                return true;
            }
            else {
                connPostgres.rollback();
                log.debug("Deleted {}, but need to delete {} Statuses. Not equals!!!", result, listId.size());
            }
        } catch (SQLException e) {
            log.warn("Exception during delete {} old 'Statuses'. SQL = {}.", listId.size() , SQL_DELETE, e);
        }
        return false;
    }
}
