package kiyv.domain.dao;

import kiyv.domain.model.Invoice;
import kiyv.domain.model.Manufacture;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import kiyv.domain.model.Order;

import java.sql.*;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static kiyv.log.ClassNameUtil.getCurrentClassName;

public class OrderDaoJdbc implements OrderDao {

    private Connection connPostgres;
    private static final Logger log = LoggerFactory.getLogger(getCurrentClassName());
    private static final String SQL_GET_ONE = "SELECT * FROM orders WHERE big_number = ?;";
    private static final String SQL_GET_ALL = "SELECT * FROM orders;";
    private static final String SQL_GET_ALL_ID = "SELECT iddoc FROM orders;";
    private static final String SQL_GET_ALL_DATE_FACTORY = "SELECT iddoc, t_factory FROM orders;";
    private static final String SQL_DELETE = "DELETE FROM orders WHERE iddoc = ?;";

    private static final String SQL_UPDATE_TIME_MANUF = "UPDATE orders SET time_manuf =?, docno_manuf =? WHERE iddoc =? ";
//            + "AND time_manuf ISNULL;";
    private static final String SQL_UPDATE_PAYMENT = "UPDATE orders SET time_invoice=?, payment =?, docno_invoice =? WHERE iddoc =? ";
    private static final String SQL_UPDATE_TIME_INVOICE = "UPDATE orders SET time22 = EXTRACT(EPOCH FROM time_invoice)*1000 " +
            "WHERE payment >= price - ? AND time22 = 0;";

    private static final String SQL_SAVE = "INSERT INTO orders(big_number, id_client, id_manager, duration, docno, " +
            "docno_manuf, docno_invoice, pos_count, client_name, manager_name, t_create, t_factory, t_end, time22, " +
            "price, payment, time_manuf, time_invoice, iddoc) VALUES " +
            "(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);";

    private static final String SQL_UPDATE = "UPDATE orders SET big_number=?, id_client=?, id_manager=?, duration=?, docno=?, " +
            "docno_manuf=?, docno_invoice=?, pos_count=?, client_name=?, manager_name=?, t_create=?, t_factory=?, t_end=?, " +
            "time22=?, price=?, payment=?, time_manuf=?, time_invoice=? WHERE iddoc = ?;";

    public OrderDaoJdbc(Connection conn) {
        this.connPostgres = conn;
        log.debug("Get connection to PostgreSQL from {}.", UtilDao.class);
    }

    @Override
    public Order getByBigNumber(int bigNumber) {
        try {
            PreparedStatement statement = connPostgres.prepareStatement(SQL_GET_ONE);
            statement.setInt(1, bigNumber);
            log.debug("Select 'Order'. SQL = {}. bigNumber = {}.", SQL_GET_ONE, bigNumber);

            ResultSet rs = statement.executeQuery();
            rs.next();

            log.debug("return new 'Order'. bigNumber = {}.", bigNumber);
            return new Order(bigNumber, rs.getString("iddoc"), rs.getString("id_client"), rs.getString("id_manager"),
                    rs.getInt("duration"), rs.getString("docno"), rs.getInt("pos_count"), rs.getString("client_name"),
                    rs.getString("manager_name"), rs.getTimestamp("t_create"), rs.getTimestamp("t_factory"),
                    rs.getTimestamp("t_end"), rs.getDouble("price"));

        } catch (SQLException e) {
            log.warn("Exception during reading 'Order' with bigNumber = {}.", bigNumber, e);
        }
        log.debug("Order with bigNumber = {} not found.", bigNumber);
        return null;
    }

    @Override
    public List<Order> getAll() {
        List<Order> result = new ArrayList<>();
        try {
            Statement statement = connPostgres.createStatement();
            ResultSet rs = statement.executeQuery(SQL_GET_ALL);
            log.debug("Select all 'Orders'. SQL = {}.", SQL_GET_ALL);

            while (rs.next()) {
                int bigNumber = rs.getInt("big_number");
//                log.debug("return new 'Order'. bigNumber = {}.", bigNumber);

                Order order = new Order(bigNumber, rs.getString("iddoc"), rs.getString("id_client"), rs.getString("id_manager"),
                        rs.getInt("duration"), rs.getString("docno"), rs.getInt("pos_count"), rs.getString("client_name"),
                        rs.getString("manager_name"), rs.getTimestamp("t_create"), rs.getTimestamp("t_factory"),
                        rs.getTimestamp("t_end"), rs.getDouble("price"));

                result.add(order);
            }
            log.debug("Was read {} Orders from Postgres.", result.size());
            return result;
        } catch (SQLException e) {
            log.warn("Exception during reading all 'Order'.", e);
        }
        log.debug("Orders not found.");
        return null;
    }

    @Override
    public List<String> getAllId() {
        List<String> result = new ArrayList<>();
        try {
            Statement statement = connPostgres.createStatement();
            ResultSet rs = statement.executeQuery(SQL_GET_ALL_ID);
            log.debug("Select all 'IdDoc'. SQL = {}.", SQL_GET_ALL_ID);

            while (rs.next()) {
                 result.add(rs.getString("iddoc"));
            }
            log.debug("Was read {} IdDoc Orders.", result.size());
            return result;
        } catch (SQLException e) {
            log.warn("Exception during reading all 'Order IdDoc'.", e);
        }
        log.debug("Order's IdDoc not found.");
        return null;
    }

    @Override
    public Map<String, Timestamp> getAllDateToFactory() {
        Map<String, Timestamp> result = new HashMap<>();
        try {
            Statement statement = connPostgres.createStatement();
            ResultSet rs = statement.executeQuery(SQL_GET_ALL_DATE_FACTORY);
            log.debug("Select all 'dateToFactory'. SQL = {}.", SQL_GET_ALL_DATE_FACTORY);

            while (rs.next()) {
                result.put(rs.getString("iddoc"), rs.getTimestamp("t_factory"));
            }
            log.debug("Was read {} dateToFactory.", result.size());
            return result;
        } catch (SQLException e) {
            log.warn("Exception during reading all 'Order dateToFactory'.", e);
        }
        log.debug("Order's dateToFactory not found.");
        return null;
    }

    private boolean saveOrUpdateAll(List<Order> orderList, String sql) {
        try {
            int result = 0;
            for (Order order : orderList) {
                PreparedStatement ps = connPostgres.prepareStatement(sql);

                log.debug("Prepared 'Order' to batch. SQL = {}. {}.", sql, order);

                ps.setString(19, order.getIdDoc());
                ps.setInt(1, order.getBigNumber());
                ps.setString(2, order.getIdClient());
                ps.setString(3, order.getIdManager());
                ps.setInt(4, order.getDurationTime());
                ps.setString(5, order.getDocNumber());
                ps.setString(6, order.getDocNumberManuf());
                ps.setString(7, order.getDocNumberInvoice());
                ps.setInt(8, order.getPosCount());
                ps.setString(9, order.getClient());
                ps.setString(10, order.getManager());
                ps.setTimestamp(11, order.getDateCreate());
                ps.setTimestamp(12, order.getDateToFactory());
                ps.setTimestamp(13, order.getDateToShipment());
                if (order.getTime22() != null) {
                    ps.setLong(14, order.getTime22());
                } else {
                    ps.setLong(14, 0L);
                }
                ps.setDouble(15, order.getPrice());
                ps.setDouble(16, order.getPayment());
                ps.setTimestamp(17, order.getTimeManuf());
                ps.setTimestamp(18, order.getTimeInvoice());

                ps.addBatch();
                int[] numberOfUpdates = ps.executeBatch();
                result += IntStream.of(numberOfUpdates).sum();
            }
            if (result == orderList.size()) {
                log.debug("Try commit");
                connPostgres.commit();
                log.debug("Commit - OK. {} Orders saved/updated.", result);
                return true;
            }
            else {
                log.debug("Saved/Updated {}, but need to save/update {} Order. Not equals!!!", result, orderList.size());
                connPostgres.rollback();
            }
        } catch (SQLException e) {
            log.warn("Exception during save/update {} new 'Order'. SQL = {}.", orderList.size() , sql, e);
        }
        return false;
    }

    @Override
    public boolean saveAll(List<Order> orderList) {
        return saveOrUpdateAll(orderList, SQL_SAVE);
    }

    @Override
    public boolean updateAll(List<Order> orderList) {
        return saveOrUpdateAll(orderList, SQL_UPDATE);
    }

    @Override
    public boolean saveFromManuf(List<Manufacture> manufactureList) {
        List<Manufacture> manufactureListAfterFilter = manufactureList
                .stream()
                .filter(m -> m.getPosition() == 1)
                .collect(Collectors.toList());
        try {
            int result = 0;
            for (Manufacture manuf : manufactureListAfterFilter) {
                PreparedStatement ps = connPostgres.prepareStatement(SQL_UPDATE_TIME_MANUF);

                log.debug("Prepared 'Order' to batch. SQL = {}. {}.", SQL_UPDATE_TIME_MANUF, manuf);

                ps.setTimestamp(1, manuf.getTimeManufacture());
                ps.setString(2, manuf.getDocNumber());
                ps.setString(3, manuf.getIdOrder());

                ps.addBatch();
                int[] numberOfUpdates = ps.executeBatch();
                result += IntStream.of(numberOfUpdates).sum();
            }
            if (result == manufactureListAfterFilter.size()) {
                log.debug("Try commit");
                connPostgres.commit();
                log.debug("Commit - OK. {} Orders saved from Manufacture.", result);
                return true;
            }
            else {
                log.debug("Updated {}, but need to update {} Order from Manufacture. Not equals!!!",
                        result, manufactureListAfterFilter.size());
                connPostgres.rollback();
            }
        } catch (SQLException e) {
            log.warn("Exception during update {} new 'Order' from Manufacture. SQL = {}.",
                    manufactureListAfterFilter.size() , SQL_UPDATE_TIME_MANUF, e);
        }
        return false;
    }

    @Override
    public boolean savePraceFromInvoice(List<Invoice> invoicesAfterFilter, double precision) {
        try {
            int result = 0;
            for (Invoice invoice : invoicesAfterFilter) {
                PreparedStatement ps = connPostgres.prepareStatement(SQL_UPDATE_PAYMENT);

                log.debug("Prepared 'Order' to batch. SQL = {}. {}.", SQL_UPDATE_PAYMENT, invoice);

                ps.setTimestamp(1, invoice.getTimeInvoice());
                ps.setDouble(2, invoice.getPrice());
                ps.setString(3, invoice.getDocNumber());
                ps.setString(4, invoice.getIdOrder());

                ps.addBatch();
                int[] numberOfUpdates = ps.executeBatch();
                result += IntStream.of(numberOfUpdates).sum();
            }

            PreparedStatement ps2 = connPostgres.prepareStatement(SQL_UPDATE_TIME_INVOICE);
            ps2.setDouble(1, precision);
            ps2.executeUpdate();
            ps2.addBatch();

            if (result == invoicesAfterFilter.size()) {
                log.debug("Try commit");
                connPostgres.commit();
                log.debug("Commit - OK. {} Orders saved from Invoice.", result);
                return true;
            }
            else {
                log.debug("Updated {}, but need to update {} Order from Invoice. Not equals!!!",
                        result, invoicesAfterFilter.size());
                connPostgres.rollback();
            }
        } catch (SQLException e) {
            log.warn("Exception during update {} new 'Order' from Invoice. SQL = {}.",
                    invoicesAfterFilter.size() , SQL_UPDATE_PAYMENT, e);
        }
        return false;
    }

    @Override
    public boolean deleteAll(Collection<String> listIdDoc) {
        try {
            int result = 0;
            for (String idDoc : listIdDoc) {
                PreparedStatement ps = connPostgres.prepareStatement(SQL_DELETE);
                ps.setString(1, idDoc);
                ps.addBatch();
                int[] numberOfUpdates = ps.executeBatch();
                result += IntStream.of(numberOfUpdates).sum();
            }
            if (result == listIdDoc.size()) {
                log.debug("Try commit");
                connPostgres.commit();
                log.debug("Commit - OK. {} Order deleted.", result);
                return true;
            }
            else {
                connPostgres.rollback();
                log.debug("Deleted {}, but need to delete {} Order. Not equals!!!", result, listIdDoc.size());
            }
        } catch (SQLException e) {
            log.warn("Exception during delete {} old 'Order'. SQL = {}.", listIdDoc.size() , SQL_DELETE, e);
        }
        return false;
    }
}
