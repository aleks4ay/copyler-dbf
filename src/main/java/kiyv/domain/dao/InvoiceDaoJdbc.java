package kiyv.domain.dao;

import kiyv.domain.model.Invoice;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.stream.IntStream;

import static kiyv.log.ClassNameUtil.getCurrentClassName;

public class InvoiceDaoJdbc implements InvoiceDao {

    private Connection connPostgres;
    private static final Logger log = LoggerFactory.getLogger(getCurrentClassName());
    private static final String SQL_GET_ONE = "SELECT * FROM invoice WHERE iddoc = ?;";
    private static final String SQL_GET_ALL = "SELECT * FROM invoice;";
    private static final String SQL_DELETE = "DELETE FROM invoice WHERE iddoc = ?;";

    private static final String SQL_SAVE = "INSERT INTO invoice (docno, id_order, time_invoice, time22, price, iddoc)" +
            " VALUES (?, ?, ?, ?, ?, ?);";

    private static final String SQL_UPDATE = "UPDATE invoice SET docno=?, id_order=?, time_invoice=?, time22=?," +
            " price=? WHERE iddoc=?;";

    public InvoiceDaoJdbc(Connection conn) {
        this.connPostgres = conn;
        log.debug("Get connection to PostgreSQL from {}.", UtilDao.class);
    }

    @Override
    public Invoice getById(String id) {
        try {
            PreparedStatement statement = connPostgres.prepareStatement(SQL_GET_ONE);
            statement.setString(1, id);
            log.debug("Select 'Invoice'. SQL = {}. Id = {}.", SQL_GET_ONE, id);

            ResultSet rs = statement.executeQuery();
            rs.next();

//            log.debug("return new 'Invoice'. Id = {}.", id);

            return new Invoice(id, rs.getString("docno"), rs.getString("id_order"), rs.getTimestamp("time_invoice"),
                    rs.getLong("time22"), rs.getDouble("price"));

        } catch (SQLException e) {
            log.warn("Exception during reading 'Invoice' with Id = {}.", id, e);
        }
        log.debug("Invoice with Id = {} not found.", id);
        return null;
    }

    @Override
    public List<Invoice> getAll() {
        List<Invoice> result = new ArrayList<>();
        try {
            Statement statement = connPostgres.createStatement();
            ResultSet rs = statement.executeQuery(SQL_GET_ALL);
            log.debug("Select all 'Invoice'. SQL = {}.", SQL_GET_ALL);

            while (rs.next()) {
                String id = rs.getString("iddoc");
//                log.debug("return new 'Invoice'. Id = {}.", id);

                Invoice invoice = new Invoice(id, rs.getString("docno"), rs.getString("id_order"), rs.getTimestamp("time_invoice"),
                        rs.getLong("time22"), rs.getDouble("price"));
                result.add(invoice);
            }
            log.debug("Was read {} Invoices from Postgres.", result.size());
            return result;
        } catch (SQLException e) {
            log.warn("Exception during reading all 'Invoices'.", e);
        }
        log.debug("Invoices not found.");
        return null;
    }

    private boolean saveOrUpdateAll(List<Invoice> invoiceList, String sql) {
        try {
            int result = 0;
            for (Invoice invoice : invoiceList) {
                PreparedStatement ps = connPostgres.prepareStatement(sql); //docno, id_order, time_invoice, time22, price, iddoc

                log.debug("Prepared 'Invoices' to batch. SQL = {}. {}.", sql, invoice);

                ps.setString(6, invoice.getIdDoc());
                ps.setString(1, invoice.getDocNumber());
                ps.setString(2, invoice.getIdOrder());
                ps.setTimestamp(3, invoice.getTimeInvoice());
                ps.setLong(4, invoice.getTime22());
                ps.setDouble(5, invoice.getPrice());

                if (invoice.getTime22() != null) {
                    ps.setLong(4, invoice.getTime22());
                } else {
                    ps.setLong(4, 0L);
                }

                ps.addBatch();
                int[] numberOfUpdates = ps.executeBatch();
                result += IntStream.of(numberOfUpdates).sum();
            }
            if (result == invoiceList.size()) {
                log.debug("Try commit");
                connPostgres.commit();
                log.debug("Commit - OK. {} Invoices saved/updated.", result);
                return true;
            }
            else {
                log.debug("Saved/Updated {}, but need to save/update {} Invoices. Not equals!!!", result, invoiceList.size());
                connPostgres.rollback();
            }
        } catch (SQLException e) {
            log.warn("Exception during save/update {} new 'Invoices'. SQL = {}.", invoiceList.size() , sql, e);
        }
        return false;
    }

    @Override
    public boolean saveAll(List<Invoice> invoiceList) {
        return saveOrUpdateAll(invoiceList, SQL_SAVE);
    }

    @Override
    public boolean updateAll(List<Invoice> invoiceList) {
        return saveOrUpdateAll(invoiceList, SQL_UPDATE);
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
                log.debug("Commit - OK. {} Invoices deleted.", result);
                return true;
            }
            else {
                connPostgres.rollback();
                log.debug("Deleted {}, but need to delete {} Invoices. Not equals!!!", result, listId.size());
            }
        } catch (SQLException e) {
            log.warn("Exception during delete {} old 'Invoices'. SQL = {}.", listId.size() , SQL_DELETE, e);
        }
        return false;
    }

}
