package kiyv.domain.dao;

import kiyv.domain.model.InvoiceDescription;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.stream.IntStream;

import static kiyv.log.ClassNameUtil.getCurrentClassName;

public class InvoiceDescriptionDaoJdbc {

    private Connection connPostgres;
    private static final Logger log = LoggerFactory.getLogger(getCurrentClassName());
    private static final String SQL_GET_ALL = "SELECT * FROM invoice_descr;";
    private static final String SQL_DELETE = "DELETE FROM invoice_descr WHERE id = ?;";
    private static final String SQL_SAVE = "INSERT INTO invoice_descr (id_invoice, id_tmc, quantity, payment, id) " +
            " VALUES (?, ?, ?, ?, ?);";
    private static final String SQL_UPDATE = "UPDATE invoice_descr SET id_invoice=?, id_tmc=?, quantity=?, payment=? WHERE id = ?;";

    public InvoiceDescriptionDaoJdbc(Connection conn) {
        this.connPostgres = conn;
    }

    public List<InvoiceDescription> getAll() {
        List<InvoiceDescription> result = new ArrayList<>();
        try {
            Statement statement = connPostgres.createStatement();
            ResultSet rs = statement.executeQuery(SQL_GET_ALL);
            while (rs.next()) {
                String id = rs.getString("id");
                InvoiceDescription invoiceDescription = new InvoiceDescription(id, rs.getString("id_invoice"),
                        rs.getString("id_tmc"), rs.getInt("quantity"), rs.getDouble("payment"));
                result.add(invoiceDescription);
            }
            return result;
        } catch (SQLException e) {
            log.warn("Exception during reading all 'InvoiceDescription'.", e);
        }
        log.debug("Description not found.");
        return new ArrayList<>();
    }

    private boolean saveOrUpdateAll(List<InvoiceDescription> descriptionList, String sql) {
        try {
            int result = 0;
            for (InvoiceDescription description : descriptionList) {
                PreparedStatement ps = connPostgres.prepareStatement(sql);
                ps.setString(1, description.getIdInvoice());
                ps.setString(2, description.getIdTmc());
                ps.setInt(3, description.getQuantity());
                ps.setDouble(4, description.getPayment());
                ps.setString(5, description.getId());
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

    public boolean saveAll(List<InvoiceDescription> descriptionList) {
        return saveOrUpdateAll(descriptionList, SQL_SAVE);
    }

    public boolean updateAll(List<InvoiceDescription> descriptionList) {
        return saveOrUpdateAll(descriptionList, SQL_UPDATE);
    }

    public boolean deleteAll(Collection<String> listId) {
        return new DefaultDaoJdbc().deleteAll(listId, connPostgres, SQL_DELETE);
    }
}
