package kiyv.domain.dao;

import kiyv.domain.model.Invoice;

import java.util.Collection;
import java.util.List;

public interface InvoiceDao {

    Invoice getById(String id);

    List<Invoice> getAll();

    boolean saveAll(List<Invoice> invoiceList);

    boolean updateAll(List<Invoice> invoiceList);

    boolean deleteAll(Collection<String> listIdDoc);
}
