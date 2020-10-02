package kiyv.domain.dao;

import kiyv.domain.model.Invoice;
import kiyv.domain.model.Manufacture;
import kiyv.domain.model.Status;

import java.util.Collection;
import java.util.List;

public interface StatusDao {

    Status getById(String kod);

    List<Status> getAll();

    boolean updateBeginValue(List<Status> statusList);

    boolean saveAll(List<Status> statusList);

    boolean updateAll(List<Status> statusList);

    boolean saveBeginValues(List<Status> statusList);

    boolean saveFromManuf(List<Manufacture> manufactureList);

    boolean saveFromInvoice(List<Invoice> invoiceList);

    boolean deleteAll(Collection<String> listId);
}
