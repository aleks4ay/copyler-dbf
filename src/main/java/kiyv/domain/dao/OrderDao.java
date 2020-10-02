package kiyv.domain.dao;

import kiyv.domain.model.Invoice;
import kiyv.domain.model.Manufacture;
import kiyv.domain.model.Order;

import java.sql.Timestamp;
import java.util.Collection;
import java.util.List;
import java.util.Map;

public interface OrderDao {

    Order getByBigNumber(int bigNumber);

    List<Order> getAll();

    List<String> getAllId();

    Map<String,Timestamp> getAllDateToFactory();

    boolean saveAll(List<Order> orderList);

    boolean updateAll(List<Order> orderList);

    boolean saveFromManuf(List<Manufacture> manufactureList);

    boolean deleteAll(Collection<String> listIdDoc);

    boolean savePraceFromInvoice(List<Invoice> listInvoice, double precision);
}
