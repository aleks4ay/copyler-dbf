package kiyv.domain.javadbf;

import java.io.*;
import com.linuxense.javadbf.*;
import kiyv.domain.model.Order;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Date;
import java.sql.*;
import java.util.HashMap;
import java.util.Map;

import static kiyv.log.ClassNameUtil.getCurrentClassName;

public class OrderReader  {
    private static final Logger log = LoggerFactory.getLogger(getCurrentClassName());

    public static void main(String[] args) {
        Map<String, Order> orderMap = new OrderReader().getAll();
        for (Order c : orderMap.values()) {
            System.out.println(c.getIdDoc() + ", " + c.getIdClient() + ", " + c.getDateToFactory() + ", " +
                    c.getDurationTime() + ", " + c.getIdManager() + ", " + c.getPrice());
        }
        System.out.println(orderMap.size());
    }

    public Map<String, Order> getAll() {

        Map<String, Order> mapOrder = new HashMap<>();

        DBFReader reader = null;
        try {
            reader = new DBFReader(new FileInputStream("D:\\KiyV management2\\DB_copy\\DH1898.DBF"));

            DBFRow row;
            while ((row = reader.nextRow()) != null) {
                int keyOrderToFactory = row.getInt("SP14694");
                if (keyOrderToFactory != 1) {
                    continue;
                }
                String idDoc = row.getString("IDDOC");
                String idClient = row.getString("SP1899");
                Date date = row.getDate("SP14836");
                Timestamp dateToFactory;
                if (date == null) {
                    dateToFactory = null;
                }
                else if (date.getTime() < 1560805200000L) {
                    continue;
                }
                else {
                    dateToFactory = new Timestamp(date.getTime());
                }
                int durationTime = row.getInt("SP14695");
                String idManager = row.getString("SP14680");
                double price = row.getDouble("SP14684");

                Order order = new Order(0, idDoc, idClient, idManager, durationTime, null, 0, null, null,
                        null, dateToFactory, null, price);

                mapOrder.put(idDoc, order);
            }
            log.debug("Was read {} orders from 1C 'DH1898'.", mapOrder.size());
            return mapOrder;
        } catch (DBFException | IOException e) {
            log.warn("Exception during reading file 'DH1898.dbf'.", e);
        } catch (Exception e) {
            log.warn("Exception during writing all 'Orders'.", e);
        }
        finally {
            DBFUtils.close(reader);
        }

        log.debug("Orders not found.");
        return null;
    }
}
