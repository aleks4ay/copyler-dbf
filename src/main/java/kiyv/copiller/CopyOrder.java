package kiyv.copiller;

import kiyv.domain.exception.FileNotReadedException;
import kiyv.domain.javadbf.*;
import kiyv.domain.model.*;
import kiyv.domain.dao.*;
import kiyv.domain.tools.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.sql.Connection;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static kiyv.log.ClassNameUtil.getCurrentClassName;

public class CopyOrder {
    private static final Logger log = LoggerFactory.getLogger(getCurrentClassName());

    public void run(String orderFileName, String clientFileName, String workerFileName, Map<String, Journal> mapJournal) {
        log.info("   writing 'O R D E R S'.");
        try {
            byte[] clientBytes = new File1CReader().file2byteArray(clientFileName);
            byte[] workerBytes = new File1CReader().file2byteArray(workerFileName);
            byte[] orderBytes = new File1CReader().file2byteArray(orderFileName);

            UtilDao utilDao = new UtilDao();
            Connection connPostgres = utilDao.getConnPostgres();
            OrderDao orderDao = new OrderDaoJdbc(connPostgres);
            Map<String, String> mapManagerName = new WorkerReader().getAll(workerBytes);
            Map<String, String> mapClientName = new ClientReader().getAll(clientBytes)
                    .stream()
                    .collect(Collectors.toMap(Client::getId, Client::getName));
            Map<String, Order> mapOrder = new OrderReader().getAll(orderBytes);
            List<Order> listNewOrder = new ArrayList<>();
            List<Order> listUpdatingOrder = new ArrayList<>();
            Map<String, Order> oldOrder = orderDao.getAll()
                    .stream()
                    .collect(Collectors.toMap(Order::getIdDoc, Order::getOrder));
            for (Order newOrder : mapOrder.values()) {
                String idOrder = newOrder.getIdDoc();
                if (mapJournal.containsKey(idOrder)) {
                    Journal journal = mapJournal.get(idOrder);
                    String docNumber = journal.getDocNumber();
                    Timestamp dateCreate = journal.getDateCreate();
                    Timestamp dateToFactory = newOrder.getDateToFactory();
                    Timestamp dateEnd = newOrder.getDateToShipment();
                    int duration = newOrder.getDurationTime();
                    int bigNumber = (DateConverter.getYearShort(dateCreate.getTime()) ) * 100000
                            + OrderNumber.getOrderNumberFromDocNumber(docNumber);
                    if ( dateToFactory == null) {
                        dateToFactory = dateCreate;
                    }
//                if (dateToFactory.getTime() < 1560805200000L) {
//                    continue;
//                }
                    if (dateEnd == null) {
                        Timestamp maximum = dateCreate.after(dateToFactory) ? dateCreate : dateToFactory;
                        dateEnd = new Timestamp(DateConverter.offset(maximum.getTime(), duration));
                    }
                    String idClient = newOrder.getIdClient();
                    String idManager = newOrder.getIdManager();
                    String managerName;
                    if (mapManagerName.get(idManager) == null) {
                        managerName = "";
                    }
                    else {
                        managerName = mapManagerName.get(idManager);
                    }
                    String clientName;
                    if (mapClientName.get(idClient) == null){
                        clientName = "";
                    }
                    else {
                        clientName = mapClientName.get(idClient);
                    }
                    newOrder.setBigNumber(bigNumber);
                    newOrder.setDocNumber(docNumber);
                    newOrder.setDateCreate(dateCreate);
                    newOrder.setManager(managerName);
                    newOrder.setClient(clientName);
                    newOrder.setDateToShipment(dateEnd);
                    if (!oldOrder.containsKey(idOrder)) {
                        listNewOrder.add(newOrder);
                    } else if (!oldOrder.get(idOrder).equals(newOrder)) {
                        log.info("   UPDATE Order with idDoc '{}', '{}'. Different fields: {}.",
                                newOrder.getIdDoc(),
                                newOrder.getDocNumber(),
                                newOrder.getDifferences(oldOrder.get(idOrder))
                        );
                        listUpdatingOrder.add(newOrder);
                        oldOrder.remove(idOrder);
                    }
                    else {
                        oldOrder.remove(idOrder);
                    }
                }
            }
            if (listNewOrder.size() > 0) {
                orderDao.saveAll(listNewOrder);
            }
            if (listUpdatingOrder.size() > 0) {
                orderDao.updateAll(listUpdatingOrder);
            }
            if (oldOrder.size() > 0) {
                for (Order order : oldOrder.values()) {
                    log.info("   DELETE Order with idDoc '{}', '{}'.", order.getIdDoc(), order.getDocNumber());
                }
                orderDao.deleteAll(oldOrder.keySet());
            }
            utilDao.closeConnection(connPostgres);
        } catch (FileNotReadedException e) {
            e.printStackTrace();
        }
    }
}
