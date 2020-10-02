package kiyv.run;

import com.linuxense.javadbf.DBFException;
import com.linuxense.javadbf.DBFReader;
import com.linuxense.javadbf.DBFRow;
import com.linuxense.javadbf.DBFUtils;
import kiyv.domain.model.Journal;
import kiyv.domain.model.Order;
import kiyv.domain.tools.DateConverter;
import kiyv.domain.tools.TimeConverter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileInputStream;
import java.io.IOException;
import java.sql.Timestamp;
import java.util.*;

import static kiyv.log.ClassNameUtil.getCurrentClassName;

public class TestNewOrderToFactory {
    private static final Logger log = LoggerFactory.getLogger(getCurrentClassName());

    public static void main(String[] args) {
        doCopyNewRecord();
    }


    public static void doCopyNewRecord() {

        Map<String, Journal> mapJournal = getAllJournal();
        Map<String, Order> mapOrder = getAll(mapJournal);

        for (Order newOrder : mapOrder.values()) {
            String idOrder = newOrder.getIdDoc();

            if (mapJournal.containsKey(idOrder)) {

                Journal journal = mapJournal.get(idOrder);
                String docNumber = journal.getDocNumber();

                Timestamp dateCreate = journal.getDateCreate();
                Timestamp dateToFactory = newOrder.getDateToFactory();
                Timestamp dateEnd = newOrder.getDateToShipment();
                int duration = newOrder.getDurationTime();
//                int bigNumber = (DateConverter.getYearShort(dateCreate.getTime()) ) * 100000 + Integer.valueOf(docNumber.substring(5));

                if ( dateToFactory == null) {
                    dateToFactory = dateCreate;
                }
                if (dateToFactory.getTime() < 1596114060000L) {
                    continue;
                }
                if (dateEnd == null) {
                    Timestamp maximum = dateCreate.after(dateToFactory) ? dateCreate : dateToFactory;
                    dateEnd = new Timestamp(DateConverter.offset(maximum.getTime(), duration));
                }

//                newOrder.setBigNumber(bigNumber);
                newOrder.setDocNumber(docNumber);
                newOrder.setDateCreate(dateCreate);
                newOrder.setDateToShipment(dateEnd);

                System.out.println(newOrder.getBigNumber() + ", " + newOrder.getDocNumber() + ", " + newOrder.getIdDoc() + ", " + newOrder.getDateToFactory()+ ", " + newOrder.getDateCreate());
            }
        }
    }

    public static Map<String, Journal> getAllJournal() {
        Map<String, Journal> mapJournal = new HashMap<>();
        DBFReader reader = null;
        try {
            reader = new DBFReader(new FileInputStream("D:\\KiyV management2\\DB_copy\\1SJOURN.DBF"));

            DBFRow row;
            while ((row = reader.nextRow()) != null) {
                Date keyOrderYear = row.getDate("DATE");
                int keyOrderIsEnable = row.getInt("CLOSED");
                if (keyOrderYear.getTime() < 1596114060000L || keyOrderIsEnable ==4 ) {
                    continue;
                }

                String idDoc = row.getString("IDDOC");
                String docNumber = new String(row.getString("DOCNO").getBytes("ISO-8859-15"), "Windows-1251");
                long dateCreate = row.getDate("DATE").getTime();
                long timeCreate = TimeConverter.convertStrToTimeMillisecond(row.getString("TIME"));

                Journal journal = new Journal(0, idDoc, docNumber, new Timestamp(dateCreate + timeCreate));

                mapJournal.put(idDoc, journal);

            }
            log.debug("Was read {} Journal from 1C '1SJOURN'.", mapJournal.size());
            return mapJournal;
        } catch (DBFException | IOException e) {
            log.warn("Exception during reading file '1SJOURN.dbf'.", e);
        } catch (Exception e) {
            log.warn("Exception during writing all 'Journal'.", e);
        }
        finally {
            DBFUtils.close(reader);
        }

        log.debug("Journal not found.");
        return null;
    }

    public static Map<String, Order> getAll(Map<String, Journal> journalMap) {

        Map<String, Order> mapOrder = new HashMap<>();

        DBFReader reader = null;
        try {
            reader = new DBFReader(new FileInputStream("D:\\KiyV management2\\DB_copy\\DH1898.DBF"));

            DBFRow row;
            while ((row = reader.nextRow()) != null) {
                int keyOrderToFactory = row.getInt("SP14694");
                if (keyOrderToFactory == 1) {
//                    System.out.println();
                    continue;
                }
                String idDoc = row.getString("IDDOC");

                if (!journalMap.containsKey(idDoc)) {
                    continue;
                }
                String idClient = row.getString("SP1899");

                int durationTime = row.getInt("SP14695");
                String idManager = row.getString("SP14680");
                double price = row.getDouble("SP14684");

                Order order = new Order(keyOrderToFactory, idDoc, idClient, idManager, durationTime, null, 0, null, null,
                        null, null, null, price);

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
