package kiyv.run;

import kiyv.copiller.*;
import kiyv.domain.javadbf.JournalReader;
import kiyv.domain.model.Journal;
import kiyv.domain.tools.DataControl;
import kiyv.domain.tools.File1CReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.sql.SQLException;
import java.util.Map;
import java.util.Properties;

public class CopyNewData1C {
    private static String serverPath = null;
    private static String fNameClient;
    private static String fNameWorker;
    private static String fNameJournal;
    private static String fNameTmc;
    private static String fNameEmbodiment;
    private static String fNameOrder;
    private static String fNameDescr;
    private static String fNameManuf;
    private static String fNameInvoice;
    private static String fNameInvoiceDescr;
    private static String fNameTmcBalance;
    private static final Logger log = LoggerFactory.getLogger(CopyNewData1C.class);

    static {
        //load DB properties
        try (InputStream in = CopyNewData1C.class.getClassLoader().getResourceAsStream("persistence.properties")){
            Properties properties = new Properties();
            properties.load(in);
            serverPath = properties.getProperty("dbf.serverPath");
            fNameClient = serverPath + properties.getProperty("fNameClient");
            fNameWorker = serverPath + properties.getProperty("fNameWorker");
            fNameJournal = serverPath + properties.getProperty("fNameJournal");
            fNameTmc = serverPath + properties.getProperty("fNameTmc");
            fNameEmbodiment = serverPath + properties.getProperty("fNameEmbodiment");
            fNameOrder = serverPath + properties.getProperty("fNameOrder");
            fNameDescr = serverPath + properties.getProperty("fNameDescr");
            fNameManuf = serverPath + properties.getProperty("fNameManuf");
            fNameInvoice = serverPath + properties.getProperty("fNameInvoice");
            fNameInvoiceDescr = serverPath + properties.getProperty("fNameInvoiceDescr");
            fNameTmcBalance = serverPath + properties.getProperty("fNameTmcBalance");
        } catch (IOException e) {
            log.warn("Exception during Loaded properties from file {}.", new File("/persistence.properties").getPath(), e);
        }
    }

    public static void main(String[] args) throws ClassNotFoundException, SQLException, IOException {
        update();
    }

    public static void update() throws SQLException, IOException, ClassNotFoundException {
        long maxTimeOld = 0L;
        while (true) {
            long t1 = System.currentTimeMillis();
            long maxTime= getLatestTime();
            if (maxTime > maxTimeOld) {
                log.info("* * * * Start copy files from 1C * * * *");
                maxTimeOld = maxTime;
                byte[] bytesJournal = File1CReader.file2byteArray(fNameJournal);
                Map<String, Journal> journalMap = new JournalReader().getAllJournal(bytesJournal);
                new CopyTmc().run(fNameTmc);
                new CopyTmcTechno().run();
                new CopyOrder().run(fNameOrder, fNameClient, fNameWorker, journalMap);
                new CopyDescription().run(fNameDescr, fNameEmbodiment);
                new CopyManuf().run(fNameManuf, journalMap);
                new CopyManufTechno().run(fNameManuf, journalMap);
                new CopyInvoice().run(fNameInvoice, journalMap);
                new CopyInvoiceDescription().run(fNameInvoiceDescr);
                new CopyTmcBalance().run(fNameTmcBalance);
                DataControl.writeTimeChange();
                DataControl.writeTimeChangeFrom1C();
                long t2 = System.currentTimeMillis();
                log.info("Total time for copy = {} c.", (double) ((t2 - t1) / 1000));
            } else {
                log.info("* * * * Not new Orders or new changes * * * *");
            }
            try {
                Thread.sleep(15 * 60 * 1000); // sleep 15 min
            } catch (InterruptedException e) {
                log.warn("Exception during sleep 15 min.", e);
            }
        }
    }

    private static long getFileLastModifiedTime(String filePath) {
        File file = new File(filePath);
        return file.lastModified();
    }

    private static long getLatestTime() {
        long[] time = new long[11];
        time[0] = getFileLastModifiedTime(fNameTmc);
        time[1] = getFileLastModifiedTime(fNameTmcBalance);
        time[2] = getFileLastModifiedTime(fNameClient);
        time[3] = getFileLastModifiedTime(fNameWorker);
        time[4] = getFileLastModifiedTime(fNameJournal);
        time[5] = getFileLastModifiedTime(fNameOrder);
        time[6] = getFileLastModifiedTime(fNameEmbodiment);
        time[7] = getFileLastModifiedTime(fNameDescr);
        time[8] = getFileLastModifiedTime(fNameManuf);
        time[9] = getFileLastModifiedTime(fNameInvoice);
        time[10] = getFileLastModifiedTime(fNameInvoiceDescr);
        long result = 0L;
        for (int i = 0; i<10; i++) {
            if (result < Math.max(time[i], time[i+1])) {
                result = Math.max(time[i], time[i + 1]);
            }
        }
        return result;
    }
}
