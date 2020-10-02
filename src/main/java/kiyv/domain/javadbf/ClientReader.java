package kiyv.domain.javadbf;

import java.io.*;
import com.linuxense.javadbf.*;
import kiyv.domain.model.Client;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

import static kiyv.log.ClassNameUtil.getCurrentClassName;

public class ClientReader {

    private static final Logger log = LoggerFactory.getLogger(getCurrentClassName());

    public static void main(String[] args) {
        List<Client> clients = new ClientReader().getAll();
        for (Client c : clients) {
            System.out.println(c.getId() + ", " + c.getName());
        }
        System.out.println(clients.size());
    }

    public List<Client> getAll() {

        List<Client> listClient = new ArrayList<>();

        DBFReader reader = null;
        try {
            reader = new DBFReader(new FileInputStream("D:\\KiyV management2\\DB_copy\\SC172.DBF"));

            DBFRow row;

            while ((row = reader.nextRow()) != null) {
                String id = row.getString("ID");
                String name = new String(row.getString("DESCR").getBytes("ISO-8859-1"), "Windows-1251");
                if (name.equals("")) {
                    name = "-";
                }
                listClient.add(new Client(id, name));
            }

            log.debug("Was read {} Clients from 1S.", listClient.size());
            return listClient;
        } catch (DBFException | IOException e) {
            log.warn("Exception during reading file 'Client'.", e);
        } catch (Exception e) {
            log.warn("Exception during writing all 'Client'.", e);
        }
        finally {
            DBFUtils.close(reader);
        }

        log.debug("Clients not found.");
        return null;
    }
}
