package kiyv.domain.javadbf;

import com.linuxense.javadbf.DBFException;
import com.linuxense.javadbf.DBFReader;
import com.linuxense.javadbf.DBFRow;
import com.linuxense.javadbf.DBFUtils;
import kiyv.domain.model.Client;
import kiyv.domain.tools.File1CReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;

public class ClientReader {
    private static final Logger log = LoggerFactory.getLogger(ClientReader.class);

    public static void main(String[] args) {
        String fileName = "c:/1C/Copy250106/SC172.DBF";
        new ClientReader()
                .getAll(File1CReader.file2byteArray(fileName))
                .forEach(System.out::println);
    }

    public List<Client> getAll(byte[] dataByteArray) {
        List<Client> listClient = new ArrayList<>();
        if (dataByteArray.length == 0) {
            return listClient;
        }
        DBFReader reader = null;
        try {
            InputStream is = new ByteArrayInputStream(dataByteArray);
            reader = new DBFReader(is);
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
            log.warn("Exception during writing all 'Client'.", e);
        }
        finally {
            DBFUtils.close(reader);
        }
        log.debug("Clients not found.");
        return null;
    }
}
