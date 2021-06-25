package kiyv.domain.tools;

import kiyv.domain.exception.FileNotReadedException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.*;
import java.util.concurrent.TimeUnit;

public class File1CReader {
    private static final Logger log = LoggerFactory.getLogger(File1CReader.class);

    public byte[] file2byteArray(String fileName) throws FileNotReadedException {
        File file = new File(fileName);
        try {
            while (true) {
                InputStream fis = new FileInputStream(file);
                try {
                    byte[] fileInArray = new byte[(int) file.length()];
                    fis.read(fileInArray);
                    fis.close();
                    return fileInArray;
                } catch (IOException e) {
                    log.warn("Exception during copy '{}'.", fileName);
                    TimeUnit.SECONDS.sleep(4);
                } finally {
                    fis.close();
                }
            }
        } catch (IOException | InterruptedException e) {
            e.printStackTrace();
        }
        throw new FileNotReadedException();
    }
}
