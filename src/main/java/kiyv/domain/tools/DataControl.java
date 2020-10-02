package kiyv.domain.tools;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.Properties;

public final class DataControl {

    public static String fileName;
    public static String fileName1C;
    private static final Logger log = LoggerFactory.getLogger(DataControl.class);

    static {
        //load DB properties
        try (InputStream in = DataControl.class.getClassLoader().getResourceAsStream("persistence.properties")){
            Properties properties = new Properties();
            properties.load(in);
            fileName = properties.getProperty("fileName");
            fileName1C = properties.getProperty("fileName1C");
            log.debug("Loaded properties as Stream: dbf.serverPath = {}, dbf.path = {}.", fileName, fileName1C);
        } catch (IOException e) {
            log.warn("Exception during Loaded properties from file {}.", new File("/persistence.properties").getPath(), e);
        }
    }

    // APPEND new time
    public static boolean writeTimeChange(){

        try (BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(
                new FileOutputStream(new File(fileName), true), "UTF8"))) {
            long millis = DateConverter.getNowDate();
            String date = DateConverter.dateToString(millis);
            String time = DateConverter.timeToString(millis);
            String s = date + "  " + time + " < 1C  >";
            bw.write( s + "\r\n");
            bw.flush();
        }
        catch(IOException ex){
            System.out.println(ex.getMessage());
            return false;
        }
        return true;

    }

    // REWRITE time from 1C
    public static boolean writeTimeChangeFrom1C(){

        try (BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(
                new FileOutputStream(new File(fileName1C), false), "UTF8"))) {
            long millis = DateConverter.getNowDate();
            String date = DateConverter.dateToString(millis);
            String time = DateConverter.timeToString(millis);
            String s = date + "  " + time + " < 1C  >";
            bw.write( s );
            bw.flush();
        }
        catch(IOException ex){
            System.out.println(ex.getMessage());
            return false;
        }
        return true;
    }

}

