package kiyv.domain.model;

import java.sql.Timestamp;

public class Journal {
    int bigNumber;
    private String idDoc;
    private String docNumber;
    private Timestamp dateCreate;

    public Journal(int bigNumber, String idDoc, String docNumber, Timestamp dateCreate) {
        this.bigNumber = bigNumber;
        this.idDoc = idDoc;
        this.docNumber = docNumber;
        this.dateCreate = dateCreate;
    }

    public int getBigNumber() {
        return bigNumber;
    }

    public void setBigNumber(int bigNumber) {
        this.bigNumber = bigNumber;
    }

    public String getIdDoc() {
        return idDoc;
    }

    public void setIdDoc(String idDoc) {
        this.idDoc = idDoc;
    }

    public String getDocNumber() {
        return docNumber;
    }

    public void setDocNumber(String docNumber) {
        this.docNumber = docNumber;
    }

    public Timestamp getDateCreate() {
        return dateCreate;
    }

    public void setDateCreate(Timestamp dateCreate) {
        this.dateCreate = dateCreate;
    }

    @Override
    public String toString() {
        return "Journal{" +
                "bigNumber=" + bigNumber +
                ", idDoc='" + idDoc + '\'' +
                ", docNumber='" + docNumber + '\'' +
                ", dateCreate=" + dateCreate +
                '}';
    }
}
