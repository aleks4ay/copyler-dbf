package kiyv.domain.model;

import java.util.Arrays;

public class Status {

    private String id;
    private String idDoc;
    private int typeIndex;
    private int statusIndex;
    private String designer = null;
    private String descrFirst;
    private int isTechno;
    private int isParsing;
    private long[] statusTimeList = new long[25];

    public Status(String id, String idDoc, int typeIndex, int statusIndex, String designer,
                  String descrFirst, int isTechno, int isParsing, long[] statusTimeList) {
        this.id = id;
        this.idDoc = idDoc;
        this.typeIndex = typeIndex;
        this.statusIndex = statusIndex;
        this.designer = designer;
        this.descrFirst = descrFirst;
        this.isTechno = isTechno;
        this.isParsing = isParsing;
        this.statusTimeList = statusTimeList;
    }

    public long[] getStatusTimeList() {
        return statusTimeList;
    }

    public void setStatusTimeList(long[] statusTimeList) {
        this.statusTimeList = statusTimeList;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getIdDoc() {
        return idDoc;
    }

    public void setIdDoc(String idDoc) {
        this.idDoc = idDoc;
    }

    public int getTypeIndex() {
        return typeIndex;
    }

    public void setTypeIndex(int typeIndex) {
        this.typeIndex = typeIndex;
    }

    public int getStatusIndex() {
        return statusIndex;
    }

    public void setStatusIndex(int statusIndex) {
        this.statusIndex = statusIndex;
    }

    public String getDesigner() {
        return designer;
    }

    public void setDesigner(String designer) {
        this.designer = designer;
    }

    public String getDescrFirst() {
        return descrFirst;
    }

    public void setDescrFirst(String descrFirst) {
        this.descrFirst = descrFirst;
    }

    public int getIsTechno() {
        return isTechno;
    }

    public void setIsTechno(int isTechno) {
        this.isTechno = isTechno;
    }

    public int getIsParsing() {
        return isParsing;
    }

    public void setIsParsing(int isParsing) {
        this.isParsing = isParsing;
    }

    public Status getStatus() {
        return this;
    }


    @Override
    public String toString() {
        return "Status{" +
                "id='" + id + '\'' +
                ", idDoc='" + idDoc + '\'' +
                ", typeIndex=" + typeIndex +
                ", statusIndex=" + statusIndex +
                ", designer='" + designer + '\'' +
                ", descrFirst='" + descrFirst + '\'' +
                ", isTechno=" + isTechno +
                ", isParsing=" + isParsing +
                ", statusTimeList=" + Arrays.toString(statusTimeList) +
                '}';
    }
}
