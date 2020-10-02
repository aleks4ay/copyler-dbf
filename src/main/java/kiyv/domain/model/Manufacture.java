package kiyv.domain.model;

import java.sql.Timestamp;
import java.util.Objects;

public class Manufacture {
    private String id;
    private String idDoc;
    private int position;
    private String docNumber;
    private String idOrder;
    private Timestamp timeManufacture;
    private Long time21;
    private int quantity;
    private String idTmc;
    private String descrSecond;
    private int sizeA;
    private int sizeB;
    private int sizeC;
    private String embodiment;

    public Manufacture(String id, String idDoc, int position, String docNumber, String idOrder, Timestamp timeManuf,
                       Long time21, int quantity, String idTmc, String descrSecond, int sizeA, int sizeB, int sizeC,
                       String embodiment) {
        this.id = id;
        this.idDoc = idDoc;
        this.position = position;
        this.docNumber = docNumber;
        this.idOrder = idOrder;
        this.timeManufacture = timeManuf;
        this.time21 = time21;
        this.quantity = quantity;
        this.idTmc = idTmc;
        this.descrSecond = descrSecond;
        this.sizeA = sizeA;
        this.sizeB = sizeB;
        this.sizeC = sizeC;
        this.embodiment = embodiment;
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

    public int getPosition() {
        return position;
    }

    public void setPosition(int position) {
        this.position = position;
    }

    public String getDocNumber() {
        return docNumber;
    }

    public void setDocNumber(String docNumber) {
        this.docNumber = docNumber;
    }

    public String getIdOrder() {
        return idOrder;
    }

    public void setIdOrder(String idOrder) {
        this.idOrder = idOrder;
    }

    public Timestamp getTimeManufacture() {
        return timeManufacture;
    }

    public void setTimeManufacture(Timestamp timeManufacture) {
        this.timeManufacture = timeManufacture;
    }

    public Long getTime21() {
        return time21;
    }

    public void setTime21(Long time21) {
        this.time21 = time21;
    }

    public int getQuantity() {
        return quantity;
    }

    public void setQuantity(int quantity) {
        this.quantity = quantity;
    }

    public String getIdTmc() {
        return idTmc;
    }

    public void setIdTmc(String idTmc) {
        this.idTmc = idTmc;
    }

    public String getDescrSecond() {
        return descrSecond;
    }

    public void setDescrSecond(String descrSecond) {
        this.descrSecond = descrSecond;
    }

    public int getSizeA() {
        return sizeA;
    }

    public void setSizeA(int sizeA) {
        this.sizeA = sizeA;
    }

    public int getSizeB() {
        return sizeB;
    }

    public void setSizeB(int sizeB) {
        this.sizeB = sizeB;
    }

    public int getSizeC() {
        return sizeC;
    }

    public void setSizeC(int sizeC) {
        this.sizeC = sizeC;
    }

    public String getEmbodiment() {
        return embodiment;
    }

    public void setEmbodiment(String embodiment) {
        this.embodiment = embodiment;
    }

    public Manufacture getManufacture() {
        return this;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj.getClass().equals(Manufacture.class)) {
            Manufacture manufacture = (Manufacture) obj;

            boolean equalsId = this.id.equals(manufacture.id);
            boolean equalsIdDoc = this.idDoc.equals(manufacture.idDoc);
            boolean equalsPosition = (this.position == manufacture.position);
            boolean equalsDocNumber = this.docNumber.equals(manufacture.docNumber);
            boolean equalsOrder = this.idOrder.equals(manufacture.idOrder);
            boolean equalsTimeManuf = this.timeManufacture.equals(manufacture.timeManufacture);
            boolean equalsTime21 = (this.time21.equals(manufacture.time21));
            boolean equalsQuantity = (this.quantity == manufacture.quantity);
            boolean equalsTmc = this.idTmc.equals(manufacture.idTmc);
            boolean equalsDescrSecond = this.descrSecond.equals(manufacture.descrSecond);
            boolean equalsSizeA = (this.sizeA == manufacture.sizeA);
            boolean equalsSizeB = (this.sizeB == manufacture.sizeB);
            boolean equalsSizeC = (this.sizeC == manufacture.sizeC);
            boolean equalsEmbodiment = this.embodiment.equals(manufacture.embodiment);

            return equalsId & equalsIdDoc & equalsPosition & equalsDocNumber & equalsOrder & equalsTimeManuf & equalsTime21 &
                    equalsQuantity & equalsTmc & equalsDescrSecond & equalsSizeA
                    & equalsSizeB & equalsSizeC & equalsEmbodiment;
        }
        return false;
    }

    public String getDifferences(Manufacture manufacture) {
        String result = "";
        if (! this.id.equals(manufacture.id)) {
            result += "id [" + manufacture.id + "--> " + this.id + "] ";
        }
        if (! this.idDoc.equals(manufacture.idDoc) ) {
            result += "idDoc [" + manufacture.idDoc + "--> " + this.idDoc + "] ";
        }
        if (this.position != manufacture.position) {
            result += "position [" + manufacture.position + "--> " + this.position + "] ";
        }
        if (! this.docNumber.equals(manufacture.docNumber) ) {
            result += "docNumber [" + manufacture.docNumber + "--> " + this.docNumber + "] ";
        }
        if (! this.idOrder.equals(manufacture.idOrder) ) {
            result += "idOrder [" + manufacture.idOrder + "--> " + this.idOrder + "] ";
        }
        if (! this.timeManufacture.equals(manufacture.timeManufacture) ) {
            result += "timeManufacture [" + manufacture.timeManufacture + "--> " + this.timeManufacture + "] ";
        }
        if (! this.time21.equals(manufacture.time21) ) {
            result += "time21 [" + manufacture.time21 + "--> " + this.time21 + "] ";
        }
        if (this.quantity != manufacture.quantity) {
            result += "quantity [" + manufacture.quantity + "--> " + this.quantity + "] ";
        }
        if (! this.idTmc.equals(manufacture.idTmc) ) {
            result += "idTmc [" + manufacture.idTmc + "--> " + this.idTmc + "] ";
        }
        if (! this.descrSecond.equals(manufacture.descrSecond) ) {
            result += "descrSecond [" + manufacture.descrSecond + "--> " + this.descrSecond + "] ";
        }
        if (this.sizeA != manufacture.sizeA) {
            result += "sizeA [" + manufacture.sizeA + "--> " + this.sizeA + "] ";
        }
        if (this.sizeB != manufacture.sizeB) {
            result += "sizeB [" + manufacture.sizeB + "--> " + this.sizeB + "] ";
        }
        if (this.sizeC != manufacture.sizeC) {
            result += "sizeC [" + manufacture.sizeC + "--> " + this.sizeC + "] ";
        }
        if (! this.embodiment.equals(manufacture.embodiment) ) {
            result += "embodiment [" + manufacture.embodiment + "--> " + this.embodiment + "] ";
        }

        return result;
    }

    @Override
    public String toString() {
        return "Manufacture{" +
                "id='" + id + '\'' +
                ", idDoc='" + idDoc + '\'' +
                ", position=" + position +
                ", docNumber='" + docNumber + '\'' +
                ", idOrder='" + idOrder + '\'' +
                ", timeManufacture=" + timeManufacture +
                ", time21=" + time21 +
                ", quantity=" + quantity +
                ", idTmc='" + idTmc + '\'' +
                ", descrSecond='" + descrSecond + '\'' +
                ", sizeA=" + sizeA +
                ", sizeB=" + sizeB +
                ", sizeC=" + sizeC +
                ", embodiment='" + embodiment + '\'' +
                '}';
    }
}
