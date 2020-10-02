package kiyv.domain.model;

import kiyv.domain.tools.DateConverter;

import java.sql.Date;
import java.sql.Timestamp;
import java.time.LocalDateTime;

public class Order {
    private int bigNumber;
    private String idDoc;
    private String idClient;
    private String idManager;
    private int durationTime;
    private String docNumber;
    private String docNumberManuf;
    private String docNumberInvoice;
    private int posCount;
    private String client; // Don't compare
    private String manager; // Don't compare
    private Timestamp dateCreate;
    private Timestamp dateToFactory;
    private Timestamp dateToShipment; // Don't compare
    private Long time22;
    private double price;
    private double payment;
    private Timestamp timeManuf;
    private Timestamp timeInvoice;
    private int isParsing;

    public Order(int bigNumber, String idDoc, String idClient, String idManager, int durationTime, String docNumber,
                 int posCount, String client, String manager, Timestamp dateCreate, Timestamp dateToFactory, Timestamp dateToShipment, double price) {
        this.bigNumber = bigNumber;
        this.idDoc = idDoc;
        this.idClient = idClient;
        this.idManager = idManager;
        this.durationTime = durationTime;
        this.docNumber = docNumber;
        this.posCount = posCount;
        this.client = client;
        this.manager = manager;
        this.dateCreate = dateCreate;
        this.dateToFactory = dateToFactory;
        this.dateToShipment = dateToShipment;
        this.price = price;
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

    public String getIdClient() {
        return idClient;
    }

    public void setIdClient(String idClient) {
        this.idClient = idClient;
    }

    public String getIdManager() {
        return idManager;
    }

    public void setIdManager(String idManager) {
        this.idManager = idManager;
    }

    public int getDurationTime() {
        return durationTime;
    }

    public void setDurationTime(int durationTime) {
        this.durationTime = durationTime;
    }

    public String getDocNumber() {
        return docNumber;
    }

    public void setDocNumber(String docNumber) {
        this.docNumber = docNumber;
    }

    public String getDocNumberManuf() {
        return docNumberManuf;
    }

    public void setDocNumberManuf(String docNumberManuf) {
        this.docNumberManuf = docNumberManuf;
    }

    public String getDocNumberInvoice() {
        return docNumberInvoice;
    }

    public void setDocNumberInvoice(String docNumberInvoice) {
        this.docNumberInvoice = docNumberInvoice;
    }

    public int getPosCount() {
        return posCount;
    }

    public void setPosCount(int posCount) {
        this.posCount = posCount;
    }

    public String getClient() {
        return client;
    }

    public void setClient(String client) {
        this.client = client;
    }

    public String getManager() {
        return manager;
    }

    public void setManager(String manager) {
        this.manager = manager;
    }

    public Timestamp getDateCreate() {
        return dateCreate;
    }

    public void setDateCreate(Timestamp dateCreate) {
        this.dateCreate = dateCreate;
    }

    public Timestamp getDateToFactory() {
        return dateToFactory;
    }

    public void setDateToFactory(Timestamp dateToFactory) {
        this.dateToFactory = dateToFactory;
    }

    public Timestamp getDateToShipment() {
        return dateToShipment;
    }

    public void setDateToShipment(Timestamp dateToShipment) {
        this.dateToShipment = dateToShipment;
    }

    public Long getTime22() {
        return time22;
    }

    public void setTime22(Long time22) {
        this.time22 = time22;
    }

    public double getPrice() {
        return price;
    }

    public void setPrice(double price) {
        this.price = price;
    }

    public double getPayment() {
        return payment;
    }

    public void setPayment(double payment) {
        this.payment = payment;
    }

    public Timestamp getTimeManuf() {
        return timeManuf;
    }

    public void setTimeManuf(Timestamp timeManuf) {
        this.timeManuf = timeManuf;
    }

    public Timestamp getTimeInvoice() {
        return timeInvoice;
    }

    public void setTimeInvoice(Timestamp timeInvoice) {
        this.timeInvoice = timeInvoice;
    }

    public int getIsParsing() {
        return isParsing;
    }

    public void setIsParsing(int isParsing) {
        this.isParsing = isParsing;
    }

    public Order getOrder() {
        return this;
    }

    @Override
    public String toString() {
        return "Order{" +
                "bigNumber=" + bigNumber +
                ", idDoc='" + idDoc + '\'' +
                ", idClient='" + idClient + '\'' +
                ", idManager='" + idManager + '\'' +
                ", durationTime=" + durationTime +
                ", docNumber='" + docNumber + '\'' +
                ", docNumberManuf='" + docNumberManuf + '\'' +
                ", docNumberInvoice='" + docNumberInvoice + '\'' +
                ", posCount=" + posCount +
                ", client='" + client + '\'' +
                ", manager='" + manager + '\'' +
                ", dateCreate=" + dateCreate +
                ", dateToFactory=" + dateToFactory +
                ", dateToShipment=" + dateToShipment +
                ", time22=" + time22 +
                ", price=" + price +
                ", payment=" + payment +
                ", timeManuf=" + timeManuf +
                ", timeInvoice=" + timeInvoice +
                ", isParsing=" + isParsing +
                '}';
    }

    @Override
    public boolean equals(Object obj) {
        if (obj.getClass().equals(Order.class)) {
            Order order = (Order) obj;

            boolean equalsBigNumber = this.bigNumber == order.bigNumber;
            boolean equalsOrder = this.idDoc.equalsIgnoreCase(order.idDoc);
            boolean equalsIdClient = this.idClient.equalsIgnoreCase(order.idClient);
            boolean equalsIdManager = this.idManager.equalsIgnoreCase(order.idManager);
            boolean equalsDurationTime = this.durationTime == order.durationTime;
            boolean equalsDocNumber = this.docNumber.equalsIgnoreCase(order.docNumber);
            boolean equalsPositionCount = this.posCount == order.posCount;
            boolean equalsDateCreate = this.dateCreate.getTime() == order.dateCreate.getTime();
            boolean equalsDateToFactory = this.dateToFactory.getTime() == order.dateToFactory.getTime();
            boolean equalsClient = this.client.equalsIgnoreCase(order.client);
            boolean equalsManager = this.manager.equalsIgnoreCase(order.manager);
            boolean equalsPrice = this.price == order.price;

            return equalsBigNumber & equalsOrder & equalsIdClient & equalsIdManager & equalsDurationTime & equalsDocNumber
                    & equalsPositionCount & equalsDateCreate & equalsDateToFactory & equalsClient & equalsManager & equalsPrice;
        }
        return false;
    }

    public String getDifferences(Order order) {
        String result = "";
        if (this.bigNumber != order.bigNumber) {
            result += "bigNumber [" + order.bigNumber + "--> " + this.bigNumber + "] ";
        }
        if (! this.idDoc.equals(order.idDoc) ) {
            result += "idDoc [" + order.idDoc + "--> " + this.idDoc + "] ";
        }
        if (! this.idClient.equals(order.idClient) ) {
            result += "idClient [" + order.idClient + "--> " + this.idClient + "] ";
        }
        if (! this.idManager.equals(order.idManager) ) {
            result += "idManager [" + order.idManager + "--> " + this.idManager + "] ";
        }
        if (this.durationTime != order.durationTime) {
            result += "durationTime [" + order.durationTime + "--> " + this.durationTime + "] ";
        }
        if (! this.docNumber.equals(order.docNumber) ) {
            result += "docNumber [" + order.docNumber + "--> " + this.docNumber + "] ";
        }
        if (this.posCount != order.posCount) {
            result += "posCount [" + order.posCount + "--> " + this.posCount + "] ";
        }
        if (this.dateCreate.getTime() != order.dateCreate.getTime()) {
            result += "dateCreate [" + DateConverter.dateToString(order.dateCreate.getTime()) + "--> "
                    + DateConverter.dateToString(this.dateCreate.getTime()) + "] ";
        }
        if (this.dateToFactory.getTime() != order.dateToFactory.getTime()) {
            result += "dateToFactory [" + DateConverter.dateToString(order.dateToFactory.getTime()) + "--> "
                    + DateConverter.dateToString(this.dateToFactory.getTime()) + "] ";
        }
        if (! this.client.equals(order.client) ) {
            result += "client [" + order.client + "--> " + this.client + "] ";
        }
        if (! this.manager.equals(order.manager) ) {
            result += "manager [" + order.manager + "--> " + this.manager + "] ";
        }
        if (this.price != order.price) {
            result += "price [" + order.price + "--> " + this.price + "] ";
        }
        return result;
    }
}
