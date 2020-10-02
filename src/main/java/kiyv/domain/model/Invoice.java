package kiyv.domain.model;

import java.sql.Timestamp;

public class Invoice {
    private String idDoc;
    private String docNumber;
    private String idOrder;
    private Timestamp timeInvoice;
    private Long time22;
    private double price;

    public Invoice(String idDoc, String docNumber, String idOrder, Timestamp timeInvoice, Long time22, double price) {
        this.idDoc = idDoc;
        this.docNumber = docNumber;
        this.idOrder = idOrder;
        this.timeInvoice = timeInvoice;
        this.time22 = time22;
        this.price = price;
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

    public String getIdOrder() {
        return idOrder;
    }

    public void setIdOrder(String idOrder) {
        this.idOrder = idOrder;
    }

    public Timestamp getTimeInvoice() {
        return timeInvoice;
    }

    public void setTimeInvoice(Timestamp timeInvoice) {
        this.timeInvoice = timeInvoice;
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

    public Invoice getInvoice() {
        return this;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj.getClass().equals(Invoice.class)) {
            Invoice invoice = (Invoice) obj;

            boolean equalsIdDoc = this.idDoc.equals(invoice.idDoc);
            boolean equalsDocNumber = this.docNumber.equals(invoice.docNumber);
            boolean equalsOrder = this.idOrder.equals(invoice.idOrder);
            boolean equalsTimeInvoice = this.timeInvoice.equals(invoice.timeInvoice);
            boolean equalsTime22 = (this.time22.equals(invoice.time22));
            boolean equalsPrice = (this.price == invoice.price);

            return equalsIdDoc & equalsDocNumber & equalsOrder & equalsTimeInvoice & equalsTime22 & equalsPrice;
        }
        return false;
    }

    public String getDifferences(Invoice invoice) {
        String result = "";
        if (! this.idDoc.equals(invoice.idDoc) ) {
            result += "idDoc [" + invoice.idDoc + "--> " + this.idDoc + "] ";
        }
        if (! this.docNumber.equals(invoice.docNumber) ) {
            result += "docNumber [" + invoice.docNumber + "--> " + this.docNumber + "] ";
        }
        if (! this.idOrder.equals(invoice.idOrder) ) {
            result += "idOrder [" + invoice.idOrder + "--> " + this.idOrder + "] ";
        }
        if (! this.timeInvoice.equals(invoice.timeInvoice) ) {
            result += "timeInvoice [" + invoice.timeInvoice + "--> " + this.timeInvoice + "] ";
        }
        if (! this.time22.equals(invoice.time22) ) {
            result += "time22 [" + invoice.time22 + "--> " + this.time22 + "] ";
        }
        if (this.price != invoice.price) {
            result += "price [" + invoice.price + "--> " + this.price + "] ";
        }
        return result;
    }

    @Override
    public String toString() {
        return "Invoice{" +
                "idDoc='" + idDoc + '\'' +
                ", docNumber='" + docNumber + '\'' +
                ", idOrder='" + idOrder + '\'' +
                ", timeInvoice=" + timeInvoice +
                ", time22=" + time22 +
                ", price=" + price +
                '}';
    }
}
