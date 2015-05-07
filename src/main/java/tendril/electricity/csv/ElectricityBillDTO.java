package tendril.electricity.csv;

import java.io.Serializable;

/**
 * Created by cromaniuc on 5/5/2015.
 */
public class ElectricityBillDTO implements Serializable {

    private String billingPeriodFrom = null;
    private String billingPeriodTo = null;
    private Integer lastBill = null;
    private Integer currentBill = null;

    public ElectricityBillDTO() {
    }

    public String getBillingPeriodFrom() {
        return billingPeriodFrom;
    }

    public void setBillingPeriodFrom(String billingPeriodFrom) {
        this.billingPeriodFrom = billingPeriodFrom;
    }

    public String getBillingPeriodTo() {
        return billingPeriodTo;
    }

    public void setBillingPeriodTo(String billingPeriodTo) {
        this.billingPeriodTo = billingPeriodTo;
    }

    public Integer getLastBill() {
        return lastBill;
    }

    public void setLastBill(Integer lastBill) {
        this.lastBill = lastBill;
    }

    public Integer getCurrentBill() {
        return currentBill;
    }

    public void setCurrentBill(Integer currentBill) {
        this.currentBill = currentBill;
    }


    @Override
    public String toString() {
        return "ElectricityBillDTO{" +
                "billingPeriodFrom='" + billingPeriodFrom + '\'' +
                ", billingPeriodTo='" + billingPeriodTo + '\'' +
                ", lastBill=" + lastBill +
                ", currentBill=" + currentBill +
                '}';
    }
}
