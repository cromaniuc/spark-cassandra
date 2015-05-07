package tendril.electricity.csv;


/**
 * Created by cromaniuc on 5/5/2015.
 */
public class ElectricityBill {

    private Integer id = null;
    private String name = null;
    private String billingPeriodFrom = null;
    private String billingPeriodTo = null;
    private Integer lastBill = null;
    private Integer currentBill = null;

    public ElectricityBill() {
    }

    public Integer getId() {
        return id;
    }

    public void setId(Integer id) {
        this.id = id;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
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
}
