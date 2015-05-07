package tendril.electricity.util;

import com.datastax.driver.core.BatchStatement;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.querybuilder.Insert;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.datastax.driver.core.querybuilder.Truncate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.supercsv.cellprocessor.ParseInt;
import org.supercsv.cellprocessor.constraint.NotNull;
import org.supercsv.cellprocessor.ift.CellProcessor;
import org.supercsv.io.CsvBeanReader;
import org.supercsv.io.ICsvBeanReader;
import org.supercsv.prefs.CsvPreference;
import tendril.electricity.csv.ElectricityBill;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URL;
import java.util.LinkedList;
import java.util.List;

/**
 * Created by cromaniuc on 5/5/2015.
 */
public final class ElectricityBillUtil {

    public static void main(String[] args) {
        initCassandraWithDataFromCsvFile();
    }

    public static final Logger LOGGER = LoggerFactory.getLogger(ElectricityBillUtil.class);

    private ElectricityBillUtil() {
    }

    public static void initCassandraWithDataFromCsvFile() {
        ICsvBeanReader beanReader = null;

        Cluster cluster;
        Session session = null;

        try {
            cluster = Cluster.builder().addContactPoint("127.0.0.1").build();
            session = cluster.connect("ElectricityBills");

            clearDB(session);

            Insert insertStatement = getInsertStatement();
            PreparedStatement ps = session.prepare(insertStatement.toString());

            URL url = Thread.currentThread().getContextClassLoader().getResource("data/electricitybills.csv");
            beanReader = new CsvBeanReader(
                    new BufferedReader(
                    new InputStreamReader(url.openStream(), "UTF-8")),
                    CsvPreference.STANDARD_PREFERENCE);
            List<ElectricityBill> electricityBills = readCsv(beanReader);

            populateCassandraWithElectricityBills(session, ps, electricityBills);

            electricityBills.clear();

        } catch (Exception e) {
            LOGGER.error(e.getMessage(), e);
        } finally {
            try {
                if (beanReader != null) {
                    beanReader.close();
                }

                if (session != null) {
                    session.close();
                }
            } catch (IOException e) {
                LOGGER.error(e.getMessage(), e);
            }
        }

    }

    private static List<ElectricityBill> readCsv(ICsvBeanReader beanReader) throws IOException {
        final String[] header = beanReader.getHeader(true);
        CellProcessor[] processors = new CellProcessor[]{
                new ParseInt(),
                new NotNull(),
                new NotNull(),
                new NotNull(),
                new ParseInt(),
                new ParseInt(),
        };
        List<ElectricityBill> electricityBills = new LinkedList<ElectricityBill>();
        ElectricityBill electricityBill;
        while ((electricityBill = beanReader.read(ElectricityBill.class, header, processors)) != null) {
            electricityBills.add(electricityBill);
        }
        return electricityBills;
    }

    private static Insert getInsertStatement() {
        Insert insertStatement = QueryBuilder.insertInto("ElectricityBill");
        insertStatement
                .value("id", QueryBuilder.bindMarker())
                .value("name", QueryBuilder.bindMarker())
                .value("billingPeriodFrom", QueryBuilder.bindMarker())
                .value("billingPeriodTo", QueryBuilder.bindMarker())
                .value("lastBill", QueryBuilder.bindMarker())
                .value("currentBill", QueryBuilder.bindMarker())
        ;
        return insertStatement;
    }

    private static void clearDB(Session session) {
        Truncate deleteStatement = QueryBuilder.truncate("ElectricityBill");
        session.execute(deleteStatement);
    }

    private static void populateCassandraWithElectricityBills(Session session, PreparedStatement ps, List<ElectricityBill> electricityBills) {
        BatchStatement batch = new BatchStatement();
        for (ElectricityBill bill : electricityBills) {
            batch.add(ps.bind(
                    bill.getId(),
                    bill.getName(),
                    bill.getBillingPeriodFrom(),
                    bill.getBillingPeriodTo(),
                    bill.getLastBill(),
                    bill.getCurrentBill()
            ));
        }
        session.execute(batch);
    }
}
