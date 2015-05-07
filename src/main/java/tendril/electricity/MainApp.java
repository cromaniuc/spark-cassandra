package tendril.electricity;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import tendril.electricity.csv.ElectricityBillDTO;
import tendril.electricity.util.ElectricityBillUtil;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import scala.Tuple2;

import com.datastax.spark.connector.japi.CassandraJavaUtil;
import com.datastax.spark.connector.japi.CassandraRow;
import com.datastax.spark.connector.japi.SparkContextJavaFunctions;
import com.datastax.spark.connector.japi.rdd.CassandraJavaRDD;


/**
 * Created by cromaniuc on 5/5/2015.
 */
public class MainApp {

    private static final Logger LOGGER = LoggerFactory
            .getLogger(MainApp.class);


    public static void main(String[] args) throws IOException {
        try {
            ElectricityBillUtil.initCassandraWithDataFromCsvFile();
            initSpark();
        } catch (Exception e) {
            LOGGER.error(e.getMessage(), e);
            System.exit(1);
        }
    }


    public static void initSpark() {
        SparkConf conf = new SparkConf(false)
/*
                .setMaster("spark://ubuntu-VirtualBox.local:7077")
*/
                .setMaster("local")
                .setAppName("DatastaxtTests")
                .set("spark.executor.memory", "1g")
                .set("spark.cassandra.connection.host", "localhost")
                .set("spark.cassandra.connection.native.port", "9042")
                .set("spark.cassandra.connection.rpc.port", "9160");
        SparkContext ctx = new SparkContext(conf);


        addClasspathEntriesForWorkers(ctx);


        SparkContextJavaFunctions functions = CassandraJavaUtil.javaFunctions(ctx);
        CassandraJavaRDD<CassandraRow> rdd = functions.cassandraTable("electricitybills", "electricitybill");
        rdd.cache();

        //1
        JavaPairRDD<String, List<ElectricityBillDTO>> billsGroupedByConsumer = getBillsGroupedByConsumer(rdd);

        //2
        printNoOfBills(billsGroupedByConsumer);


        //3
        JavaPairRDD<String, Integer> consumerBillsTotal = printBillsTotal(billsGroupedByConsumer);


        JavaPairRDD<String, Integer> consumerBillsSize = billsGroupedByConsumer.mapValues(new Function<List<ElectricityBillDTO>, Integer>() {

            @Override
            public Integer call(List<ElectricityBillDTO> allBills) throws Exception {
                return allBills.size();
            }
        });

        //4
        printAverageBill(consumerBillsSize, consumerBillsTotal);

        //5
        printNoOfConsumersWithBillsGT10(consumerBillsTotal);


    }

    private static JavaPairRDD<String, Integer> printBillsTotal(JavaPairRDD<String, List<ElectricityBillDTO>> billsGroupedByConsumer) {
        JavaPairRDD<String, Integer> consumerBillsTotal = billsGroupedByConsumer.mapValues(new Function<List<ElectricityBillDTO>, Integer>() {
            @Override
            public Integer call(List<ElectricityBillDTO> allBills) throws Exception {
                int total = 0;
                for (ElectricityBillDTO billDTO : allBills) {
                    total += billDTO.getCurrentBill();
                }
                return total;
            }
        });

        LOGGER.info("Bills total");
        List<Tuple2<String, Integer>> totals = consumerBillsTotal.collect();

        for (Tuple2<String, Integer> tuple : totals) {
            LOGGER.info(tuple._1() + " : " + tuple._2());
        }
        return consumerBillsTotal;
    }

    private static void printAverageBill(JavaPairRDD<String, Integer> consumerBillsSize, JavaPairRDD<String, Integer> consumerBillsTotal) {
        List<Tuple2<String, Double>> averageResults = consumerBillsTotal.join(consumerBillsSize).mapValues(new Function<Tuple2<Integer, Integer>, Double>() {
            @Override
            public Double call(Tuple2<Integer, Integer> tuple) throws Exception {
                return Double.valueOf((double) tuple._1() / tuple._2());
            }
        }).collect();

        LOGGER.info("Average bill");
        for (Tuple2<String, Double> tuple : averageResults) {
            LOGGER.info(tuple._1() + " : " + tuple._2());
        }
    }

    private static void printNoOfConsumersWithBillsGT10(JavaPairRDD<String, Integer> consumerBillsTotal) {
        long numberOfConsumersWithBillsGT10 = consumerBillsTotal.filter(new Function<Tuple2<String, Integer>, Boolean>() {
            public Boolean call(Tuple2<String, Integer> s) {
                return s._2() > 10;
        }
        }).count();

        LOGGER.info("Number of consumers with bills greater than 10, {}", numberOfConsumersWithBillsGT10);
    }

    private static void printNoOfBills(JavaPairRDD<String, List<ElectricityBillDTO>> billsGroupedByConsumer) {
        List<Tuple2<String, List<ElectricityBillDTO>>> sizesResults = billsGroupedByConsumer.collect();
        LOGGER.info("No of bills");
        for (Tuple2<String, List<ElectricityBillDTO>> tuple : sizesResults) {
            LOGGER.info(tuple._1() + " : " + tuple._2());
        }
    }

    private static JavaPairRDD<String, List<ElectricityBillDTO>> getBillsGroupedByConsumer(CassandraJavaRDD<CassandraRow> rdd) {
        JavaPairRDD<String, List<ElectricityBillDTO>> billsGroupedByConsumer = rdd.groupBy(new Function<CassandraRow, String>() {
            @Override
            public String call(CassandraRow row) throws Exception {
                return row.getString("name");
            }
        }).
                mapToPair(new PairFunction<Tuple2<String, Iterable<CassandraRow>>, String, List<ElectricityBillDTO>>() {
                    @Override
                    public Tuple2<String, List<ElectricityBillDTO>> call(Tuple2<String, Iterable<CassandraRow>> t) throws Exception {
                        List<ElectricityBillDTO> electricityBills = new ArrayList<ElectricityBillDTO>();

                        Iterator<CassandraRow> rows = t._2().iterator();
                        while (rows.hasNext()) {

                            CassandraRow row = rows.next();
                            ElectricityBillDTO bill = new ElectricityBillDTO();
                            bill.setBillingPeriodFrom(row.getString("billingperiodfrom"));
                            bill.setBillingPeriodTo(row.getString("billingperiodto"));
                            bill.setCurrentBill(Integer.valueOf(row.getString("currentbill")));

                            electricityBills.add(bill);
                        }


                        return new Tuple2<String, List<ElectricityBillDTO>>(t._1(), electricityBills);
                    }
                });
        billsGroupedByConsumer.cache();
        return billsGroupedByConsumer;
    }

    private static void addClasspathEntriesForWorkers(SparkContext ctx) {
        ctx.addJar("./lib/spark-cassandra-connector_2.10-1.1.0-beta2.jar");
        ctx.addJar("./lib/spark-cassandra-connector-java_2.10-1.1.0-beta2.jar");
        ctx.addJar("./lib/cassandra-driver-core-2.1.2.jar");
        ctx.addJar("./lib/cassandra-thrift-2.1.1.jar");
        ctx.addJar("./lib/joda-time-2.3.jar");
        ctx.addJar("./lib/energybills-1.0.0-SNAPSHOT.jar");
        ctx.addJar("./lib/google-collections-1.0.jar");
        ctx.addJar("./lib/guava-16.0.1.jar");
    }
}
