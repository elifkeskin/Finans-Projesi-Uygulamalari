package com.xbank.bigdata.efthavale.consumer;


import com.mongodb.spark.MongoSpark;
import org.apache.spark.api.java.function.VoidFunction2;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;

import java.util.concurrent.TimeoutException;

public class Application {
    public static void main(String[] args) throws TimeoutException, StreamingQueryException {

        //mongodb://ip adresi/database adı.collection adı
        SparkSession sparkSession = SparkSession.builder().master("local").appName("EFT/HAVALE TAKİP")
                .config("spark.mongodb.output.uri","mongodb://206.189.117.218/finansDB.havaleeft").getOrCreate();

        // Verilerin düzgün okunabilmesi için veriye şema giydiririz.
        StructType accountSchema=new StructType()
                .add("iban", DataTypes.StringType)
                .add("oid", DataTypes.LongType)
                .add("title", DataTypes.StringType);

        StructType infoSchema=new StructType()
                .add("bank", DataTypes.StringType)
                .add("iban", DataTypes.StringType)
                .add("title", DataTypes.StringType);


        StructType schema=new StructType()
                .add("current_ts", DataTypes.TimestampType)
                .add("balance", DataTypes.IntegerType)
                .add("btype", DataTypes.StringType)
                .add("pid", DataTypes.LongType)
                .add("ptype", DataTypes.StringType)
                .add("account", accountSchema)
                .add("info", infoSchema);


        //loadDS.printSchema();--> data (value: binary (nullable = true)) olduğu için datayı STRING'e çeviririz.
        Dataset<Row> loadDS =sparkSession.readStream().format("kafka")
                .option("kafka.bootstrap.servers", "206.189.117.218:9092")
                .option("subscribe", "efthavale")  //Abone olacağımız topic'i belirtiriz.
                .load().selectExpr("CAST(value AS STRING)");


        //loadDS'in column adı value (data) olanı, yukarıda belirttiğimiz şemaya göre JSON formatıyla okur.
        // Adına "data" ismini veririz ve  .select("data.*") ifadesi ile data altındaki tüm kolonları seçmiş oluruz.
        Dataset<Row> rawDS = loadDS.select(functions.from_json(loadDS.col("value"), schema).as("data"))
                .select("data.*");


        // Kontrol amaçlı  printSchema() ile kolonların isimlerini ve tiplerini ve tüm datanın okunup okunmadığı  kontrol ederiz.
        /*rawDS.printSchema();

        rawDS.show();*/

       //Analizlere başlamadan önce, gelen dataset üzerinde işlem yapacağımız alanlar için filtreleme yaparız.
        Dataset<Row> havaleTypeDS = rawDS.filter("ptype = 'H'");

        //En yoğun havale yapılan gün ve saati bulmak için timestamp ve time window'u kullanırız.
        Dataset<Row> volumeDS = havaleTypeDS.groupBy(functions.window(havaleTypeDS.col("current_ts"), "5 minute"),
                havaleTypeDS.col("btype")).sum("balance");


        //MongoDB'ye veriyi aktarmak için: (rowDataset--> volumeDS)
        // Streaming bir veri olduğu için direkt write() yazamıyoruz.
        volumeDS.writeStream().outputMode("complete").foreachBatch(new VoidFunction2<Dataset<Row>, Long>() {
            @Override
            public void call(Dataset<Row> rowDataset, Long aLong) throws Exception {
                 MongoSpark.write(rowDataset).mode("append").save();

            }
        }).start().awaitTermination();

        //Kafka'dan gelen anlık veriyi konsole yazmak için:
        //volumeDS.writeStream().outputMode("complete").format("console").start().awaitTermination();





    }
}
