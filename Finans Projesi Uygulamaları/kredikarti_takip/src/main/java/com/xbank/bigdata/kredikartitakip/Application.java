package com.xbank.bigdata.kredikartitakip;

import com.mongodb.spark.MongoSpark;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.types.DataTypes;

public class Application {
    public static void main(String[] args) {
        SparkSession sparkSession = SparkSession.builder()
                .appName("Kredi Kartı Takip")
                .config("spark.mongodb.output.uri","mongodb://206.189.117.218/finansDB")
                .master("local").getOrCreate();

        Dataset<Row> rawDS = sparkSession.read()
                 .option("header", true)
                 .option("inferSchema", true).csv("C:\\Finans Projesi Kaynaklar\\kredikartı_islem.csv");

        /*rawDS.show();*/
        //Hangi kullanıcı, hangi kanaldan işlem yapıyor? oid--> Kullanıcı numarası
        Dataset<Row> processDS = rawDS.groupBy("oid", "ptype").count();

        //Yatayda işlemleri daha rahat görebilmek için "pivot()" kullanıyoruz.
        //Olmayan kayıtları da sıfır olarak gösteriyoruz.
        Dataset<Row> resultDS = processDS.groupBy("oid").pivot("ptype").sum("count").na().fill(0);

        // Kredi kartını günde en az 2 kere kullanan, ama QR kodu hiç kullamayan kişileri bize gösterir.
        Dataset<Row> qrCodeDS = resultDS.filter(resultDS.col("1000").$greater(2).and(resultDS.col("1001")
                .equalTo(0)));
        Dataset<Row> resultQrCodeDS = qrCodeDS.sort(functions.desc("1000"));

        //İnternet alışverişinde normalde kredi kartı kullanmasına rağmen sanal kart kullanmayanlar:
        Dataset<Row> cardDS = resultDS.filter(resultDS.col("2000").$greater$eq("3")
                .and(resultDS.col("2001").equalTo("0")))
                .sort(functions.desc("2000"));

        //Hangi kanalda, 2 dk'lık aralıklarla kaç işlem yapılmış?
        Dataset<Row> windowDayDS = rawDS.groupBy(functions.window(rawDS.col("current_ts"), "2 minute"),
                        rawDS.col("ptype")).count().groupBy("window").pivot("ptype")
                        .sum("count").na().fill(0);


        //Her bir rdd'yi ayrı bir Collection'a yazmak için:
        MongoSpark.write(resultQrCodeDS).option("Collection", "qrCode").mode("append").save();
        MongoSpark.write(cardDS).option("Collection", "creditcard").mode("append").save();
        MongoSpark.write(windowDayDS).option("Collection", "windowDay").mode("append").save();


    }
}
