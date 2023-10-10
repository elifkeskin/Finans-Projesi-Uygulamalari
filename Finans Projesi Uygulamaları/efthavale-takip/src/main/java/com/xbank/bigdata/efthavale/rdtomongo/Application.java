package com.xbank.bigdata.efthavale.rdtomongo;

import com.mongodb.spark.MongoSpark;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;

public class Application {

    public static void main(String[] args) {
        SparkSession sparkSession=SparkSession.builder()
                 .master("local")
                  //"mongodb://server_ip/database_adi.collection_adi"
                 .config("spark.mongodb.output.uri", "mongodb://206.189.117.218/dwh.havaleislem")
                 .appName("PostgreSQL to MongoDB").getOrCreate();

        //İlişkisel veritabanına bağlanacağımız için format --> jdbc
        Dataset<Row> loadDS = sparkSession.read().format("jdbc")
                .option("driver", "org.postgresql.Driver")
                .option("url", "jdbc:postgresql://206.189.117.218/postgres")
                .option("dbtable", "public.havale_islem")
                .option("user", "postgres")
                .option("password", "12345").load();

        //Analiz yapmadan önce mutlaka kolonların tiplerini kontrol ederiz.
        //loadDS.printSchema();

        // String olan bir column'ı intger'e cast etmek için:
        Dataset<Row> rowDataset = loadDS.withColumn("balance", loadDS.col("balance").cast(DataTypes.IntegerType));
        rowDataset.printSchema();

        MongoSpark.write(rowDataset).mode("append").save();



    }
}
