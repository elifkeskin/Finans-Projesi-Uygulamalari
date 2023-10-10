package com.xbank.bigdata.efthavale.producer;

import org.json.simple.JSONObject;

import java.io.File;
import java.io.FileNotFoundException;
import java.sql.Timestamp;
import java.util.*;


public class DataGenerator {


    // Global olarak listeleri tanımlarız.
    public static List<String> names = new ArrayList<>();
    public static List<String> surnames = new ArrayList<>();

    public static Random r = new Random();

    public static int pid=10000;

    public DataGenerator() throws FileNotFoundException {
        File fileName = new File("C:\\Finans Projesi Kaynaklar\\isimler.txt");
        File fileSurname = new File("C:\\Finans Projesi Kaynaklar\\soyisimler.txt");

        Scanner fileNameScanner = new Scanner(fileName);
        Scanner fileSurnameScanner = new Scanner(fileSurname);


        // hasNext => Dosyaları bitene kadar okuma yapar.
        while (fileNameScanner.hasNext()) {
            names.add(fileNameScanner.nextLine()); // Okunan her satırı listenin içine ekler.

        }

        while (fileSurnameScanner.hasNext()) {
            surnames.add(fileSurnameScanner.nextLine());   // Okunan her satırı listenin içine ekler.
        }


    }

    public String generate() {
            Timestamp timestamp = new Timestamp(System.currentTimeMillis());

            List<String> btype = Arrays.asList("TL", "USD", "EUR");


                JSONObject data = new JSONObject();
                data.put("pid", pid++);
                data.put("ptype", "H");

                JSONObject account = new JSONObject();
                account.put("oid", generateID());
                account.put("title", nameSurnameGenerator());
                account.put("iban", "TR" + generateID());

                data.put("account", account); // İç içe JSON obje oluşmasını sağlamak için.

                JSONObject info = new JSONObject();
                info.put("title", nameSurnameGenerator());
                info.put("iban", "TR" + generateID());
                info.put("bank", "X bank");

                data.put("info", info);

                data.put("balance", r.nextInt((0) + 999999));
                data.put("btype", btype.get(r.nextInt(btype.size())));
                data.put("current_ts", timestamp.toString());


                return data.toJSONString();


        }
        // 11 haneli rastgele sayı oluşturma metodu
        public static long generateID ()
        {
            Random r = new Random();
            long numbers = 10000000000L + (long) (r.nextDouble() * 99999999999L);
            return numbers;
        }

        // İsim ve soyismi rastgele oluşturan metot
        public static String nameSurnameGenerator()
        {
            String name = names.get(r.nextInt(names.size()));
            String surname = surnames.get(r.nextInt(surnames.size()));

            return name + " " + surname;

        }
    }

