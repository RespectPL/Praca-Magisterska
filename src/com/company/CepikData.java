package com.company;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

public class CepikData {
    private SparkSession spark;
    private StructType schema;
    //dane pojazdow z poszczegolnych wojewodztw
    private Dataset<Row> wojewodztwo_malopolskie;       // 12
    private Dataset<Row> wojewodztwo_mazowieckie;       // 14
    private Dataset<Row> wojewodztwo_opolskie;          // 16
    private Dataset<Row> wojewodztwo_podkarpackie;      // 18
    private Dataset<Row> wojewodztwo_podlaskie;         // 20
    private Dataset<Row> wojewodztwo_pomorskie;         // 22
    private Dataset<Row> wojewodztwo_slaskie;           // 24
    private Dataset<Row> wojewodztwo_dolnoslaskie;      // 02
    private Dataset<Row> wojewodztwo_kujawskopomorskie; // 04
    private Dataset<Row> wojewodztwo_swietokrzyskie;    // 26
    private Dataset<Row> wojewodztwo_warminskomazurskie;// 28
    private Dataset<Row> wojewodztwo_wielkopolskie;     // 30
    private Dataset<Row> wojewodztwo_zachodniopomorskie;// 32
    private Dataset<Row> wojewodztwo_lubelskie;         // 06
    private Dataset<Row> wojewodztwo_lubuskie;          // 08
    private Dataset<Row> wojewodztwo_lodzkie;           // 10
    private Dataset<Row> nieokreslone_wojewodztwo;      // xx

    public CepikData(SparkSession spark) {
        this.spark = spark;
        this.schema = DataTypes.createStructType(new StructField[] {
                DataTypes.createStructField("pojazd_id", DataTypes.LongType, true),
                DataTypes.createStructField("marka", DataTypes.StringType, true),
                DataTypes.createStructField("kategoria", DataTypes.LongType, true),
                DataTypes.createStructField("typ", DataTypes.StringType, true),
                DataTypes.createStructField("model", DataTypes.StringType, true),
                DataTypes.createStructField("wariant", DataTypes.StringType, true),
                DataTypes.createStructField("wersja", DataTypes.StringType, true),
                DataTypes.createStructField("rodzaj", DataTypes.StringType, true),
                DataTypes.createStructField("podrodzaj", DataTypes.StringType, true),
                DataTypes.createStructField("przeznaczenie", DataTypes.StringType, true),
                DataTypes.createStructField("pochodzenie", DataTypes.StringType, true),
                DataTypes.createStructField("rodzaj_tab_znamionowej", DataTypes.StringType, true),
                DataTypes.createStructField("rok_produkcji", DataTypes.IntegerType, true),
                DataTypes.createStructField("sposob_produkcji", DataTypes.StringType, true),
                DataTypes.createStructField("data_pierwszej_rej", DataTypes.DateType, true),
                DataTypes.createStructField("data_rejestracji_ost", DataTypes.DateType, true),
                DataTypes.createStructField("data_pierwszej_rej_za_granica", DataTypes.DateType, true),
                DataTypes.createStructField("pojemnosc_silnika", DataTypes.DoubleType, true),
                DataTypes.createStructField("moc_do_masy", DataTypes.DoubleType, true),
                DataTypes.createStructField("moc_silnika", DataTypes.DoubleType, true),
                DataTypes.createStructField("moc_silnika_hybrydowego", DataTypes.DoubleType, true),
                DataTypes.createStructField("masa_wlasna", DataTypes.IntegerType, true),
                DataTypes.createStructField("masa_pgj", DataTypes.IntegerType, true),
                DataTypes.createStructField("dopuszczalna_masa_calkowita", DataTypes.IntegerType, true),
                DataTypes.createStructField("maksymalna_masa_calkowita", DataTypes.IntegerType, true),
                DataTypes.createStructField("dopuszczalna_ladownosc_calk", DataTypes.IntegerType, true),
                DataTypes.createStructField("maksymalna_ladownosc_calk", DataTypes.IntegerType, true),
                DataTypes.createStructField("dopuszczalna_masa_ciag_zesp", DataTypes.IntegerType, true),
                DataTypes.createStructField("liczba_osi", DataTypes.IntegerType, true),
                DataTypes.createStructField("naj_dopuszczalny_nacisk_osi", DataTypes.IntegerType, true),
                DataTypes.createStructField("naj_maksymalny_nacisk_osi", DataTypes.IntegerType, true),
                DataTypes.createStructField("max_masa_przyczepy_z_hamulcem", DataTypes.IntegerType, true),
                DataTypes.createStructField("max_masa_przyczepy_bez_ham", DataTypes.IntegerType, true),
                DataTypes.createStructField("liczba_miejsc_ogolem", DataTypes.IntegerType, true),
                DataTypes.createStructField("liczba_miejsc_siedzacych", DataTypes.IntegerType, true),
                DataTypes.createStructField("liczba_miejsc_stojacych", DataTypes.IntegerType, true),
                DataTypes.createStructField("rodzaj_paliwa", DataTypes.StringType, true),
                DataTypes.createStructField("rodzaj_paliwa_alternatywnego", DataTypes.StringType, true),
                DataTypes.createStructField("rodzaj_paliwa_alternatywnego2", DataTypes.StringType, true),
                DataTypes.createStructField("sr_zuzycie_pal", DataTypes.DoubleType, true),
                DataTypes.createStructField("rodzaj_zawieszenia", DataTypes.StringType, true),
                DataTypes.createStructField("radar", DataTypes.IntegerType, true),
                DataTypes.createStructField("hak", DataTypes.StringType, true),
                DataTypes.createStructField("kierownica_polozenie", DataTypes.StringType, true),
                DataTypes.createStructField("kierownica_z_prawej", DataTypes.StringType, true),
                DataTypes.createStructField("katalizator", DataTypes.StringType, true),
                DataTypes.createStructField("producent_podstawowy", DataTypes.StringType, true),
                DataTypes.createStructField("kod_ident", DataTypes.StringType, true),
                DataTypes.createStructField("rozstaw_osi_kierowanej", DataTypes.LongType, true),
                DataTypes.createStructField("rozstaw_kol_max", DataTypes.IntegerType, true),
                DataTypes.createStructField("rozstaw_kol_sred", DataTypes.DoubleType, true),
                DataTypes.createStructField("rozstaw_kol_min", DataTypes.IntegerType, true),
                DataTypes.createStructField("emisja_co2_redukcja", DataTypes.IntegerType, true),
                DataTypes.createStructField("wersja_rpp", DataTypes.IntegerType, true),
                DataTypes.createStructField("kod_rpp", DataTypes.IntegerType, true),
                DataTypes.createStructField("data_wyrejestrowania", DataTypes.DateType, true),
                DataTypes.createStructField("przyczyna_wyrejestrowania", DataTypes.StringType, true),
                DataTypes.createStructField("data_wprowadzenia_danych", DataTypes.DateType, true),
                DataTypes.createStructField("akt_miejsce_rej_wojwe", DataTypes.StringType, true),
                DataTypes.createStructField("akt_miejsce_rej_powiat", DataTypes.StringType, true),
                DataTypes.createStructField("akt_miejsce_rej_gmina", DataTypes.StringType, true),
                DataTypes.createStructField("siedziba_wlasciciela_woj", DataTypes.StringType, true),
                DataTypes.createStructField("siedziba_wlasciciela_pow", DataTypes.StringType, true),
                DataTypes.createStructField("siedziba_wlasciciela_gmina", DataTypes.StringType, true),
                DataTypes.createStructField("data_pierwszej_rej_w_kraju", DataTypes.DateType, true),
                DataTypes.createStructField("createtimestamp", DataTypes.DateType, true),
                DataTypes.createStructField("modifytimestamp", DataTypes.DateType, true),
                DataTypes.createStructField("siedziba_wlasciciela_woj_teryt", DataTypes.StringType, true),
                DataTypes.createStructField("akt_miejsce_rej_wojew_teryt", DataTypes.IntegerType, true),
                DataTypes.createStructField("emisja_co2", DataTypes.DoubleType, true),
                DataTypes.createStructField("emisja_co2_pal_alternatywne1", DataTypes.DoubleType, true)
        });
        //pobranie danych z 17 plikow csv (16 wojewodztw + pojazdy bez okreslonego wojewodztwa)
        this.wojewodztwo_dolnoslaskie = spark.read().format("csv")
                .option("sep", ",")
                .schema(schema)
                .option("header", "true")
                .load("data/pojazdy_02_2021-04-22.csv");

        this.wojewodztwo_kujawskopomorskie = spark.read().format("csv")
                .option("sep", ",")
                .schema(schema)
                .option("header", "true")
                .load("data/pojazdy_04_2021-04-22.csv");

        this.wojewodztwo_lubelskie = spark.read().format("csv")
                .option("sep", ",")
                .schema(schema)
                .option("header", "true")
                .load("data/pojazdy_06_2021-04-22.csv");

        this.wojewodztwo_lubuskie = spark.read().format("csv")
                .option("sep", ",")
                .schema(schema)
                .option("header", "true")
                .load("data/pojazdy_08_2021-04-22.csv");

        this.wojewodztwo_lodzkie = spark.read().format("csv")
                .option("sep", ",")
                .schema(schema)
                .option("header", "true")
                .load("data/pojazdy_10_2021-04-22.csv");

        this.wojewodztwo_malopolskie = spark.read().format("csv")
                .option("sep", ",")
                .schema(schema)
                .option("header", "true")
                .load("data/pojazdy_12_2021-04-22.csv");

        this.wojewodztwo_mazowieckie = spark.read().format("csv")
                .option("sep", ",")
                .schema(schema)
                .option("header", "true")
                .load("data/pojazdy_14_2021-04-22.csv");

        this.wojewodztwo_opolskie = spark.read().format("csv")
                .option("sep", ",")
                .schema(schema)
                .option("header", "true")
                .load("data/pojazdy_16_2021-04-22.csv");

        this.wojewodztwo_podkarpackie = spark.read().format("csv")
                .option("sep", ",")
                .schema(schema)
                .option("header", "true")
                .load("data/pojazdy_18_2021-04-22.csv");

        this.wojewodztwo_podlaskie = spark.read().format("csv")
                .option("sep", ",")
                .schema(schema)
                .option("header", "true")
                .load("data/pojazdy_20_2021-04-22.csv");

        this.wojewodztwo_pomorskie = spark.read().format("csv")
                .option("sep", ",")
                .schema(schema)
                .option("header", "true")
                .load("data/pojazdy_22_2021-04-22.csv");

        this.wojewodztwo_slaskie = spark.read().format("csv")
                .option("sep", ",")
                .schema(schema)
                .option("header", "true")
                .load("data/pojazdy_24_2021-04-22.csv");

        this.wojewodztwo_swietokrzyskie = spark.read().format("csv")
                .option("sep", ",")
                .schema(schema)
                .option("header", "true")
                .load("data/pojazdy_26_2021-04-22.csv");

        this.wojewodztwo_warminskomazurskie = spark.read().format("csv")
                .option("sep", ",")
                .schema(schema)
                .option("header", "true")
                .load("data/pojazdy_28_2021-04-22.csv");

        this.wojewodztwo_wielkopolskie = spark.read().format("csv")
                .option("sep", ",")
                .schema(schema)
                .option("header", "true")
                .load("data/pojazdy_30_2021-04-22.csv");

        this.wojewodztwo_zachodniopomorskie = spark.read().format("csv")
                .option("sep", ",")
                .schema(schema)
                .option("header", "true")
                .load("data/pojazdy_32_2021-04-22.csv");

        this.nieokreslone_wojewodztwo = spark.read().format("csv")
                .option("sep", ",")
                .schema(schema)
                .option("header", "true")
                .load("data/pojazdy_xx_2021-04-22.csv");
    }

    public Dataset<Row> getWojewodztwo_malopolskie() {
        return wojewodztwo_malopolskie;
    }

    public Dataset<Row> getWojewodztwo_mazowieckie() {
        return wojewodztwo_mazowieckie;
    }

    public Dataset<Row> getWojewodztwo_opolskie() {
        return wojewodztwo_opolskie;
    }

    public Dataset<Row> getWojewodztwo_podkarpackie() {
        return wojewodztwo_podkarpackie;
    }

    public Dataset<Row> getWojewodztwo_podlaskie() {
        return wojewodztwo_podlaskie;
    }

    public Dataset<Row> getWojewodztwo_pomorskie() {
        return wojewodztwo_pomorskie;
    }

    public Dataset<Row> getWojewodztwo_slaskie() {
        return wojewodztwo_slaskie;
    }

    public Dataset<Row> getWojewodztwo_dolnoslaskie() {
        return wojewodztwo_dolnoslaskie;
    }

    public Dataset<Row> getWojewodztwo_kujawskopomorskie() {
        return wojewodztwo_kujawskopomorskie;
    }

    public Dataset<Row> getWojewodztwo_swietokrzyskie() {
        return wojewodztwo_swietokrzyskie;
    }

    public Dataset<Row> getWojewodztwo_warminskomazurskie() {
        return wojewodztwo_warminskomazurskie;
    }

    public Dataset<Row> getWojewodztwo_wielkopolskie() {
        return wojewodztwo_wielkopolskie;
    }

    public Dataset<Row> getWojewodztwo_zachodniopomorskie() {
        return wojewodztwo_zachodniopomorskie;
    }

    public Dataset<Row> getWojewodztwo_lubelskie() {
        return wojewodztwo_lubelskie;
    }

    public Dataset<Row> getWojewodztwo_lubuskie() {
        return wojewodztwo_lubuskie;
    }

    public Dataset<Row> getWojewodztwo_lodzkie() {
        return wojewodztwo_lodzkie;
    }

    public Dataset<Row> getNieokreslone_wojewodztwo() {
        return nieokreslone_wojewodztwo;
    }

    public SparkSession getSpark() {
        return spark;
    }

    public StructType getSchema() {
        return schema;
    }
}
