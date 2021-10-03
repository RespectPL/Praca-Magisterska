package com.company;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.StructType;

import javax.swing.*;
import javax.swing.table.TableColumnModel;
import java.awt.*;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.apache.spark.sql.functions.col;

public class ClassificationCars {
    private CepikData cepikData;
    private SparkSession spark;
    private StructType schema;
    private Dataset<Row> samochody_osobowe;

    private Dataset<Row> samochody_male_miejskie;
    private Dataset<Row> samochody_kompaktowe;
    private Dataset<Row> samochody_rodzinne;
    private Dataset<Row> samochody_terenowe_limuzyny;
    private Dataset<Row> samochody_sportowe;

    public ClassificationCars(CepikData cepikData) {
        this.cepikData = cepikData;
        this.spark = cepikData.getSpark();
        this.schema = cepikData.getSchema();

        samochody_male_miejskie = spark.emptyDataFrame();
        samochody_kompaktowe = spark.emptyDataFrame();
        samochody_rodzinne = spark.emptyDataFrame();
        samochody_terenowe_limuzyny = spark.emptyDataFrame();
        samochody_sportowe = spark.emptyDataFrame();

        /*samochody_osobowe = cepikData.getWojewodztwo_dolnoslaskie()
                .filter(col("rodzaj").equalTo("SAMOCHÓD OSOBOWY"));

        samochody_osobowe = samochody_osobowe.unionByName(cepikData.getWojewodztwo_kujawskopomorskie()
                .filter(col("rodzaj").equalTo("SAMOCHÓD OSOBOWY")));

        samochody_osobowe = samochody_osobowe.unionByName(cepikData.getWojewodztwo_lubelskie()
                .filter(col("rodzaj").equalTo("SAMOCHÓD OSOBOWY")));

        samochody_osobowe = samochody_osobowe.unionByName(cepikData.getWojewodztwo_lubuskie()
                .filter(col("rodzaj").equalTo("SAMOCHÓD OSOBOWY")));

        samochody_osobowe = samochody_osobowe.unionByName(cepikData.getWojewodztwo_lodzkie()
                .filter(col("rodzaj").equalTo("SAMOCHÓD OSOBOWY")));

        samochody_osobowe = samochody_osobowe.unionByName(cepikData.getWojewodztwo_malopolskie()
                .filter(col("rodzaj").equalTo("SAMOCHÓD OSOBOWY")));

        samochody_osobowe = samochody_osobowe.unionByName(cepikData.getWojewodztwo_mazowieckie()
                .filter(col("rodzaj").equalTo("SAMOCHÓD OSOBOWY")));

        samochody_osobowe = samochody_osobowe.unionByName(cepikData.getWojewodztwo_opolskie()
                .filter(col("rodzaj").equalTo("SAMOCHÓD OSOBOWY")));

        samochody_osobowe = samochody_osobowe.unionByName(cepikData.getWojewodztwo_podkarpackie()
                .filter(col("rodzaj").equalTo("SAMOCHÓD OSOBOWY")));

        samochody_osobowe = samochody_osobowe.unionByName(cepikData.getWojewodztwo_podlaskie()
                .filter(col("rodzaj").equalTo("SAMOCHÓD OSOBOWY")));

        samochody_osobowe = samochody_osobowe.unionByName(cepikData.getWojewodztwo_pomorskie()
                .filter(col("rodzaj").equalTo("SAMOCHÓD OSOBOWY")));

        samochody_osobowe = samochody_osobowe.unionByName(cepikData.getWojewodztwo_slaskie()
                .filter(col("rodzaj").equalTo("SAMOCHÓD OSOBOWY")));

        samochody_osobowe = samochody_osobowe.unionByName(cepikData.getWojewodztwo_swietokrzyskie()
                .filter(col("rodzaj").equalTo("SAMOCHÓD OSOBOWY")));

        samochody_osobowe = samochody_osobowe.unionByName(cepikData.getWojewodztwo_warminskomazurskie()
                .filter(col("rodzaj").equalTo("SAMOCHÓD OSOBOWY")));

        samochody_osobowe = samochody_osobowe.unionByName(cepikData.getWojewodztwo_wielkopolskie()
                .filter(col("rodzaj").equalTo("SAMOCHÓD OSOBOWY")));

        samochody_osobowe = samochody_osobowe.unionByName(cepikData.getWojewodztwo_zachodniopomorskie()
                .filter(col("rodzaj").equalTo("SAMOCHÓD OSOBOWY")));

        samochody_osobowe = samochody_osobowe.unionByName(cepikData.getNieokreslone_wojewodztwo()
                .filter(col("rodzaj").equalTo("SAMOCHÓD OSOBOWY")));*/

        classifier(cepikData.getWojewodztwo_dolnoslaskie()
                .filter(col("rodzaj").equalTo("SAMOCHÓD OSOBOWY")));

        classifier(cepikData.getWojewodztwo_kujawskopomorskie()
                .filter(col("rodzaj").equalTo("SAMOCHÓD OSOBOWY")));

        classifier(cepikData.getWojewodztwo_lubelskie()
                .filter(col("rodzaj").equalTo("SAMOCHÓD OSOBOWY")));

        classifier(cepikData.getWojewodztwo_lubuskie()
                .filter(col("rodzaj").equalTo("SAMOCHÓD OSOBOWY")));

        classifier(cepikData.getWojewodztwo_lodzkie()
                .filter(col("rodzaj").equalTo("SAMOCHÓD OSOBOWY")));

        classifier(cepikData.getWojewodztwo_malopolskie()
                .filter(col("rodzaj").equalTo("SAMOCHÓD OSOBOWY")));

        classifier(cepikData.getWojewodztwo_mazowieckie()
                .filter(col("rodzaj").equalTo("SAMOCHÓD OSOBOWY")));

        classifier(cepikData.getWojewodztwo_opolskie()
                .filter(col("rodzaj").equalTo("SAMOCHÓD OSOBOWY")));

        classifier(cepikData.getWojewodztwo_podkarpackie()
                .filter(col("rodzaj").equalTo("SAMOCHÓD OSOBOWY")));

        classifier(cepikData.getWojewodztwo_podlaskie()
                .filter(col("rodzaj").equalTo("SAMOCHÓD OSOBOWY")));

        classifier(cepikData.getWojewodztwo_pomorskie()
                .filter(col("rodzaj").equalTo("SAMOCHÓD OSOBOWY")));

        classifier(cepikData.getWojewodztwo_slaskie()
                .filter(col("rodzaj").equalTo("SAMOCHÓD OSOBOWY")));

        classifier(cepikData.getWojewodztwo_swietokrzyskie()
                .filter(col("rodzaj").equalTo("SAMOCHÓD OSOBOWY")));

        classifier(cepikData.getWojewodztwo_warminskomazurskie()
                .filter(col("rodzaj").equalTo("SAMOCHÓD OSOBOWY")));

        classifier(cepikData.getWojewodztwo_wielkopolskie()
                .filter(col("rodzaj").equalTo("SAMOCHÓD OSOBOWY")));

        classifier(cepikData.getWojewodztwo_zachodniopomorskie()
                .filter(col("rodzaj").equalTo("SAMOCHÓD OSOBOWY")));

        classifier(cepikData.getNieokreslone_wojewodztwo()
                .filter(col("rodzaj").equalTo("SAMOCHÓD OSOBOWY")));
    }

    public void classifier(Dataset<Row> wojewodztwo) {
        List<Row> s_male_miejskie = new ArrayList<>();
        List<Row> s_kompaktowe = new ArrayList<>();
        List<Row> s_rodzinne = new ArrayList<>();
        List<Row> s_terenowe_limuzyny = new ArrayList<>();
        List<Row> s_sportowe = new ArrayList<>();

        List<Row> so = wojewodztwo
                .filter(col("marka").isNotNull()
                    .and(col("model").isNotNull()
                    .and(col("podrodzaj").isNotNull()
                    .and(col("pojemnosc_silnika").isNotNull()
                    .and(col("moc_silnika").isNotNull()
                    .and(col("masa_wlasna").isNotNull()))))))
                .limit(600000)
                .collectAsList();

        for(Row row : so) {
            if(row.get(row.fieldIndex("masa_wlasna")) != null &&
                    row.get(row.fieldIndex("pojemnosc_silnika")) != null &&
                    row.get(row.fieldIndex("podrodzaj")) != null &&
                    row.get(row.fieldIndex("moc_silnika")) != null) {
                if((int)row.get(row.fieldIndex("masa_wlasna")) >= 1100) {
                    if((double)row.get(row.fieldIndex("pojemnosc_silnika")) >= 1600.0) {
                        if(row.get(row.fieldIndex("podrodzaj")).equals("SEDAN")
                                || row.get(row.fieldIndex("podrodzaj")).equals("KOMBI")) {
                            s_rodzinne.add(row);
                        }
                        else {
                            if((double)row.get(row.fieldIndex("moc_silnika")) >= 110.32) {
                                if((int)row.get(row.fieldIndex("masa_wlasna")) >= 2000) {
                                    s_terenowe_limuzyny.add(row);
                                }
                                else { s_sportowe.add(row);}
                            }
                            else { s_rodzinne.add(row);}
                        }
                    }
                    else { s_kompaktowe.add(row);  }
                }
                else { s_male_miejskie.add(row); }
            }
        }

        if(samochody_male_miejskie.isEmpty())
            samochody_male_miejskie = spark.createDataFrame(s_male_miejskie, schema);
        else
            samochody_male_miejskie.union(spark.createDataFrame(s_male_miejskie, schema));

        if(samochody_kompaktowe.isEmpty())
            samochody_kompaktowe = spark.createDataFrame(s_kompaktowe, schema);
        else
            samochody_kompaktowe.union(spark.createDataFrame(s_kompaktowe, schema));

        if(samochody_rodzinne.isEmpty())
            samochody_rodzinne = spark.createDataFrame(s_rodzinne, schema);
        else
            samochody_rodzinne.union(spark.createDataFrame(s_rodzinne, schema));

        if(samochody_terenowe_limuzyny.isEmpty())
            samochody_terenowe_limuzyny = spark.createDataFrame(s_terenowe_limuzyny, schema);
        else
            samochody_terenowe_limuzyny.union(spark.createDataFrame(s_terenowe_limuzyny, schema));

        if(samochody_sportowe.isEmpty())
            samochody_sportowe = spark.createDataFrame(s_sportowe, schema);
        else
            samochody_sportowe.union(spark.createDataFrame(s_sportowe, schema));
    }

    public void most_popular_model_in_small_cars() {
        // SAMOCHODY MAŁE / MIEJSKIE
        List<Row> smm = samochody_male_miejskie
                .filter(col("marka").isNotNull())
                .filter(col("model").isNotNull())
                .groupBy("marka", "model").count()
                .sort(col("count").desc())
                .limit(10)
                .collectAsList();

        String[][] data_tabela = new String[10][3];

        String[] data_kolumny = { "LP.", "MARKA I MODEL", "ILOŚĆ" };

        int i = 0;
        for(Row row : smm) {
            data_tabela[i][0] = (i+1) + "";
            data_tabela[i][1] = row.get(0) + " " + row.get(1);
            data_tabela[i][2] = row.get(2) + "";
            i++;
        }

        JFrame frame = new JFrame("Samochody małe i miejskie - TOP 10");
        JTable table = new JTable(data_tabela, data_kolumny);
        table.setBounds(30,40,200,300);
        table.setRowHeight(25);
        TableColumnModel columnModel = table.getColumnModel();
        columnModel.getColumn(0).setPreferredWidth(70);
        columnModel.getColumn(1).setPreferredWidth(350);
        columnModel.getColumn(2).setPreferredWidth(180);
        table.setFont(new Font("Arial", Font.PLAIN, 14));
        table.setEnabled(false);
        JScrollPane sp = new JScrollPane(table);
        frame.add(sp);
        frame.setVisible(true);
        frame.setSize(600, 450);
    }

    public void most_popular_model_in_compact_car() {
        // SAMOCHODY KOMPAKTOWE
        List<Row> sk = samochody_kompaktowe
                .filter(col("marka").isNotNull())
                .filter(col("model").isNotNull())
                .groupBy("marka", "model").count()
                .sort(col("count").desc())
                .limit(10).collectAsList();

        String[][] data_tabela = new String[10][3];

        String[] data_kolumny = { "LP.", "MARKA I MODEL", "ILOŚĆ" };

        int i = 0;
        for(Row row : sk) {
            data_tabela[i][0] = (i+1) + "";
            data_tabela[i][1] = row.get(0) + " " + row.get(1);
            data_tabela[i][2] = row.get(2) + "";
            i++;
        }

        JFrame frame = new JFrame("Samochody kompaktowe - TOP 10");
        JTable table = new JTable(data_tabela, data_kolumny);
        table.setBounds(30,40,200,300);
        table.setRowHeight(25);
        TableColumnModel columnModel = table.getColumnModel();
        columnModel.getColumn(0).setPreferredWidth(70);
        columnModel.getColumn(1).setPreferredWidth(350);
        columnModel.getColumn(2).setPreferredWidth(180);
        table.setFont(new Font("Arial", Font.PLAIN, 14));
        table.setEnabled(false);
        JScrollPane sp = new JScrollPane(table);
        frame.add(sp);
        frame.setVisible(true);
        frame.setSize(600, 450);
    }

    public void most_popular_model_in_family_car() {
        // SAMOCHODY RODZINNE
        List<Row> sr = samochody_rodzinne
                .filter(col("marka").isNotNull())
                .filter(col("model").isNotNull())
                .groupBy("marka", "model").count()
                .sort(col("count").desc())
                .limit(10).collectAsList();

        String[][] data_tabela = new String[10][3];

        String[] data_kolumny = { "LP.", "MARKA I MODEL", "ILOŚĆ" };

        int i = 0;
        for(Row row : sr) {
            data_tabela[i][0] = (i+1) + "";
            data_tabela[i][1] = row.get(0) + " " + row.get(1);
            data_tabela[i][2] = row.get(2) + "";
            i++;
        }

        JFrame frame = new JFrame("Samochody rodzinne - TOP 10");
        JTable table = new JTable(data_tabela, data_kolumny);
        table.setBounds(30,40,200,300);
        table.setRowHeight(25);
        TableColumnModel columnModel = table.getColumnModel();
        columnModel.getColumn(0).setPreferredWidth(70);
        columnModel.getColumn(1).setPreferredWidth(350);
        columnModel.getColumn(2).setPreferredWidth(180);
        table.setFont(new Font("Arial", Font.PLAIN, 14));
        table.setEnabled(false);
        JScrollPane sp = new JScrollPane(table);
        frame.add(sp);
        frame.setVisible(true);
        frame.setSize(600, 450);
    }

    public void most_popular_model_in_offroad_and_limousine() {
        // SAMOCHODY TERENOWE I LIMUZYNY
        List<Row> stl = samochody_terenowe_limuzyny
                .filter(col("marka").isNotNull())
                .filter(col("model").isNotNull())
                .groupBy("marka", "model").count()
                .sort(col("count").desc())
                .limit(10).collectAsList();

        String[][] data_tabela = new String[10][3];

        String[] data_kolumny = { "LP.", "MARKA I MODEL", "ILOŚĆ" };

        int i = 0;
        for(Row row : stl) {
            data_tabela[i][0] = (i+1) + "";
            data_tabela[i][1] = row.get(0) + " " + row.get(1);
            data_tabela[i][2] = row.get(2) + "";
            i++;
        }

        JFrame frame = new JFrame("Samochody terenowe i limuzyny - TOP 10");
        JTable table = new JTable(data_tabela, data_kolumny);
        table.setBounds(30,40,200,300);
        table.setRowHeight(25);
        TableColumnModel columnModel = table.getColumnModel();
        columnModel.getColumn(0).setPreferredWidth(70);
        columnModel.getColumn(1).setPreferredWidth(350);
        columnModel.getColumn(2).setPreferredWidth(180);
        table.setFont(new Font("Arial", Font.PLAIN, 14));
        table.setEnabled(false);
        JScrollPane sp = new JScrollPane(table);
        frame.add(sp);
        frame.setVisible(true);
        frame.setSize(600, 450);

    }

    public void most_popular_model_in_sport_car() {
        // SAMOCHODY SPORTOWE
        List<Row> ss = samochody_sportowe
                .filter(col("marka").isNotNull())
                .filter(col("model").isNotNull())
                .groupBy("marka", "model").count()
                .sort(col("count").desc())
                .limit(10).collectAsList();

        String[][] data_tabela = new String[10][3];

        String[] data_kolumny = { "LP.", "MARKA I MODEL", "ILOŚĆ" };

        int i = 0;
        for(Row row : ss) {
            data_tabela[i][0] = (i+1) + "";
            data_tabela[i][1] = row.get(0) + " " + row.get(1);
            data_tabela[i][2] = row.get(2) + "";
            i++;
        }

        JFrame frame = new JFrame("Samochody sportowe - TOP 10");
        JTable table = new JTable(data_tabela, data_kolumny);
        table.setBounds(30,40,200,300);
        table.setRowHeight(25);
        TableColumnModel columnModel = table.getColumnModel();
        columnModel.getColumn(0).setPreferredWidth(70);
        columnModel.getColumn(1).setPreferredWidth(350);
        columnModel.getColumn(2).setPreferredWidth(180);
        table.setFont(new Font("Arial", Font.PLAIN, 14));
        table.setEnabled(false);
        JScrollPane sp = new JScrollPane(table);
        frame.add(sp);
        frame.setVisible(true);
        frame.setSize(600, 450);
    }
}
