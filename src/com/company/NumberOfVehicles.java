package com.company;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.jfree.chart.ChartFactory;
import org.jfree.chart.ChartPanel;
import org.jfree.chart.ChartUtilities;
import org.jfree.chart.JFreeChart;
import org.jfree.chart.labels.PieSectionLabelGenerator;
import org.jfree.chart.labels.StandardPieSectionLabelGenerator;
import org.jfree.chart.plot.CenterTextMode;
import org.jfree.chart.plot.RingPlot;
import org.jfree.data.general.DefaultPieDataset;

import javax.swing.*;

import java.awt.*;
import java.io.File;
import java.text.DecimalFormat;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.spark.sql.functions.col;

public class NumberOfVehicles {
    private CepikData cepikData;
    private long ilosc_wszystkich_pojazdow;
    private long ilosc_pojazdow_aktualnie_zarejestrowanych;
    private long ilosc_pojazdow_wyrejestrowanych;
    private Map<String, Long> ilosc_pojazdow_w_wojewodztwach;
    private Map<String, Long> ilosc_aktual_zarej_poj_wojew;
    private Map<String, Long> ilosc_wyrej_poj_wojew;

    public NumberOfVehicles(CepikData cepikData) {
        this.cepikData = cepikData;
        this.ilosc_pojazdow_w_wojewodztwach = new HashMap<String, Long>();
        this.ilosc_aktual_zarej_poj_wojew = new HashMap<String, Long>();
        this.ilosc_wyrej_poj_wojew = new HashMap<String, Long>();
        count_number_of_vehicles_in_voivodeships();
        this.ilosc_wszystkich_pojazdow = this.ilosc_pojazdow_w_wojewodztwach.values().stream().mapToLong(Long::longValue).sum();
        this.ilosc_pojazdow_aktualnie_zarejestrowanych = this.ilosc_aktual_zarej_poj_wojew.values().stream().mapToLong(Long::longValue).sum();
        this.ilosc_pojazdow_wyrejestrowanych = this.ilosc_wszystkich_pojazdow - this.ilosc_pojazdow_aktualnie_zarejestrowanych;
    }

    /*public NumberOfVehicles(SparkSession spark) {
        this.cepikData = new CepikData(spark);
        this.ilosc_pojazdow_w_wojewodztwach = new HashMap<String, Long>();
        this.ilosc_aktual_zarej_poj_wojew = new HashMap<String, Long>();
        this.ilosc_wyrej_poj_wojew = new HashMap<String, Long>();
        count_number_of_vehicles_in_voivodeships();
        this.ilosc_wszystkich_pojazdow = this.ilosc_pojazdow_w_wojewodztwach.values().stream().mapToLong(Long::longValue).sum();
        this.ilosc_pojazdow_aktualnie_zarejestrowanych = this.ilosc_aktual_zarej_poj_wojew.values().stream().mapToLong(Long::longValue).sum();
        this.ilosc_pojazdow_wyrejestrowanych = this.ilosc_wszystkich_pojazdow - this.ilosc_pojazdow_aktualnie_zarejestrowanych;
    }*/

    public void count_number_of_vehicles_in_voivodeships() {
        // ilosc wszystkich pojazdow w bazie CEPiK w poszczegolnych wojewodztwach
        ilosc_pojazdow_w_wojewodztwach.put("dolnoslaskie", cepikData.getWojewodztwo_dolnoslaskie().count());
        ilosc_pojazdow_w_wojewodztwach.put("kujawskopomorskie", cepikData.getWojewodztwo_kujawskopomorskie().count());
        ilosc_pojazdow_w_wojewodztwach.put("lubelskie", cepikData.getWojewodztwo_lubelskie().count());
        ilosc_pojazdow_w_wojewodztwach.put("lubuskie", cepikData.getWojewodztwo_lubuskie().count());
        ilosc_pojazdow_w_wojewodztwach.put("lodzkie", cepikData.getWojewodztwo_lodzkie().count());
        ilosc_pojazdow_w_wojewodztwach.put("malopolskie", cepikData.getWojewodztwo_malopolskie().count());
        ilosc_pojazdow_w_wojewodztwach.put("mazowieckie", cepikData.getWojewodztwo_mazowieckie().count());
        ilosc_pojazdow_w_wojewodztwach.put("opolskie", cepikData.getWojewodztwo_opolskie().count());
        ilosc_pojazdow_w_wojewodztwach.put("podkarpackie", cepikData.getWojewodztwo_podkarpackie().count());
        ilosc_pojazdow_w_wojewodztwach.put("podlaskie", cepikData.getWojewodztwo_podlaskie().count());
        ilosc_pojazdow_w_wojewodztwach.put("pomorskie", cepikData.getWojewodztwo_pomorskie().count());
        ilosc_pojazdow_w_wojewodztwach.put("slaskie", cepikData.getWojewodztwo_slaskie().count());
        ilosc_pojazdow_w_wojewodztwach.put("swietokrzyskie", cepikData.getWojewodztwo_swietokrzyskie().count());
        ilosc_pojazdow_w_wojewodztwach.put("warminskomazurskie", cepikData.getWojewodztwo_warminskomazurskie().count());
        ilosc_pojazdow_w_wojewodztwach.put("wielkopolskie", cepikData.getWojewodztwo_wielkopolskie().count());
        ilosc_pojazdow_w_wojewodztwach.put("zachodniopomorskie", cepikData.getWojewodztwo_zachodniopomorskie().count());
        ilosc_pojazdow_w_wojewodztwach.put("nieokreslone", cepikData.getNieokreslone_wojewodztwo().count());

        // ilosc aktualnie zarejestrowanych pojazdow (niewyrejestrowanych) w poszczegolnych wojewodztwach
        ilosc_aktual_zarej_poj_wojew.put("dolnoslaskie", cepikData.getWojewodztwo_dolnoslaskie().filter(col("data_wyrejestrowania").isNull()).count());
        ilosc_aktual_zarej_poj_wojew.put("kujawskopomorskie", cepikData.getWojewodztwo_kujawskopomorskie().filter(col("data_wyrejestrowania").isNull()).count());
        ilosc_aktual_zarej_poj_wojew.put("lubelskie", cepikData.getWojewodztwo_lubelskie().filter(col("data_wyrejestrowania").isNull()).count());
        ilosc_aktual_zarej_poj_wojew.put("lubuskie", cepikData.getWojewodztwo_lubuskie().filter(col("data_wyrejestrowania").isNull()).count());
        ilosc_aktual_zarej_poj_wojew.put("lodzkie", cepikData.getWojewodztwo_lodzkie().filter(col("data_wyrejestrowania").isNull()).count());
        ilosc_aktual_zarej_poj_wojew.put("malopolskie", cepikData.getWojewodztwo_malopolskie().filter(col("data_wyrejestrowania").isNull()).count());
        ilosc_aktual_zarej_poj_wojew.put("mazowieckie", cepikData.getWojewodztwo_mazowieckie().filter(col("data_wyrejestrowania").isNull()).count());
        ilosc_aktual_zarej_poj_wojew.put("opolskie", cepikData.getWojewodztwo_opolskie().filter(col("data_wyrejestrowania").isNull()).count());
        ilosc_aktual_zarej_poj_wojew.put("podkarpackie", cepikData.getWojewodztwo_podkarpackie().filter(col("data_wyrejestrowania").isNull()).count());
        ilosc_aktual_zarej_poj_wojew.put("podlaskie", cepikData.getWojewodztwo_podlaskie().filter(col("data_wyrejestrowania").isNull()).count());
        ilosc_aktual_zarej_poj_wojew.put("pomorskie", cepikData.getWojewodztwo_pomorskie().filter(col("data_wyrejestrowania").isNull()).count());
        ilosc_aktual_zarej_poj_wojew.put("slaskie", cepikData.getWojewodztwo_slaskie().filter(col("data_wyrejestrowania").isNull()).count());
        ilosc_aktual_zarej_poj_wojew.put("swietokrzyskie", cepikData.getWojewodztwo_swietokrzyskie().filter(col("data_wyrejestrowania").isNull()).count());
        ilosc_aktual_zarej_poj_wojew.put("warminskomazurskie", cepikData.getWojewodztwo_warminskomazurskie().filter(col("data_wyrejestrowania").isNull()).count());
        ilosc_aktual_zarej_poj_wojew.put("wielkopolskie", cepikData.getWojewodztwo_wielkopolskie().filter(col("data_wyrejestrowania").isNull()).count());
        ilosc_aktual_zarej_poj_wojew.put("zachodniopomorskie", cepikData.getWojewodztwo_zachodniopomorskie().filter(col("data_wyrejestrowania").isNull()).count());
        ilosc_aktual_zarej_poj_wojew.put("nieokreslone", cepikData.getNieokreslone_wojewodztwo().filter(col("data_wyrejestrowania").isNull()).count());

        // ilosc wyrejestrowanych pojazdow w poszczegolnych wojewodztwach
        ilosc_wyrej_poj_wojew.put("dolnoslaskie", (ilosc_pojazdow_w_wojewodztwach.get("dolnoslaskie") - ilosc_aktual_zarej_poj_wojew.get("dolnoslaskie")));
        ilosc_wyrej_poj_wojew.put("kujawskopomorskie", (ilosc_pojazdow_w_wojewodztwach.get("kujawskopomorskie") - ilosc_aktual_zarej_poj_wojew.get("kujawskopomorskie")));
        ilosc_wyrej_poj_wojew.put("lubelskie", (ilosc_pojazdow_w_wojewodztwach.get("lubelskie") - ilosc_aktual_zarej_poj_wojew.get("lubelskie")));
        ilosc_wyrej_poj_wojew.put("lubuskie", (ilosc_pojazdow_w_wojewodztwach.get("lubuskie") - ilosc_aktual_zarej_poj_wojew.get("lubuskie")));
        ilosc_wyrej_poj_wojew.put("lodzkie", (ilosc_pojazdow_w_wojewodztwach.get("lodzkie") - ilosc_aktual_zarej_poj_wojew.get("lodzkie")));
        ilosc_wyrej_poj_wojew.put("malopolskie", (ilosc_pojazdow_w_wojewodztwach.get("malopolskie") - ilosc_aktual_zarej_poj_wojew.get("malopolskie")));
        ilosc_wyrej_poj_wojew.put("mazowieckie", (ilosc_pojazdow_w_wojewodztwach.get("mazowieckie") - ilosc_aktual_zarej_poj_wojew.get("mazowieckie")));
        ilosc_wyrej_poj_wojew.put("opolskie", (ilosc_pojazdow_w_wojewodztwach.get("opolskie") - ilosc_aktual_zarej_poj_wojew.get("opolskie")));
        ilosc_wyrej_poj_wojew.put("podkarpackie", (ilosc_pojazdow_w_wojewodztwach.get("podkarpackie") - ilosc_aktual_zarej_poj_wojew.get("podkarpackie")));
        ilosc_wyrej_poj_wojew.put("podlaskie", (ilosc_pojazdow_w_wojewodztwach.get("podlaskie") - ilosc_aktual_zarej_poj_wojew.get("podlaskie")));
        ilosc_wyrej_poj_wojew.put("pomorskie", (ilosc_pojazdow_w_wojewodztwach.get("pomorskie") - ilosc_aktual_zarej_poj_wojew.get("pomorskie")));
        ilosc_wyrej_poj_wojew.put("slaskie", (ilosc_pojazdow_w_wojewodztwach.get("slaskie") - ilosc_aktual_zarej_poj_wojew.get("slaskie")));
        ilosc_wyrej_poj_wojew.put("swietokrzyskie", (ilosc_pojazdow_w_wojewodztwach.get("swietokrzyskie") - ilosc_aktual_zarej_poj_wojew.get("swietokrzyskie")));
        ilosc_wyrej_poj_wojew.put("warminskomazurskie", (ilosc_pojazdow_w_wojewodztwach.get("warminskomazurskie") - ilosc_aktual_zarej_poj_wojew.get("warminskomazurskie")));
        ilosc_wyrej_poj_wojew.put("wielkopolskie", (ilosc_pojazdow_w_wojewodztwach.get("wielkopolskie") - ilosc_aktual_zarej_poj_wojew.get("wielkopolskie")));
        ilosc_wyrej_poj_wojew.put("zachodniopomorskie", (ilosc_pojazdow_w_wojewodztwach.get("zachodniopomorskie") - ilosc_aktual_zarej_poj_wojew.get("zachodniopomorskie")));
        ilosc_wyrej_poj_wojew.put("nieokreslone", (ilosc_pojazdow_w_wojewodztwach.get("nieokreslone") - ilosc_aktual_zarej_poj_wojew.get("nieokreslone")));
    }

    public void info_about_all_registered_vehicles_in_poland() {
        /*JOptionPane.showMessageDialog(null,
                "Ilość wszystkich zarejestrowanych pojazdów w Polsce wynosi: " +
                        ilosc_wszystkich_pojazdow + "\n" +
                        "Ilość aktualnie zarejestrowanych pojazdów w Polsce wynosi: " +
                        ilosc_pojazdow_aktualnie_zarejestrowanych + "\n" +
                        "Ilość wyrejestrowanych pojazdów w Polsce wynosi: " +
                        ilosc_pojazdow_wyrejestrowanych);*/
        DefaultPieDataset dpd = new DefaultPieDataset();
        dpd.setValue("Aktualnie zarejestrowane", ilosc_pojazdow_aktualnie_zarejestrowanych);
        dpd.setValue("Wyrejestrowane", ilosc_pojazdow_wyrejestrowanych);

        JFreeChart chart = ChartFactory.createRingChart(
                "Wszystkie pojazdy w Polsce",
                dpd,
                true,
                true,
                false
        );

        RingPlot plot = (RingPlot) chart.getPlot();
        plot.setSectionPaint("Aktualnie zarejestrowane", Color.decode("#0096ff"));
        plot.setSectionPaint("Wyrejestrowane", Color.RED);
        plot.setCenterTextMode(CenterTextMode.FIXED);
        plot.setCenterTextFont(plot.getCenterTextFont().deriveFont(Font.BOLD, 30f));
        plot.setCenterTextColor(Color.BLACK);
        plot.setCenterText(ilosc_wszystkich_pojazdow + "");
        plot.setBackgroundPaint(Color.WHITE);
        plot.setSimpleLabels(true);

        PieSectionLabelGenerator gen = new StandardPieSectionLabelGenerator(
                "{0}: {1}", new DecimalFormat("0"), new DecimalFormat("0%"));
        plot.setLabelGenerator(gen);

        try {
            ChartUtilities.saveChartAsPNG(
                    new File("F:\\II stopień\\Semestr IV\\Praca magisterska\\Analiza danych\\pojazdy_polska.png"),
                    chart,
                    500,
                    400);
        } catch (Exception e) {
            System.out.println("Wystapil problem podczas tworzenia wykresu");
        }

        JFrame frame = new JFrame("Wszystkie pojazdy w Polsce");
        frame.setLayout(new FlowLayout());
        frame.getContentPane().add(new ChartPanel(chart));
        frame.pack();
        frame.setVisible(true);
    }

    public void info_about_types_of_vehicles_in_poland() {
        Dataset<Row> pojazdy = cepikData.getWojewodztwo_dolnoslaskie()
                .unionByName(cepikData.getWojewodztwo_kujawskopomorskie())
                .unionByName(cepikData.getWojewodztwo_lubelskie())
                .unionByName(cepikData.getWojewodztwo_lubuskie())
                .unionByName(cepikData.getWojewodztwo_lodzkie())
                .unionByName(cepikData.getWojewodztwo_malopolskie())
                .unionByName(cepikData.getWojewodztwo_mazowieckie())
                .unionByName(cepikData.getWojewodztwo_opolskie())
                .unionByName(cepikData.getWojewodztwo_podkarpackie())
                .unionByName(cepikData.getWojewodztwo_podlaskie())
                .unionByName(cepikData.getWojewodztwo_pomorskie())
                .unionByName(cepikData.getWojewodztwo_slaskie())
                .unionByName(cepikData.getWojewodztwo_swietokrzyskie())
                .unionByName(cepikData.getWojewodztwo_warminskomazurskie())
                .unionByName(cepikData.getWojewodztwo_wielkopolskie())
                .unionByName(cepikData.getWojewodztwo_zachodniopomorskie())
                .unionByName(cepikData.getNieokreslone_wojewodztwo());

        pojazdy = pojazdy.filter(col("rodzaj").isNotNull())
                .filter(col("rodzaj").equalTo("SAMOCHÓD OSOBOWY")
                    .or(col("rodzaj").equalTo("SAMOCHÓD CIĘŻAROWY")
                    .or(col("rodzaj").equalTo("CIĄGNIK ROLNICZY")
                    .or(col("rodzaj").equalTo("MOTOCYKL")
                    .or(col("rodzaj").equalTo("MOTOROWER")
                    .or(col("rodzaj").equalTo("AUTOBUS")
                    .or(col("rodzaj").equalTo("SAMOCHÓD SPECJALNY"))))))));

        List<Row> podzial = pojazdy.groupBy("rodzaj").count().sort(col("count").desc())
                .collectAsList();

        String[][] data_tabela = new String[7][2];

        String[] data_kolumny = { "RODZAJ POJAZDU", "ILOŚĆ" };

        int i = 0;
        for(Row row : podzial) {
            data_tabela[i][0] = (String)row.get(0);
            data_tabela[i][1] = row.get(1) + "";
            i++;
        }

        JFrame frame = new JFrame("Ilość pojazdów poszczególnych rodzajów");
        JTable table = new JTable(data_tabela, data_kolumny);
        table.setBounds(30,40,200,300);
        table.setRowHeight(25);
        table.setFont(new Font("Arial", Font.PLAIN, 14));
        table.setEnabled(false);
        JScrollPane sp = new JScrollPane(table);
        frame.add(sp);
        frame.setVisible(true);
        frame.setSize(500, 400);
    }

    public long getIlosc_wszystkich_pojazdow() {
        return ilosc_wszystkich_pojazdow;
    }

    public long getIlosc_pojazdow_aktualnie_zarejestrowanych() {
        return ilosc_pojazdow_aktualnie_zarejestrowanych;
    }

    public long getIlosc_pojazdow_wyrejestrowanych() {
        return ilosc_pojazdow_wyrejestrowanych;
    }

    public Map<String, Long> getIlosc_pojazdow_w_wojewodztwach() {
        return ilosc_pojazdow_w_wojewodztwach;
    }

    public Map<String, Long> getIlosc_aktual_zarej_poj_wojew() {
        return ilosc_aktual_zarej_poj_wojew;
    }

    public Map<String, Long> getIlosc_wyrej_poj_wojew() {
        return ilosc_wyrej_poj_wojew;
    }

    public CepikData getCepikData() {
        return cepikData;
    }
}
