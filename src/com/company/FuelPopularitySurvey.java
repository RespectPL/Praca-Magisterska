package com.company;

import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.jfree.chart.ChartFactory;
import org.jfree.chart.ChartPanel;
import org.jfree.chart.ChartUtilities;
import org.jfree.chart.JFreeChart;
import org.jfree.chart.labels.PieSectionLabelGenerator;
import org.jfree.chart.labels.StandardPieSectionLabelGenerator;
import org.jfree.chart.plot.PiePlot;
import org.jfree.data.general.DefaultPieDataset;

import javax.swing.*;
import java.awt.*;
import java.io.File;
import java.text.DecimalFormat;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.spark.sql.functions.col;

public class FuelPopularitySurvey {
    private CepikData cepikData;
    private Map<String, Map<String, Long>> wszystkieDaneZWojewodztw;

    /*ublic FuelPopularitySurvey(SparkSession spark) {
        this.cepikData = new CepikData(spark);
        this.wszystkieDaneZWojewodztw = new HashMap<String, Map<String, Long>>();
    }*/

    public FuelPopularitySurvey(CepikData cepikData) {
        this.cepikData = cepikData;
        this.wszystkieDaneZWojewodztw = new HashMap<String, Map<String, Long>>();
    }

    public void popularity_of_fuels_woj_lubelskie() {
        List<Row> lubelskie = cepikData.getWojewodztwo_lubelskie()
                .filter(col("rodzaj").equalTo("SAMOCHÓD OSOBOWY")
                        .and(col("data_wyrejestrowania").isNull()))
                .filter(col("rodzaj_paliwa").isNotNull())
                .filter(col("rodzaj_paliwa").like("%BENZYNA%")
                        .or(col("rodzaj_paliwa").like("%OLEJ NAPĘDOWY%")))
                .filter(col("rodzaj_paliwa_alternatywnego").like("%GAZ PŁYNNY%")
                        .or(col("rodzaj_paliwa_alternatywnego").isNull()))
                .groupBy("rodzaj_paliwa","rodzaj_paliwa_alternatywnego")
                .count()
                .sort(col("count").desc())
                .limit(3)
                .collectAsList();

        DefaultPieDataset dpd = new DefaultPieDataset();

        Map<String, Long> dane_z_wojewodztwa = new HashMap<String, Long>();

        for(Row row : lubelskie) {
            if(row.get(1) == null) {
                dpd.setValue((String)row.get(0), (long)row.get(2));
                dane_z_wojewodztwa.put((String)row.get(0), (long)row.get(2));
            }
            else {
                dpd.setValue((row.get(0) + " + LPG"), (long)row.get(2));
                dane_z_wojewodztwa.put((row.get(0) + " + LPG"), (long)row.get(2));
            }
        }

        wszystkieDaneZWojewodztw.put("lubelskie", dane_z_wojewodztwa);

        JFreeChart chart = ChartFactory.createPieChart(
                "Porównanie popularności rodzajów paliwa - województwo lubelskie",
                dpd,
                true,
                true,
                false
        );

        PiePlot plot = (PiePlot) chart.getPlot();
        plot.setSectionPaint("OLEJ NAPĘDOWY", Color.BLACK);
        plot.setSectionPaint("BENZYNA", Color.decode("#0096ff"));
        plot.setSectionPaint("BENZYNA + LPG", Color.YELLOW);
        plot.setForegroundAlpha(0.5f);
        plot.setExplodePercent("OLEJ NAPĘDOWY", 0.20);
        plot.setBackgroundPaint(Color.WHITE);
        plot.setSimpleLabels(true);

        PieSectionLabelGenerator gen = new StandardPieSectionLabelGenerator(
                "{0}: {1} ({2})", new DecimalFormat("0"), new DecimalFormat("0%"));
        plot.setLabelGenerator(gen);

        try {
            ChartUtilities.saveChartAsPNG(new File("F:\\II stopień\\Semestr IV\\Praca magisterska\\Analiza danych\\fuel_research\\fuel_lubelskie.png"),
                    chart, 500, 400);
        } catch (Exception e) {
            System.out.println("Wystapil problem podczas tworzenia wykresu");
        }

        JFrame frame = new JFrame("Popularność poszczególnych rodzajów paliwa " +
                "- województwo lubelskie");
        frame.setLayout(new FlowLayout());
        frame.getContentPane().add(new ChartPanel(chart));
        frame.pack();
        frame.setVisible(true);
    }

    public void popularity_of_fuels_woj_dolnoslaskie() {
        List<Row> dolnoslaskie = cepikData.getWojewodztwo_dolnoslaskie()
                .filter(col("rodzaj").equalTo("SAMOCHÓD OSOBOWY")
                        .and(col("data_wyrejestrowania").isNull()))
                .filter(col("rodzaj_paliwa").isNotNull())
                .filter(col("rodzaj_paliwa").like("%BENZYNA%")
                        .or(col("rodzaj_paliwa").like("%OLEJ NAPĘDOWY%")))
                .filter(col("rodzaj_paliwa_alternatywnego").like("%GAZ PŁYNNY%")
                        .or(col("rodzaj_paliwa_alternatywnego").isNull()))
                .groupBy("rodzaj_paliwa","rodzaj_paliwa_alternatywnego")
                .count()
                .sort(col("count").desc())
                .limit(3)
                .collectAsList();

        DefaultPieDataset dpd = new DefaultPieDataset();

        Map<String, Long> dane_z_wojewodztwa = new HashMap<String, Long>();

        for(Row row : dolnoslaskie) {
            if(row.get(1) == null) {
                dpd.setValue((String)row.get(0), (long)row.get(2));
                dane_z_wojewodztwa.put((String)row.get(0), (long)row.get(2));
            }
            else {
                dpd.setValue((row.get(0) + " + LPG"), (long)row.get(2));
                dane_z_wojewodztwa.put((row.get(0) + " + LPG"), (long)row.get(2));
            }
        }

        wszystkieDaneZWojewodztw.put("dolnoslaskie", dane_z_wojewodztwa);

        JFreeChart chart = ChartFactory.createPieChart(
                "Porównanie popularności rodzajów paliwa - województwo dolnośląskie",
                dpd,
                true,
                true,
                false
        );

        PiePlot plot = (PiePlot) chart.getPlot();
        plot.setSectionPaint("OLEJ NAPĘDOWY", Color.BLACK);
        plot.setSectionPaint("BENZYNA", Color.decode("#0096ff"));
        plot.setSectionPaint("BENZYNA + LPG", Color.YELLOW);
        plot.setForegroundAlpha(0.5f);
        plot.setExplodePercent("BENZYNA", 0.20);
        plot.setBackgroundPaint(Color.WHITE);
        plot.setSimpleLabels(true);

        PieSectionLabelGenerator gen = new StandardPieSectionLabelGenerator(
                "{0}: {1} ({2})", new DecimalFormat("0"), new DecimalFormat("0%"));
        plot.setLabelGenerator(gen);

        try {
            ChartUtilities.saveChartAsPNG(new File("F:\\II stopień\\Semestr IV\\Praca magisterska\\Analiza danych\\fuel_research\\fuel_dolnoslaskie.png"),
                    chart, 500, 400);
        } catch (Exception e) {
            System.out.println("Wystapil problem podczas tworzenia wykresu");
        }

        JFrame frame = new JFrame("Popularność poszczególnych rodzajów paliwa " +
                "- województwo dolnośląskie");
        frame.setLayout(new FlowLayout());
        frame.getContentPane().add(new ChartPanel(chart));
        frame.pack();
        frame.setVisible(true);
    }

    public void popularity_of_fuels_woj_kujawskopomorskie() {
        List<Row> kujawskopomorskie = cepikData.getWojewodztwo_kujawskopomorskie()
                .filter(col("rodzaj").equalTo("SAMOCHÓD OSOBOWY")
                        .and(col("data_wyrejestrowania").isNull()))
                .filter(col("rodzaj_paliwa").isNotNull())
                .filter(col("rodzaj_paliwa").like("%BENZYNA%")
                        .or(col("rodzaj_paliwa").like("%OLEJ NAPĘDOWY%")))
                .filter(col("rodzaj_paliwa_alternatywnego").like("%GAZ PŁYNNY%")
                        .or(col("rodzaj_paliwa_alternatywnego").isNull()))
                .groupBy("rodzaj_paliwa","rodzaj_paliwa_alternatywnego")
                .count()
                .sort(col("count").desc())
                .limit(3)
                .collectAsList();

        DefaultPieDataset dpd = new DefaultPieDataset();

        Map<String, Long> dane_z_wojewodztwa = new HashMap<String, Long>();

        for(Row row : kujawskopomorskie) {
            if(row.get(1) == null) {
                dpd.setValue((String)row.get(0), (long)row.get(2));
                dane_z_wojewodztwa.put((String)row.get(0), (long)row.get(2));
            }
            else {
                dpd.setValue((row.get(0) + " + LPG"), (long)row.get(2));
                dane_z_wojewodztwa.put((row.get(0) + " + LPG"), (long)row.get(2));
            }
        }

        wszystkieDaneZWojewodztw.put("kujawskopomorskie", dane_z_wojewodztwa);

        JFreeChart chart = ChartFactory.createPieChart(
                "Porównanie popularności rodzajów paliwa - województwo kujawsko-pomorskie",
                dpd,
                true,
                true,
                false
        );

        PiePlot plot = (PiePlot) chart.getPlot();
        plot.setSectionPaint("OLEJ NAPĘDOWY", Color.BLACK);
        plot.setSectionPaint("BENZYNA", Color.decode("#0096ff"));
        plot.setSectionPaint("BENZYNA + LPG", Color.YELLOW);
        plot.setForegroundAlpha(0.5f);
        plot.setExplodePercent("BENZYNA", 0.20);
        plot.setBackgroundPaint(Color.WHITE);
        plot.setSimpleLabels(true);

        PieSectionLabelGenerator gen = new StandardPieSectionLabelGenerator(
                "{0}: {1} ({2})", new DecimalFormat("0"), new DecimalFormat("0%"));
        plot.setLabelGenerator(gen);

        try {
            ChartUtilities.saveChartAsPNG(new File("F:\\II stopień\\Semestr IV\\Praca magisterska\\Analiza danych\\fuel_research\\fuel_kujawskopomorskie.png"),
                    chart, 500, 400);
        } catch (Exception e) {
            System.out.println("Wystapil problem podczas tworzenia wykresu");
        }

        JFrame frame = new JFrame("Popularność poszczególnych rodzajów paliwa " +
                "- województwo kujawsko-pomorskie");
        frame.setLayout(new FlowLayout());
        frame.getContentPane().add(new ChartPanel(chart));
        frame.pack();
        frame.setVisible(true);
    }

    public void popularity_of_fuels_woj_podlaskie() {
        List<Row> podlaskie = cepikData.getWojewodztwo_podlaskie()
                .filter(col("rodzaj").equalTo("SAMOCHÓD OSOBOWY")
                        .and(col("data_wyrejestrowania").isNull()))
                .filter(col("rodzaj_paliwa").isNotNull())
                .filter(col("rodzaj_paliwa").like("%BENZYNA%")
                        .or(col("rodzaj_paliwa").like("%OLEJ NAPĘDOWY%")))
                .filter(col("rodzaj_paliwa_alternatywnego").like("%GAZ PŁYNNY%")
                        .or(col("rodzaj_paliwa_alternatywnego").isNull()))
                .groupBy("rodzaj_paliwa","rodzaj_paliwa_alternatywnego")
                .count()
                .sort(col("count").desc())
                .limit(3)
                .collectAsList();

        DefaultPieDataset dpd = new DefaultPieDataset();

        Map<String, Long> dane_z_wojewodztwa = new HashMap<String, Long>();

        for(Row row : podlaskie) {
            if(row.get(1) == null) {
                dpd.setValue((String)row.get(0), (long)row.get(2));
                dane_z_wojewodztwa.put((String)row.get(0), (long)row.get(2));
            }
            else {
                dpd.setValue((row.get(0) + " + LPG"), (long)row.get(2));
                dane_z_wojewodztwa.put((row.get(0) + " + LPG"), (long)row.get(2));
            }
        }

        wszystkieDaneZWojewodztw.put("podlaskie", dane_z_wojewodztwa);

        JFreeChart chart = ChartFactory.createPieChart(
                "Porównanie popularności rodzajów paliwa - województwo podlaskie",
                dpd,
                true,
                true,
                false
        );

        PiePlot plot = (PiePlot) chart.getPlot();
        plot.setSectionPaint("OLEJ NAPĘDOWY", Color.BLACK);
        plot.setSectionPaint("BENZYNA", Color.decode("#0096ff"));
        plot.setSectionPaint("BENZYNA + LPG", Color.YELLOW);
        plot.setForegroundAlpha(0.5f);
        plot.setExplodePercent("BENZYNA", 0.20);
        plot.setBackgroundPaint(Color.WHITE);
        plot.setSimpleLabels(true);

        PieSectionLabelGenerator gen = new StandardPieSectionLabelGenerator(
                "{0}: {1} ({2})", new DecimalFormat("0"), new DecimalFormat("0%"));
        plot.setLabelGenerator(gen);

        try {
            ChartUtilities.saveChartAsPNG(new File("F:\\II stopień\\Semestr IV\\Praca magisterska\\Analiza danych\\fuel_research\\fuel_podlaskie.png"),
                    chart, 500, 400);
        } catch (Exception e) {
            System.out.println("Wystapil problem podczas tworzenia wykresu");
        }

        JFrame frame = new JFrame("Popularność poszczególnych rodzajów paliwa " +
                "- województwo podlaskie");
        frame.setLayout(new FlowLayout());
        frame.getContentPane().add(new ChartPanel(chart));
        frame.pack();
        frame.setVisible(true);
    }

    public void popularity_of_fuels_woj_lubuskie() {
        List<Row> lubuskie = cepikData.getWojewodztwo_lubuskie()
                .filter(col("rodzaj").equalTo("SAMOCHÓD OSOBOWY")
                        .and(col("data_wyrejestrowania").isNull()))
                .filter(col("rodzaj_paliwa").isNotNull())
                .filter(col("rodzaj_paliwa").like("%BENZYNA%")
                        .or(col("rodzaj_paliwa").like("%OLEJ NAPĘDOWY%")))
                .filter(col("rodzaj_paliwa_alternatywnego").like("%GAZ PŁYNNY%")
                        .or(col("rodzaj_paliwa_alternatywnego").isNull()))
                .groupBy("rodzaj_paliwa","rodzaj_paliwa_alternatywnego")
                .count()
                .sort(col("count").desc())
                .limit(3)
                .collectAsList();

        DefaultPieDataset dpd = new DefaultPieDataset();

        Map<String, Long> dane_z_wojewodztwa = new HashMap<String, Long>();

        for(Row row : lubuskie) {
            if(row.get(1) == null) {
                dpd.setValue((String)row.get(0), (long)row.get(2));
                dane_z_wojewodztwa.put((String)row.get(0), (long)row.get(2));
            }
            else {
                dpd.setValue((row.get(0) + " + LPG"), (long)row.get(2));
                dane_z_wojewodztwa.put((row.get(0) + " + LPG"), (long)row.get(2));
            }
        }

        wszystkieDaneZWojewodztw.put("lubuskie", dane_z_wojewodztwa);

        JFreeChart chart = ChartFactory.createPieChart(
                "Porównanie popularności rodzajów paliwa - województwo lubuskie",
                dpd,
                true,
                true,
                false
        );

        PiePlot plot = (PiePlot) chart.getPlot();
        plot.setSectionPaint("OLEJ NAPĘDOWY", Color.BLACK);
        plot.setSectionPaint("BENZYNA", Color.decode("#0096ff"));
        plot.setSectionPaint("BENZYNA + LPG", Color.YELLOW);
        plot.setForegroundAlpha(0.5f);
        plot.setExplodePercent("BENZYNA", 0.20);
        plot.setBackgroundPaint(Color.WHITE);
        plot.setSimpleLabels(true);

        PieSectionLabelGenerator gen = new StandardPieSectionLabelGenerator(
                "{0}: {1} ({2})", new DecimalFormat("0"), new DecimalFormat("0%"));
        plot.setLabelGenerator(gen);

        try {
            ChartUtilities.saveChartAsPNG(new File("F:\\II stopień\\Semestr IV\\Praca magisterska\\Analiza danych\\fuel_research\\fuel_lubuskie.png"),
                    chart, 500, 400);
        } catch (Exception e) {
            System.out.println("Wystapil problem podczas tworzenia wykresu");
        }

        JFrame frame = new JFrame("Popularność poszczególnych rodzajów paliwa " +
                "- województwo lubuskie");
        frame.setLayout(new FlowLayout());
        frame.getContentPane().add(new ChartPanel(chart));
        frame.pack();
        frame.setVisible(true);
    }

    public void popularity_of_fuels_woj_mazowieckie() {
        List<Row> mazowieckie = cepikData.getWojewodztwo_mazowieckie()
                .filter(col("rodzaj").equalTo("SAMOCHÓD OSOBOWY")
                        .and(col("data_wyrejestrowania").isNull()))
                .filter(col("rodzaj_paliwa").isNotNull())
                .filter(col("rodzaj_paliwa").like("%BENZYNA%")
                        .or(col("rodzaj_paliwa").like("%OLEJ NAPĘDOWY%")))
                .filter(col("rodzaj_paliwa_alternatywnego").like("%GAZ PŁYNNY%")
                        .or(col("rodzaj_paliwa_alternatywnego").isNull()))
                .groupBy("rodzaj_paliwa","rodzaj_paliwa_alternatywnego")
                .count()
                .sort(col("count").desc())
                .limit(3)
                .collectAsList();

        DefaultPieDataset dpd = new DefaultPieDataset();

        Map<String, Long> dane_z_wojewodztwa = new HashMap<String, Long>();

        for(Row row : mazowieckie) {
            if(row.get(1) == null) {
                dpd.setValue((String)row.get(0), (long)row.get(2));
                dane_z_wojewodztwa.put((String)row.get(0), (long)row.get(2));
            }
            else {
                dpd.setValue((row.get(0) + " + LPG"), (long)row.get(2));
                dane_z_wojewodztwa.put((row.get(0) + " + LPG"), (long)row.get(2));
            }
        }

        wszystkieDaneZWojewodztw.put("mazowieckie", dane_z_wojewodztwa);

        JFreeChart chart = ChartFactory.createPieChart(
                "Porównanie popularności rodzajów paliwa - województwo mazowieckie",
                dpd,
                true,
                true,
                false
        );

        PiePlot plot = (PiePlot) chart.getPlot();
        plot.setSectionPaint("OLEJ NAPĘDOWY", Color.BLACK);
        plot.setSectionPaint("BENZYNA", Color.decode("#0096ff"));
        plot.setSectionPaint("BENZYNA + LPG", Color.YELLOW);
        plot.setForegroundAlpha(0.5f);
        plot.setExplodePercent("BENZYNA", 0.20);
        plot.setBackgroundPaint(Color.WHITE);
        plot.setSimpleLabels(true);

        PieSectionLabelGenerator gen = new StandardPieSectionLabelGenerator(
                "{0}: {1} ({2})", new DecimalFormat("0"), new DecimalFormat("0%"));
        plot.setLabelGenerator(gen);

        try {
            ChartUtilities.saveChartAsPNG(new File("F:\\II stopień\\Semestr IV\\Praca magisterska\\Analiza danych\\fuel_research\\fuel_mazowieckie.png"),
                    chart, 500, 400);
        } catch (Exception e) {
            System.out.println("Wystapil problem podczas tworzenia wykresu");
        }

        JFrame frame = new JFrame("Popularność poszczególnych rodzajów paliwa " +
                "- województwo mazowieckie");
        frame.setLayout(new FlowLayout());
        frame.getContentPane().add(new ChartPanel(chart));
        frame.pack();
        frame.setVisible(true);
    }

    public void popularity_of_fuels_woj_lodzkie() {
        List<Row> lodzkie = cepikData.getWojewodztwo_lodzkie()
                .filter(col("rodzaj").equalTo("SAMOCHÓD OSOBOWY")
                        .and(col("data_wyrejestrowania").isNull()))
                .filter(col("rodzaj_paliwa").isNotNull())
                .filter(col("rodzaj_paliwa").like("%BENZYNA%")
                        .or(col("rodzaj_paliwa").like("%OLEJ NAPĘDOWY%")))
                .filter(col("rodzaj_paliwa_alternatywnego").like("%GAZ PŁYNNY%")
                        .or(col("rodzaj_paliwa_alternatywnego").isNull()))
                .groupBy("rodzaj_paliwa","rodzaj_paliwa_alternatywnego")
                .count()
                .sort(col("count").desc())
                .limit(3)
                .collectAsList();

        DefaultPieDataset dpd = new DefaultPieDataset();

        Map<String, Long> dane_z_wojewodztwa = new HashMap<String, Long>();

        for(Row row : lodzkie) {
            if(row.get(1) == null) {
                dpd.setValue((String)row.get(0), (long)row.get(2));
                dane_z_wojewodztwa.put((String)row.get(0), (long)row.get(2));
            }
            else {
                dpd.setValue((row.get(0) + " + LPG"), (long)row.get(2));
                dane_z_wojewodztwa.put((row.get(0) + " + LPG"), (long)row.get(2));
            }
        }

        wszystkieDaneZWojewodztw.put("lodzkie", dane_z_wojewodztwa);

        JFreeChart chart = ChartFactory.createPieChart(
                "Porównanie popularności rodzajów paliwa - województwo łódzkie",
                dpd,
                true,
                true,
                false
        );

        PiePlot plot = (PiePlot) chart.getPlot();
        plot.setSectionPaint("OLEJ NAPĘDOWY", Color.BLACK);
        plot.setSectionPaint("BENZYNA", Color.decode("#0096ff"));
        plot.setSectionPaint("BENZYNA + LPG", Color.YELLOW);
        plot.setForegroundAlpha(0.5f);
        plot.setExplodePercent("BENZYNA", 0.20);
        plot.setBackgroundPaint(Color.WHITE);
        plot.setSimpleLabels(true);

        PieSectionLabelGenerator gen = new StandardPieSectionLabelGenerator(
                "{0}: {1} ({2})", new DecimalFormat("0"), new DecimalFormat("0%"));
        plot.setLabelGenerator(gen);

        try {
            ChartUtilities.saveChartAsPNG(new File("F:\\II stopień\\Semestr IV\\Praca magisterska\\Analiza danych\\fuel_research\\fuel_lodzkie.png"),
                    chart, 500, 400);
        } catch (Exception e) {
            System.out.println("Wystapil problem podczas tworzenia wykresu");
        }

        JFrame frame = new JFrame("Popularność poszczególnych rodzajów paliwa " +
                "- województwo łódzkie");
        frame.setLayout(new FlowLayout());
        frame.getContentPane().add(new ChartPanel(chart));
        frame.pack();
        frame.setVisible(true);
    }

    public void popularity_of_fuels_woj_maloplskie() {
        List<Row> malopolskie = cepikData.getWojewodztwo_malopolskie()
                .filter(col("rodzaj").equalTo("SAMOCHÓD OSOBOWY")
                        .and(col("data_wyrejestrowania").isNull()))
                .filter(col("rodzaj_paliwa").isNotNull())
                .filter(col("rodzaj_paliwa").like("%BENZYNA%")
                        .or(col("rodzaj_paliwa").like("%OLEJ NAPĘDOWY%")))
                .filter(col("rodzaj_paliwa_alternatywnego").like("%GAZ PŁYNNY%")
                        .or(col("rodzaj_paliwa_alternatywnego").isNull()))
                .groupBy("rodzaj_paliwa","rodzaj_paliwa_alternatywnego")
                .count()
                .sort(col("count").desc())
                .limit(3)
                .collectAsList();

        DefaultPieDataset dpd = new DefaultPieDataset();

        Map<String, Long> dane_z_wojewodztwa = new HashMap<String, Long>();

        for(Row row : malopolskie) {
            if(row.get(1) == null) {
                dpd.setValue((String)row.get(0), (long)row.get(2));
                dane_z_wojewodztwa.put((String)row.get(0), (long)row.get(2));
            }
            else {
                dpd.setValue((row.get(0) + " + LPG"), (long)row.get(2));
                dane_z_wojewodztwa.put((row.get(0) + " + LPG"), (long)row.get(2));
            }
        }

        wszystkieDaneZWojewodztw.put("malopolskie", dane_z_wojewodztwa);

        JFreeChart chart = ChartFactory.createPieChart(
                "Porównanie popularności rodzajów paliwa - województwo małopolskie",
                dpd,
                true,
                true,
                false
        );

        PiePlot plot = (PiePlot) chart.getPlot();
        plot.setSectionPaint("OLEJ NAPĘDOWY", Color.BLACK);
        plot.setSectionPaint("BENZYNA", Color.decode("#0096ff"));
        plot.setSectionPaint("BENZYNA + LPG", Color.YELLOW);
        plot.setForegroundAlpha(0.5f);
        plot.setExplodePercent("BENZYNA", 0.20);
        plot.setBackgroundPaint(Color.WHITE);
        plot.setSimpleLabels(true);

        PieSectionLabelGenerator gen = new StandardPieSectionLabelGenerator(
                "{0}: {1} ({2})", new DecimalFormat("0"), new DecimalFormat("0%"));
        plot.setLabelGenerator(gen);

        try {
            ChartUtilities.saveChartAsPNG(new File("F:\\II stopień\\Semestr IV\\Praca magisterska\\Analiza danych\\fuel_research\\fuel_malopolskie.png"),
                    chart, 500, 400);
        } catch (Exception e) {
            System.out.println("Wystapil problem podczas tworzenia wykresu");
        }

        JFrame frame = new JFrame("Popularność poszczególnych rodzajów paliwa " +
                "- województwo małopolskie");
        frame.setLayout(new FlowLayout());
        frame.getContentPane().add(new ChartPanel(chart));
        frame.pack();
        frame.setVisible(true);
    }

    public void popularity_of_fuels_woj_opolskie() {
        List<Row> opolskie = cepikData.getWojewodztwo_opolskie()
                .filter(col("rodzaj").equalTo("SAMOCHÓD OSOBOWY")
                        .and(col("data_wyrejestrowania").isNull()))
                .filter(col("rodzaj_paliwa").isNotNull())
                .filter(col("rodzaj_paliwa").like("%BENZYNA%")
                        .or(col("rodzaj_paliwa").like("%OLEJ NAPĘDOWY%")))
                .filter(col("rodzaj_paliwa_alternatywnego").like("%GAZ PŁYNNY%")
                        .or(col("rodzaj_paliwa_alternatywnego").isNull()))
                .groupBy("rodzaj_paliwa","rodzaj_paliwa_alternatywnego")
                .count()
                .sort(col("count").desc())
                .limit(3)
                .collectAsList();

        DefaultPieDataset dpd = new DefaultPieDataset();

        Map<String, Long> dane_z_wojewodztwa = new HashMap<String, Long>();

        for(Row row : opolskie) {
            if(row.get(1) == null) {
                dpd.setValue((String)row.get(0), (long)row.get(2));
                dane_z_wojewodztwa.put((String)row.get(0), (long)row.get(2));
            }
            else {
                dpd.setValue((row.get(0) + " + LPG"), (long)row.get(2));
                dane_z_wojewodztwa.put((row.get(0) + " + LPG"), (long)row.get(2));
            }
        }

        wszystkieDaneZWojewodztw.put("opolskie", dane_z_wojewodztwa);

        JFreeChart chart = ChartFactory.createPieChart(
                "Porównanie popularności rodzajów paliwa - województwo opolskie",
                dpd,
                true,
                true,
                false
        );

        PiePlot plot = (PiePlot) chart.getPlot();
        plot.setSectionPaint("OLEJ NAPĘDOWY", Color.BLACK);
        plot.setSectionPaint("BENZYNA", Color.decode("#0096ff"));
        plot.setSectionPaint("BENZYNA + LPG", Color.YELLOW);
        plot.setForegroundAlpha(0.5f);
        plot.setExplodePercent("BENZYNA", 0.20);
        plot.setBackgroundPaint(Color.WHITE);
        plot.setSimpleLabels(true);

        PieSectionLabelGenerator gen = new StandardPieSectionLabelGenerator(
                "{0}: {1} ({2})", new DecimalFormat("0"), new DecimalFormat("0%"));
        plot.setLabelGenerator(gen);

        try {
            ChartUtilities.saveChartAsPNG(new File("F:\\II stopień\\Semestr IV\\Praca magisterska\\Analiza danych\\fuel_research\\fuel_opolskie.png"),
                    chart, 500, 400);
        } catch (Exception e) {
            System.out.println("Wystapil problem podczas tworzenia wykresu");
        }

        JFrame frame = new JFrame("Popularność poszczególnych rodzajów paliwa " +
                "- województwo opolskie");
        frame.setLayout(new FlowLayout());
        frame.getContentPane().add(new ChartPanel(chart));
        frame.pack();
        frame.setVisible(true);
    }

    public void popularity_of_fuels_woj_podkarpackie() {
        List<Row> podkarpackie = cepikData.getWojewodztwo_podkarpackie()
                .filter(col("rodzaj").equalTo("SAMOCHÓD OSOBOWY")
                        .and(col("data_wyrejestrowania").isNull()))
                .filter(col("rodzaj_paliwa").isNotNull())
                .filter(col("rodzaj_paliwa").like("%BENZYNA%")
                        .or(col("rodzaj_paliwa").like("%OLEJ NAPĘDOWY%")))
                .filter(col("rodzaj_paliwa_alternatywnego").like("%GAZ PŁYNNY%")
                        .or(col("rodzaj_paliwa_alternatywnego").isNull()))
                .groupBy("rodzaj_paliwa","rodzaj_paliwa_alternatywnego")
                .count()
                .sort(col("count").desc())
                .limit(3)
                .collectAsList();

        DefaultPieDataset dpd = new DefaultPieDataset();

        Map<String, Long> dane_z_wojewodztwa = new HashMap<String, Long>();

        for(Row row : podkarpackie) {
            if(row.get(1) == null) {
                dpd.setValue((String)row.get(0), (long)row.get(2));
                dane_z_wojewodztwa.put((String)row.get(0), (long)row.get(2));
            }
            else {
                dpd.setValue((row.get(0) + " + LPG"), (long)row.get(2));
                dane_z_wojewodztwa.put((row.get(0) + " + LPG"), (long)row.get(2));
            }
        }

        wszystkieDaneZWojewodztw.put("podkarpackie", dane_z_wojewodztwa);

        JFreeChart chart = ChartFactory.createPieChart(
                "Porównanie popularności rodzajów paliwa - województwo podkarpackie",
                dpd,
                true,
                true,
                false
        );

        PiePlot plot = (PiePlot) chart.getPlot();
        plot.setSectionPaint("OLEJ NAPĘDOWY", Color.BLACK);
        plot.setSectionPaint("BENZYNA", Color.decode("#0096ff"));
        plot.setSectionPaint("BENZYNA + LPG", Color.YELLOW);
        plot.setForegroundAlpha(0.5f);
        plot.setExplodePercent("BENZYNA", 0.20);
        plot.setBackgroundPaint(Color.WHITE);
        plot.setSimpleLabels(true);

        PieSectionLabelGenerator gen = new StandardPieSectionLabelGenerator(
                "{0}: {1} ({2})", new DecimalFormat("0"), new DecimalFormat("0%"));
        plot.setLabelGenerator(gen);

        try {
            ChartUtilities.saveChartAsPNG(new File("F:\\II stopień\\Semestr IV\\Praca magisterska\\Analiza danych\\fuel_research\\fuel_podkarpackie.png"),
                    chart, 500, 400);
        } catch (Exception e) {
            System.out.println("Wystapil problem podczas tworzenia wykresu");
        }

        JFrame frame = new JFrame("Popularność poszczególnych rodzajów paliwa " +
                "- województwo podkarpackie");
        frame.setLayout(new FlowLayout());
        frame.getContentPane().add(new ChartPanel(chart));
        frame.pack();
        frame.setVisible(true);
    }

    public void popularity_of_fuels_woj_pomorskie() {
        List<Row> pomorskie = cepikData.getWojewodztwo_pomorskie()
                .filter(col("rodzaj").equalTo("SAMOCHÓD OSOBOWY")
                        .and(col("data_wyrejestrowania").isNull()))
                .filter(col("rodzaj_paliwa").isNotNull())
                .filter(col("rodzaj_paliwa").like("%BENZYNA%")
                        .or(col("rodzaj_paliwa").like("%OLEJ NAPĘDOWY%")))
                .filter(col("rodzaj_paliwa_alternatywnego").like("%GAZ PŁYNNY%")
                        .or(col("rodzaj_paliwa_alternatywnego").isNull()))
                .groupBy("rodzaj_paliwa","rodzaj_paliwa_alternatywnego")
                .count()
                .sort(col("count").desc())
                .limit(3)
                .collectAsList();

        DefaultPieDataset dpd = new DefaultPieDataset();

        Map<String, Long> dane_z_wojewodztwa = new HashMap<String, Long>();

        for(Row row : pomorskie) {
            if(row.get(1) == null) {
                dpd.setValue((String)row.get(0), (long)row.get(2));
                dane_z_wojewodztwa.put((String)row.get(0), (long)row.get(2));
            }
            else {
                dpd.setValue((row.get(0) + " + LPG"), (long)row.get(2));
                dane_z_wojewodztwa.put((row.get(0) + " + LPG"), (long)row.get(2));
            }
        }

        wszystkieDaneZWojewodztw.put("pomorskie", dane_z_wojewodztwa);

        JFreeChart chart = ChartFactory.createPieChart(
                "Porównanie popularności rodzajów paliwa - województwo pomorskie",
                dpd,
                true,
                true,
                false
        );

        PiePlot plot = (PiePlot) chart.getPlot();
        plot.setSectionPaint("OLEJ NAPĘDOWY", Color.BLACK);
        plot.setSectionPaint("BENZYNA", Color.decode("#0096ff"));
        plot.setSectionPaint("BENZYNA + LPG", Color.YELLOW);
        plot.setForegroundAlpha(0.5f);
        plot.setExplodePercent("BENZYNA", 0.20);
        plot.setBackgroundPaint(Color.WHITE);
        plot.setSimpleLabels(true);

        PieSectionLabelGenerator gen = new StandardPieSectionLabelGenerator(
                "{0}: {1} ({2})", new DecimalFormat("0"), new DecimalFormat("0%"));
        plot.setLabelGenerator(gen);

        try {
            ChartUtilities.saveChartAsPNG(new File("F:\\II stopień\\Semestr IV\\Praca magisterska\\Analiza danych\\fuel_research\\fuel_pomorskie.png"),
                    chart, 500, 400);
        } catch (Exception e) {
            System.out.println("Wystapil problem podczas tworzenia wykresu");
        }

        JFrame frame = new JFrame("Popularność poszczególnych rodzajów paliwa " +
                "- województwo pomorskie");
        frame.setLayout(new FlowLayout());
        frame.getContentPane().add(new ChartPanel(chart));
        frame.pack();
        frame.setVisible(true);
    }

    public void popularity_of_fuels_woj_slaskie() {
        List<Row> slaskie = cepikData.getWojewodztwo_slaskie()
                .filter(col("rodzaj").equalTo("SAMOCHÓD OSOBOWY")
                        .and(col("data_wyrejestrowania").isNull()))
                .filter(col("rodzaj_paliwa").isNotNull())
                .filter(col("rodzaj_paliwa").like("%BENZYNA%")
                        .or(col("rodzaj_paliwa").like("%OLEJ NAPĘDOWY%")))
                .filter(col("rodzaj_paliwa_alternatywnego").like("%GAZ PŁYNNY%")
                        .or(col("rodzaj_paliwa_alternatywnego").isNull()))
                .groupBy("rodzaj_paliwa","rodzaj_paliwa_alternatywnego")
                .count()
                .sort(col("count").desc())
                .limit(3)
                .collectAsList();

        DefaultPieDataset dpd = new DefaultPieDataset();

        Map<String, Long> dane_z_wojewodztwa = new HashMap<String, Long>();

        for(Row row : slaskie) {
            if(row.get(1) == null) {
                dpd.setValue((String)row.get(0), (long)row.get(2));
                dane_z_wojewodztwa.put((String)row.get(0), (long)row.get(2));
            }
            else {
                dpd.setValue((row.get(0) + " + LPG"), (long)row.get(2));
                dane_z_wojewodztwa.put((row.get(0) + " + LPG"), (long)row.get(2));
            }
        }

        wszystkieDaneZWojewodztw.put("slaskie", dane_z_wojewodztwa);

        JFreeChart chart = ChartFactory.createPieChart(
                "Porównanie popularności rodzajów paliwa - województwo śląskie",
                dpd,
                true,
                true,
                false
        );

        PiePlot plot = (PiePlot) chart.getPlot();
        plot.setSectionPaint("OLEJ NAPĘDOWY", Color.BLACK);
        plot.setSectionPaint("BENZYNA", Color.decode("#0096ff"));
        plot.setSectionPaint("BENZYNA + LPG", Color.YELLOW);
        plot.setForegroundAlpha(0.5f);
        plot.setExplodePercent("BENZYNA", 0.20);
        plot.setBackgroundPaint(Color.WHITE);
        plot.setSimpleLabels(true);

        PieSectionLabelGenerator gen = new StandardPieSectionLabelGenerator(
                "{0}: {1} ({2})", new DecimalFormat("0"), new DecimalFormat("0%"));
        plot.setLabelGenerator(gen);

        try {
            ChartUtilities.saveChartAsPNG(new File("F:\\II stopień\\Semestr IV\\Praca magisterska\\Analiza danych\\fuel_research\\fuel_slaskie.png"),
                    chart, 500, 400);
        } catch (Exception e) {
            System.out.println("Wystapil problem podczas tworzenia wykresu");
        }

        JFrame frame = new JFrame("Popularność poszczególnych rodzajów paliwa " +
                "- województwo śląskie");
        frame.setLayout(new FlowLayout());
        frame.getContentPane().add(new ChartPanel(chart));
        frame.pack();
        frame.setVisible(true);
    }

    public void popularity_of_fuels_woj_swietokrzyskie() {
        List<Row> swietokrzyskie = cepikData.getWojewodztwo_swietokrzyskie()
                .filter(col("rodzaj").equalTo("SAMOCHÓD OSOBOWY")
                        .and(col("data_wyrejestrowania").isNull()))
                .filter(col("rodzaj_paliwa").isNotNull())
                .filter(col("rodzaj_paliwa").like("%BENZYNA%")
                        .or(col("rodzaj_paliwa").like("%OLEJ NAPĘDOWY%")))
                .filter(col("rodzaj_paliwa_alternatywnego").like("%GAZ PŁYNNY%")
                        .or(col("rodzaj_paliwa_alternatywnego").isNull()))
                .groupBy("rodzaj_paliwa","rodzaj_paliwa_alternatywnego")
                .count()
                .sort(col("count").desc())
                .limit(3)
                .collectAsList();

        DefaultPieDataset dpd = new DefaultPieDataset();

        Map<String, Long> dane_z_wojewodztwa = new HashMap<String, Long>();

        for(Row row : swietokrzyskie) {
            if(row.get(1) == null) {
                dpd.setValue((String)row.get(0), (long)row.get(2));
                dane_z_wojewodztwa.put((String)row.get(0), (long)row.get(2));
            }
            else {
                dpd.setValue((row.get(0) + " + LPG"), (long)row.get(2));
                dane_z_wojewodztwa.put((row.get(0) + " + LPG"), (long)row.get(2));
            }
        }

        wszystkieDaneZWojewodztw.put("swietokrzyskie", dane_z_wojewodztwa);

        JFreeChart chart = ChartFactory.createPieChart(
                "Porównanie popularności rodzajów paliwa - województwo świętokrzyskie",
                dpd,
                true,
                true,
                false
        );

        PiePlot plot = (PiePlot) chart.getPlot();
        plot.setSectionPaint("OLEJ NAPĘDOWY", Color.BLACK);
        plot.setSectionPaint("BENZYNA", Color.decode("#0096ff"));
        plot.setSectionPaint("BENZYNA + LPG", Color.YELLOW);
        plot.setForegroundAlpha(0.5f);
        plot.setExplodePercent("BENZYNA", 0.20);
        plot.setBackgroundPaint(Color.WHITE);
        plot.setSimpleLabels(true);

        PieSectionLabelGenerator gen = new StandardPieSectionLabelGenerator(
                "{0}: {1} ({2})", new DecimalFormat("0"), new DecimalFormat("0%"));
        plot.setLabelGenerator(gen);

        try {
            ChartUtilities.saveChartAsPNG(new File("F:\\II stopień\\Semestr IV\\Praca magisterska\\Analiza danych\\fuel_research\\fuel_swietokrzyskie.png"),
                    chart, 500, 400);
        } catch (Exception e) {
            System.out.println("Wystapil problem podczas tworzenia wykresu");
        }

        JFrame frame = new JFrame("Popularność poszczególnych rodzajów paliwa " +
                "- województwo świętokrzyskie");
        frame.setLayout(new FlowLayout());
        frame.getContentPane().add(new ChartPanel(chart));
        frame.pack();
        frame.setVisible(true);
    }

    public void popularity_of_fuels_woj_warminskomazurskie() {
        List<Row> warminskomazurskie = cepikData.getWojewodztwo_warminskomazurskie()
                .filter(col("rodzaj").equalTo("SAMOCHÓD OSOBOWY")
                        .and(col("data_wyrejestrowania").isNull()))
                .filter(col("rodzaj_paliwa").isNotNull())
                .filter(col("rodzaj_paliwa").like("%BENZYNA%")
                        .or(col("rodzaj_paliwa").like("%OLEJ NAPĘDOWY%")))
                .filter(col("rodzaj_paliwa_alternatywnego").like("%GAZ PŁYNNY%")
                        .or(col("rodzaj_paliwa_alternatywnego").isNull()))
                .groupBy("rodzaj_paliwa","rodzaj_paliwa_alternatywnego")
                .count()
                .sort(col("count").desc())
                .limit(3)
                .collectAsList();

        DefaultPieDataset dpd = new DefaultPieDataset();

        Map<String, Long> dane_z_wojewodztwa = new HashMap<String, Long>();

        for(Row row : warminskomazurskie) {
            if(row.get(1) == null) {
                dpd.setValue((String)row.get(0), (long)row.get(2));
                dane_z_wojewodztwa.put((String)row.get(0), (long)row.get(2));
            }
            else {
                dpd.setValue((row.get(0) + " + LPG"), (long)row.get(2));
                dane_z_wojewodztwa.put((row.get(0) + " + LPG"), (long)row.get(2));
            }
        }

        wszystkieDaneZWojewodztw.put("warminskomazurskie", dane_z_wojewodztwa);

        JFreeChart chart = ChartFactory.createPieChart(
                "Porównanie popularności rodzajów paliwa - województwo warmińsko-mazurskie",
                dpd,
                true,
                true,
                false
        );

        PiePlot plot = (PiePlot) chart.getPlot();
        plot.setSectionPaint("OLEJ NAPĘDOWY", Color.BLACK);
        plot.setSectionPaint("BENZYNA", Color.decode("#0096ff"));
        plot.setSectionPaint("BENZYNA + LPG", Color.YELLOW);
        plot.setForegroundAlpha(0.5f);
        plot.setExplodePercent("BENZYNA", 0.20);
        plot.setBackgroundPaint(Color.WHITE);
        plot.setSimpleLabels(true);

        PieSectionLabelGenerator gen = new StandardPieSectionLabelGenerator(
                "{0}: {1} ({2})", new DecimalFormat("0"), new DecimalFormat("0%"));
        plot.setLabelGenerator(gen);

        try {
            ChartUtilities.saveChartAsPNG(new File("F:\\II stopień\\Semestr IV\\Praca magisterska\\Analiza danych\\fuel_research\\fuel_warminskomazurskie.png"),
                    chart, 500, 400);
        } catch (Exception e) {
            System.out.println("Wystapil problem podczas tworzenia wykresu");
        }

        JFrame frame = new JFrame("Popularność poszczególnych rodzajów paliwa " +
                "- województwo warmińsko-mazurskie");
        frame.setLayout(new FlowLayout());
        frame.getContentPane().add(new ChartPanel(chart));
        frame.pack();
        frame.setVisible(true);
    }

    public void popularity_of_fuels_woj_wielkopolskie() {
        List<Row> wielkopolskie = cepikData.getWojewodztwo_wielkopolskie()
                .filter(col("rodzaj").equalTo("SAMOCHÓD OSOBOWY")
                        .and(col("data_wyrejestrowania").isNull()))
                .filter(col("rodzaj_paliwa").isNotNull())
                .filter(col("rodzaj_paliwa").like("%BENZYNA%")
                        .or(col("rodzaj_paliwa").like("%OLEJ NAPĘDOWY%")))
                .filter(col("rodzaj_paliwa_alternatywnego").like("%GAZ PŁYNNY%")
                        .or(col("rodzaj_paliwa_alternatywnego").isNull()))
                .groupBy("rodzaj_paliwa","rodzaj_paliwa_alternatywnego")
                .count()
                .sort(col("count").desc())
                .limit(3)
                .collectAsList();

        DefaultPieDataset dpd = new DefaultPieDataset();

        Map<String, Long> dane_z_wojewodztwa = new HashMap<String, Long>();

        for(Row row : wielkopolskie) {
            if(row.get(1) == null) {
                dpd.setValue((String)row.get(0), (long)row.get(2));
                dane_z_wojewodztwa.put((String)row.get(0), (long)row.get(2));
            }
            else {
                dpd.setValue((row.get(0) + " + LPG"), (long)row.get(2));
                dane_z_wojewodztwa.put((row.get(0) + " + LPG"), (long)row.get(2));
            }
        }

        wszystkieDaneZWojewodztw.put("wielkopolskie", dane_z_wojewodztwa);

        JFreeChart chart = ChartFactory.createPieChart(
                "Porównanie popularności rodzajów paliwa - województwo wielkopolskie",
                dpd,
                true,
                true,
                false
        );

        PiePlot plot = (PiePlot) chart.getPlot();
        plot.setSectionPaint("OLEJ NAPĘDOWY", Color.BLACK);
        plot.setSectionPaint("BENZYNA", Color.decode("#0096ff"));
        plot.setSectionPaint("BENZYNA + LPG", Color.YELLOW);
        plot.setForegroundAlpha(0.5f);
        plot.setExplodePercent("BENZYNA", 0.20);
        plot.setBackgroundPaint(Color.WHITE);
        plot.setSimpleLabels(true);

        PieSectionLabelGenerator gen = new StandardPieSectionLabelGenerator(
                "{0}: {1} ({2})", new DecimalFormat("0"), new DecimalFormat("0%"));
        plot.setLabelGenerator(gen);

        try {
            ChartUtilities.saveChartAsPNG(new File("F:\\II stopień\\Semestr IV\\Praca magisterska\\Analiza danych\\fuel_research\\fuel_wielkopolskie.png"),
                    chart, 500, 400);
        } catch (Exception e) {
            System.out.println("Wystapil problem podczas tworzenia wykresu");
        }

        JFrame frame = new JFrame("Popularność poszczególnych rodzajów paliwa " +
                "- województwo wielkopolskie");
        frame.setLayout(new FlowLayout());
        frame.getContentPane().add(new ChartPanel(chart));
        frame.pack();
        frame.setVisible(true);
    }

    public void popularity_of_fuels_woj_zachodniopomorskie() {
        List<Row> zachodniopomorskie = cepikData.getWojewodztwo_zachodniopomorskie()
                .filter(col("rodzaj").equalTo("SAMOCHÓD OSOBOWY")
                        .and(col("data_wyrejestrowania").isNull()))
                .filter(col("rodzaj_paliwa").isNotNull())
                .filter(col("rodzaj_paliwa").like("%BENZYNA%")
                        .or(col("rodzaj_paliwa").like("%OLEJ NAPĘDOWY%")))
                .filter(col("rodzaj_paliwa_alternatywnego").like("%GAZ PŁYNNY%")
                        .or(col("rodzaj_paliwa_alternatywnego").isNull()))
                .groupBy("rodzaj_paliwa","rodzaj_paliwa_alternatywnego")
                .count()
                .sort(col("count").desc())
                .limit(3)
                .collectAsList();

        DefaultPieDataset dpd = new DefaultPieDataset();

        Map<String, Long> dane_z_wojewodztwa = new HashMap<String, Long>();

        for(Row row : zachodniopomorskie) {
            if(row.get(1) == null) {
                dpd.setValue((String)row.get(0), (long)row.get(2));
                dane_z_wojewodztwa.put((String)row.get(0), (long)row.get(2));
            }
            else {
                dpd.setValue((row.get(0) + " + LPG"), (long)row.get(2));
                dane_z_wojewodztwa.put((row.get(0) + " + LPG"), (long)row.get(2));
            }
        }

        wszystkieDaneZWojewodztw.put("zachodniopomorskie", dane_z_wojewodztwa);

        JFreeChart chart = ChartFactory.createPieChart(
                "Porównanie popularności rodzajów paliwa - województwo zachodniopomorskie",
                dpd,
                true,
                true,
                false
        );

        PiePlot plot = (PiePlot) chart.getPlot();
        plot.setSectionPaint("OLEJ NAPĘDOWY", Color.BLACK);
        plot.setSectionPaint("BENZYNA", Color.decode("#0096ff"));
        plot.setSectionPaint("BENZYNA + LPG", Color.YELLOW);
        plot.setForegroundAlpha(0.5f);
        plot.setExplodePercent("BENZYNA", 0.20);
        plot.setBackgroundPaint(Color.WHITE);
        plot.setSimpleLabels(true);

        PieSectionLabelGenerator gen = new StandardPieSectionLabelGenerator(
                "{0}: {1} ({2})", new DecimalFormat("0"), new DecimalFormat("0%"));
        plot.setLabelGenerator(gen);

        try {
            ChartUtilities.saveChartAsPNG(new File("F:\\II stopień\\Semestr IV\\Praca magisterska\\Analiza danych\\fuel_research\\fuel_zachodniopomorskie.png"),
                    chart, 500, 400);
        } catch (Exception e) {
            System.out.println("Wystapil problem podczas tworzenia wykresu");
        }

        JFrame frame = new JFrame("Popularność poszczególnych rodzajów paliwa " +
                "- województwo zachodniopomorskie");
        frame.setLayout(new FlowLayout());
        frame.getContentPane().add(new ChartPanel(chart));
        frame.pack();
        frame.setVisible(true);
    }

    public void popularity_of_fuels_poland() {
        List<Row> nieokreslone = cepikData.getNieokreslone_wojewodztwo()
                .filter(col("rodzaj").equalTo("SAMOCHÓD OSOBOWY")
                        .and(col("data_wyrejestrowania").isNull()))
                .filter(col("rodzaj_paliwa").isNotNull())
                .filter(col("rodzaj_paliwa").like("%BENZYNA%")
                        .or(col("rodzaj_paliwa").like("%OLEJ NAPĘDOWY%")))
                .filter(col("rodzaj_paliwa_alternatywnego").like("%GAZ PŁYNNY%")
                        .or(col("rodzaj_paliwa_alternatywnego").isNull()))
                .groupBy("rodzaj_paliwa","rodzaj_paliwa_alternatywnego")
                .count()
                .sort(col("count").desc())
                .limit(3)
                .collectAsList();

        Map<String, Long> dane_z_wojewodztwa = new HashMap<String, Long>();

        for(Row row : nieokreslone) {
            if (row.get(1) == null) {
                dane_z_wojewodztwa.put((String) row.get(0), (long) row.get(2));
            } else {
                dane_z_wojewodztwa.put((row.get(0) + " + LPG"), (long) row.get(2));
            }
        }

        wszystkieDaneZWojewodztw.put("nieokreslone", dane_z_wojewodztwa);

        Map<String, Long> paliwa = new HashMap<String, Long>();
        wszystkieDaneZWojewodztw.forEach((k,v) -> {
            v.forEach((kp,vp) -> {
                if(paliwa.containsKey(kp)) {
                    long nowa_wartosc = vp + paliwa.get(kp);
                    paliwa.remove(kp);
                    paliwa.put(kp, nowa_wartosc);
                }
                else {
                    paliwa.put(kp, vp);
                }
            });
        });

        DefaultPieDataset dpd = new DefaultPieDataset();

        paliwa.forEach((k,v) -> {
            dpd.setValue(k, v);
        });

        JFreeChart chart = ChartFactory.createPieChart(
                "Porównanie popularności rodzajów paliwa w całym kraju (Polska) z uwzględnieniem danych bez przydziału województwa",
                dpd,
                true,
                true,
                false
        );

        PiePlot plot = (PiePlot) chart.getPlot();
        plot.setSectionPaint("OLEJ NAPĘDOWY", Color.BLACK);
        plot.setSectionPaint("BENZYNA", Color.decode("#0096ff"));
        plot.setSectionPaint("BENZYNA + LPG", Color.YELLOW);
        plot.setForegroundAlpha(0.5f);
        plot.setExplodePercent("BENZYNA", 0.20);
        plot.setBackgroundPaint(Color.WHITE);
        plot.setSimpleLabels(true);

        PieSectionLabelGenerator gen = new StandardPieSectionLabelGenerator(
                "{0}: {1} ({2})", new DecimalFormat("0"), new DecimalFormat("0%"));
        plot.setLabelGenerator(gen);

        try {
            ChartUtilities.saveChartAsPNG(new File("F:\\II stopień\\Semestr IV\\Praca magisterska\\Analiza danych\\fuel_research\\fuel_poland.png"),
                    chart, 500, 400);
        } catch (Exception e) {
            System.out.println("Wystapil problem podczas tworzenia wykresu");
        }

        JFrame frame = new JFrame("Popularność poszczególnych rodzajów paliwa " +
                "- Polska (ogółem)");
        frame.setLayout(new FlowLayout());
        frame.getContentPane().add(new ChartPanel(chart));
        frame.pack();
        frame.setVisible(true);
    }
}
