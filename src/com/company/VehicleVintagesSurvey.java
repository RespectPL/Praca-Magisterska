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
import org.jfree.chart.plot.CategoryPlot;
import org.jfree.chart.plot.PiePlot;
import org.jfree.chart.plot.PlotOrientation;
import org.jfree.chart.renderer.category.BarRenderer;
import org.jfree.data.category.DefaultCategoryDataset;
import org.jfree.data.general.DefaultPieDataset;

import javax.swing.*;
import java.awt.*;
import java.io.File;
import java.io.IOException;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.apache.spark.sql.functions.col;

public class VehicleVintagesSurvey {
    private CepikData cepikData;
    private Map<String, Long> wszystkieDaneZWojewodztw_roczniki;
    private Map<String, Long> wszystkieDaneZWojewodztw_paliwa;

    /*public VehicleVintagesSurvey(SparkSession spark) {
        this.cepikData = new CepikData(spark);
        this.wszystkieDaneZWojewodztw_roczniki = new HashMap<String, Long>();
        this.wszystkieDaneZWojewodztw_paliwa = new HashMap<String, Long>();
    }*/

    public VehicleVintagesSurvey(CepikData cepikData) {
        this.cepikData = cepikData;
        this.wszystkieDaneZWojewodztw_roczniki = new HashMap<String, Long>();
        this.wszystkieDaneZWojewodztw_paliwa = new HashMap<String, Long>();
    }

    public void popularity_of_vintages_of_vehicles_and_fuels_woj_lubelskie() {
        // Obliczanie ilosci samochodow osobowych w danych latach produkcji
        Dataset<Row> samochody_zarej_rp = cepikData.getWojewodztwo_lubelskie()
                .filter(col("rodzaj").equalTo("SAMOCHÓD OSOBOWY")
                        .and(col("data_wyrejestrowania").isNull()))
                .filter(col("rok_produkcji").isNotNull());

        Dataset<Row> pojazdy_leq2003 = samochody_zarej_rp
                .filter(col("rok_produkcji").leq(2003));

        Dataset<Row> pojazdy_gt2003leq2013 = samochody_zarej_rp
                .filter(col("rok_produkcji").gt(2003)
                        .and(col("rok_produkcji").leq(2013)));

        Dataset<Row> pojazdy_gt2013 = samochody_zarej_rp
                .filter(col("rok_produkcji").gt(2013));

        long liczba_leq2003 = pojazdy_leq2003.count();
        long liczba_gt2003leq2013 = pojazdy_gt2003leq2013.count();
        long liczba_gt2013 = pojazdy_gt2013.count();

        if(wszystkieDaneZWojewodztw_roczniki.containsKey("do 2003")) {
            long nowa_wartosc = liczba_leq2003 + wszystkieDaneZWojewodztw_roczniki.get("do 2003");
            wszystkieDaneZWojewodztw_roczniki.remove("do 2003");
            wszystkieDaneZWojewodztw_roczniki.put("do 2003", nowa_wartosc);
        }
        else {
            wszystkieDaneZWojewodztw_roczniki.put("do 2003", liczba_leq2003);
        }

        if(wszystkieDaneZWojewodztw_roczniki.containsKey("2004 - 2013")) {
            long nowa_wartosc = liczba_gt2003leq2013 + wszystkieDaneZWojewodztw_roczniki.get("2004 - 2013");
            wszystkieDaneZWojewodztw_roczniki.remove("2004 - 2013");
            wszystkieDaneZWojewodztw_roczniki.put("2004 - 2013", nowa_wartosc);
        }
        else {
            wszystkieDaneZWojewodztw_roczniki.put("2004 - 2013", liczba_gt2003leq2013);
        }

        if(wszystkieDaneZWojewodztw_roczniki.containsKey("od 2014")) {
            long nowa_wartosc = liczba_gt2013 + wszystkieDaneZWojewodztw_roczniki.get("od 2014");
            wszystkieDaneZWojewodztw_roczniki.remove("od 2014");
            wszystkieDaneZWojewodztw_roczniki.put("od 2014", nowa_wartosc);
        }
        else {
            wszystkieDaneZWojewodztw_roczniki.put("od 2014", liczba_gt2013);
        }

        // Tworzenie wykresu 1 - Porównanie popularności aut z poszczególnych roczników - województwo lubelskie
        DefaultPieDataset dpd = new DefaultPieDataset();
        dpd.setValue("do 2003", liczba_leq2003);
        dpd.setValue("2004 - 2013", liczba_gt2003leq2013);
        dpd.setValue("od 2014", liczba_gt2013);

        JFreeChart chart = ChartFactory.createPieChart(
                "Porównanie popularności aut z poszczególnych roczników" +
                        " - województwo lubelskie",
                dpd,
                true,
                true,
                false
        );

        PiePlot plot = (PiePlot) chart.getPlot();
        plot.setSectionPaint("do 2003", Color.decode("#0096ff"));
        plot.setSectionPaint("2004 - 2013", Color.BLACK);
        plot.setSectionPaint("od 2014", Color.YELLOW);
        plot.setForegroundAlpha(0.5f);
        plot.setExplodePercent("do 2003", 0.20);
        plot.setBackgroundPaint(Color.WHITE);
        plot.setSimpleLabels(true);

        PieSectionLabelGenerator gen = new StandardPieSectionLabelGenerator(
                "{0}: {1} ({2})", new DecimalFormat("0"), new DecimalFormat("0%"));
        plot.setLabelGenerator(gen);

        try {
            ChartUtilities.saveChartAsPNG(
                    new File("F:\\II stopień\\Semestr IV\\Praca magisterska\\Analiza danych\\vehicle_vintages_survey\\lubelskie_ilosc.png"),
                    chart,
                    500,
                    400);
        } catch (Exception e) {
            System.out.println("Wystapil problem podczas tworzenia wykresu");
        }

        // Obliczanie ilosci pojazdow na dany rodzaj paliwa w danym przedziale lat produkcji
        List<Row> paliwa_leq2003 = pojazdy_leq2003
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

        List<Row> paliwa_gt2003leq2013 = pojazdy_gt2003leq2013
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

        List<Row> paliwa_gt2013 = pojazdy_gt2013
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

        // Tworzenie wykresu 2 - Podział samochodów z poszczególnych roczników
        // względem rodzaju paliwa - województwo lubelskie
        DefaultCategoryDataset dcd = new DefaultCategoryDataset();

        for(Row row : paliwa_leq2003) {
            if(row.get(1) == null) {
                dcd.setValue((long)row.get(2), (String)row.get(0), "do 2003");
                if(row.get(0).equals("BENZYNA")) {
                    if(wszystkieDaneZWojewodztw_paliwa.containsKey("PB (do 2003)")) {
                        long nowa_wartosc = (long)row.get(2)
                                + wszystkieDaneZWojewodztw_paliwa.get("PB (do 2003)");
                        wszystkieDaneZWojewodztw_paliwa.remove("PB (do 2003)");
                        wszystkieDaneZWojewodztw_paliwa.put("PB (do 2003)", nowa_wartosc);
                    }
                    else {
                        wszystkieDaneZWojewodztw_paliwa.put("PB (do 2003)", (long)row.get(2));
                    }
                }
                else if(row.get(0).equals("OLEJ NAPĘDOWY")){
                    if(wszystkieDaneZWojewodztw_paliwa.containsKey("ON (do 2003)")) {
                        long nowa_wartosc = (long)row.get(2)
                                + wszystkieDaneZWojewodztw_paliwa.get("ON (do 2003)");
                        wszystkieDaneZWojewodztw_paliwa.remove("ON (do 2003)");
                        wszystkieDaneZWojewodztw_paliwa.put("ON (do 2003)", nowa_wartosc);
                    }
                    else {
                        wszystkieDaneZWojewodztw_paliwa.put("ON (do 2003)", (long)row.get(2));
                    }
                }
            }
            else {
                dcd.setValue((long)row.get(2), row.get(0) + " + LPG", "do 2003");
                if(wszystkieDaneZWojewodztw_paliwa.containsKey("PB+LPG (do 2003)")) {
                    long nowa_wartosc = (long)row.get(2)
                            + wszystkieDaneZWojewodztw_paliwa.get("PB+LPG (do 2003)");
                    wszystkieDaneZWojewodztw_paliwa.remove("PB+LPG (do 2003)");
                    wszystkieDaneZWojewodztw_paliwa.put("PB+LPG (do 2003)", nowa_wartosc);
                }
                else {
                    wszystkieDaneZWojewodztw_paliwa.put("PB+LPG (do 2003)", (long)row.get(2));
                }
            }
        }
        for(Row row : paliwa_gt2003leq2013) {
            if(row.get(1) == null) {
                dcd.setValue((long)row.get(2), (String)row.get(0), "2004 - 2013");
                if(row.get(0).equals("BENZYNA")) {
                    if(wszystkieDaneZWojewodztw_paliwa.containsKey("PB (2004 - 2013)")) {
                        long nowa_wartosc = (long)row.get(2)
                                + wszystkieDaneZWojewodztw_paliwa.get("PB (2004 - 2013)");
                        wszystkieDaneZWojewodztw_paliwa.remove("PB (2004 - 2013)");
                        wszystkieDaneZWojewodztw_paliwa.put("PB (2004 - 2013)", nowa_wartosc);
                    }
                    else {
                        wszystkieDaneZWojewodztw_paliwa.put("PB (2004 - 2013)", (long)row.get(2));
                    }
                }
                else if(row.get(0).equals("OLEJ NAPĘDOWY")){
                    if(wszystkieDaneZWojewodztw_paliwa.containsKey("ON (2004 - 2013)")) {
                        long nowa_wartosc = (long)row.get(2)
                                + wszystkieDaneZWojewodztw_paliwa.get("ON (2004 - 2013)");
                        wszystkieDaneZWojewodztw_paliwa.remove("ON (2004 - 2013)");
                        wszystkieDaneZWojewodztw_paliwa.put("ON (2004 - 2013)", nowa_wartosc);
                    }
                    else {
                        wszystkieDaneZWojewodztw_paliwa.put("ON (2004 - 2013)", (long)row.get(2));
                    }
                }
            }
            else {
                dcd.setValue((long)row.get(2), row.get(0) + " + LPG", "2004 - 2013");
                if(wszystkieDaneZWojewodztw_paliwa.containsKey("PB+LPG (2004 - 2013)")) {
                    long nowa_wartosc = (long)row.get(2)
                            + wszystkieDaneZWojewodztw_paliwa.get("PB+LPG (2004 - 2013)");
                    wszystkieDaneZWojewodztw_paliwa.remove("PB+LPG (2004 - 2013)");
                    wszystkieDaneZWojewodztw_paliwa.put("PB+LPG (2004 - 2013)", nowa_wartosc);
                }
                else {
                    wszystkieDaneZWojewodztw_paliwa.put("PB+LPG (2004 - 2013)", (long)row.get(2));
                }
            }
        }
        for(Row row : paliwa_gt2013) {
            if(row.get(1) == null) {
                dcd.setValue((long)row.get(2), (String)row.get(0), "od 2014");
                if(row.get(0).equals("BENZYNA")) {
                    if(wszystkieDaneZWojewodztw_paliwa.containsKey("PB (od 2014)")) {
                        long nowa_wartosc = (long)row.get(2)
                                + wszystkieDaneZWojewodztw_paliwa.get("PB (od 2014)");
                        wszystkieDaneZWojewodztw_paliwa.remove("PB (od 2014)");
                        wszystkieDaneZWojewodztw_paliwa.put("PB (od 2014)", nowa_wartosc);
                    }
                    else {
                        wszystkieDaneZWojewodztw_paliwa.put("PB (od 2014)", (long)row.get(2));
                    }
                }
                else if(row.get(0).equals("OLEJ NAPĘDOWY")){
                    if(wszystkieDaneZWojewodztw_paliwa.containsKey("ON (od 2014)")) {
                        long nowa_wartosc = (long)row.get(2)
                                + wszystkieDaneZWojewodztw_paliwa.get("ON (od 2014)");
                        wszystkieDaneZWojewodztw_paliwa.remove("ON (od 2014)");
                        wszystkieDaneZWojewodztw_paliwa.put("ON (od 2014)", nowa_wartosc);
                    }
                    else {
                        wszystkieDaneZWojewodztw_paliwa.put("ON (od 2014)", (long)row.get(2));
                    }
                }
            }
            else {
                dcd.setValue((long)row.get(2), row.get(0) + " + LPG", "od 2014");
                if(wszystkieDaneZWojewodztw_paliwa.containsKey("PB+LPG (od 2014)")) {
                    long nowa_wartosc = (long)row.get(2)
                            + wszystkieDaneZWojewodztw_paliwa.get("PB+LPG (od 2014)");
                    wszystkieDaneZWojewodztw_paliwa.remove("PB+LPG (od 2014)");
                    wszystkieDaneZWojewodztw_paliwa.put("PB+LPG (od 2014)", nowa_wartosc);
                }
                else {
                    wszystkieDaneZWojewodztw_paliwa.put("PB+LPG (od 2014)", (long)row.get(2));
                }
            }
        }

        JFreeChart chart2 = ChartFactory.createBarChart(
                "Podział samochodów z poszczególnych roczników względem rodzaju paliwa" +
                        " - województwo lubelskie",
                "Rok produkcji",
                "Ilość pojazdów",
                dcd,
                PlotOrientation.VERTICAL,
                true,
                true,
                false
        );

        CategoryPlot plot2 = (CategoryPlot) chart2.getPlot();
        plot2.setForegroundAlpha(0.7f);
        plot2.getRenderer().setSeriesPaint(0, Color.decode("#006ff9"));
        plot2.getRenderer().setSeriesPaint(1, Color.ORANGE);
        plot2.getRenderer().setSeriesPaint(2, Color.DARK_GRAY);
        plot2.setRangeGridlinePaint(Color.BLACK);
        plot2.setBackgroundPaint(Color.WHITE);
        BarRenderer renderer = (BarRenderer) plot2.getRenderer();
        renderer.setItemMargin(.1);

        try {
            ChartUtilities.saveChartAsPNG(
                    new File("F:\\II stopień\\Semestr IV\\Praca magisterska\\Analiza danych\\vehicle_vintages_survey\\lubelskie_paliwa.png"),
                    chart2,
                    500,
                    400
            );
        } catch (IOException e) {
            System.out.println("Wystapil problem podczas tworzenia wykresu");
        }

        JFrame frame = new JFrame("Popularność poszczególnych roczników pojazdów względem" +
                " rodzaju paliwa - województwo lubelskie");
        frame.setLayout(new FlowLayout());
        frame.getContentPane().add(new ChartPanel(chart));
        frame.getContentPane().add(new ChartPanel(chart2));
        frame.pack();
        frame.setVisible(true);
    }

    public void popularity_of_vintages_of_vehicles_and_fuels_woj_dolnoslaskie() {
        // Obliczanie ilosci samochodow osobowych w danych latach produkcji
        Dataset<Row> samochody_zarej_rp = cepikData.getWojewodztwo_dolnoslaskie()
                .filter(col("rodzaj").equalTo("SAMOCHÓD OSOBOWY")
                        .and(col("data_wyrejestrowania").isNull()))
                .filter(col("rok_produkcji").isNotNull());

        Dataset<Row> pojazdy_leq2003 = samochody_zarej_rp
                .filter(col("rok_produkcji").leq(2003));

        Dataset<Row> pojazdy_gt2003leq2013 = samochody_zarej_rp
                .filter(col("rok_produkcji").gt(2003)
                        .and(col("rok_produkcji").leq(2013)));

        Dataset<Row> pojazdy_gt2013 = samochody_zarej_rp
                .filter(col("rok_produkcji").gt(2013));

        long liczba_leq2003 = pojazdy_leq2003.count();
        long liczba_gt2003leq2013 = pojazdy_gt2003leq2013.count();
        long liczba_gt2013 = pojazdy_gt2013.count();

        if(wszystkieDaneZWojewodztw_roczniki.containsKey("do 2003")) {
            long nowa_wartosc = liczba_leq2003 + wszystkieDaneZWojewodztw_roczniki.get("do 2003");
            wszystkieDaneZWojewodztw_roczniki.remove("do 2003");
            wszystkieDaneZWojewodztw_roczniki.put("do 2003", nowa_wartosc);
        }
        else {
            wszystkieDaneZWojewodztw_roczniki.put("do 2003", liczba_leq2003);
        }

        if(wszystkieDaneZWojewodztw_roczniki.containsKey("2004 - 2013")) {
            long nowa_wartosc = liczba_gt2003leq2013 + wszystkieDaneZWojewodztw_roczniki.get("2004 - 2013");
            wszystkieDaneZWojewodztw_roczniki.remove("2004 - 2013");
            wszystkieDaneZWojewodztw_roczniki.put("2004 - 2013", nowa_wartosc);
        }
        else {
            wszystkieDaneZWojewodztw_roczniki.put("2004 - 2013", liczba_gt2003leq2013);
        }

        if(wszystkieDaneZWojewodztw_roczniki.containsKey("od 2014")) {
            long nowa_wartosc = liczba_gt2013 + wszystkieDaneZWojewodztw_roczniki.get("od 2014");
            wszystkieDaneZWojewodztw_roczniki.remove("od 2014");
            wszystkieDaneZWojewodztw_roczniki.put("od 2014", nowa_wartosc);
        }
        else {
            wszystkieDaneZWojewodztw_roczniki.put("od 2014", liczba_gt2013);
        }

        // Tworzenie wykresu 1 - Porównanie popularności aut z poszczególnych roczników - województwo lubelskie
        DefaultPieDataset dpd = new DefaultPieDataset();
        dpd.setValue("do 2003", liczba_leq2003);
        dpd.setValue("2004 - 2013", liczba_gt2003leq2013);
        dpd.setValue("od 2014", liczba_gt2013);

        JFreeChart chart = ChartFactory.createPieChart(
                "Porównanie popularności aut z poszczególnych roczników" +
                        " - województwo dolnośląskie",
                dpd,
                true,
                true,
                false
        );

        PiePlot plot = (PiePlot) chart.getPlot();
        plot.setSectionPaint("do 2003", Color.decode("#0096ff"));
        plot.setSectionPaint("2004 - 2013", Color.BLACK);
        plot.setSectionPaint("od 2014", Color.YELLOW);
        plot.setForegroundAlpha(0.5f);
        plot.setExplodePercent("do 2003", 0.20);
        plot.setBackgroundPaint(Color.WHITE);
        plot.setSimpleLabels(true);

        PieSectionLabelGenerator gen = new StandardPieSectionLabelGenerator(
                "{0}: {1} ({2})", new DecimalFormat("0"), new DecimalFormat("0%"));
        plot.setLabelGenerator(gen);

        try {
            ChartUtilities.saveChartAsPNG(
                    new File("F:\\II stopień\\Semestr IV\\Praca magisterska\\Analiza danych\\vehicle_vintages_survey\\dolnoslaskie_ilosc.png"),
                    chart,
                    500,
                    400);
        } catch (Exception e) {
            System.out.println("Wystapil problem podczas tworzenia wykresu");
        }

        // Obliczanie ilosci pojazdow na dany rodzaj paliwa w danym przedziale lat produkcji
        List<Row> paliwa_leq2003 = pojazdy_leq2003
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

        List<Row> paliwa_gt2003leq2013 = pojazdy_gt2003leq2013
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

        List<Row> paliwa_gt2013 = pojazdy_gt2013
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

        // Tworzenie wykresu 2 - Podział samochodów z poszczególnych roczników
        // względem rodzaju paliwa - województwo lubelskie
        DefaultCategoryDataset dcd = new DefaultCategoryDataset();

        for(Row row : paliwa_leq2003) {
            if(row.get(1) == null) {
                dcd.setValue((long)row.get(2), (String)row.get(0), "do 2003");
                if(row.get(0).equals("BENZYNA")) {
                    if(wszystkieDaneZWojewodztw_paliwa.containsKey("PB (do 2003)")) {
                        long nowa_wartosc = (long)row.get(2)
                                + wszystkieDaneZWojewodztw_paliwa.get("PB (do 2003)");
                        wszystkieDaneZWojewodztw_paliwa.remove("PB (do 2003)");
                        wszystkieDaneZWojewodztw_paliwa.put("PB (do 2003)", nowa_wartosc);
                    }
                    else {
                        wszystkieDaneZWojewodztw_paliwa.put("PB (do 2003)", (long)row.get(2));
                    }
                }
                else if(row.get(0).equals("OLEJ NAPĘDOWY")){
                    if(wszystkieDaneZWojewodztw_paliwa.containsKey("ON (do 2003)")) {
                        long nowa_wartosc = (long)row.get(2)
                                + wszystkieDaneZWojewodztw_paliwa.get("ON (do 2003)");
                        wszystkieDaneZWojewodztw_paliwa.remove("ON (do 2003)");
                        wszystkieDaneZWojewodztw_paliwa.put("ON (do 2003)", nowa_wartosc);
                    }
                    else {
                        wszystkieDaneZWojewodztw_paliwa.put("ON (do 2003)", (long)row.get(2));
                    }
                }
            }
            else {
                dcd.setValue((long)row.get(2), row.get(0) + " + LPG", "do 2003");
                if(wszystkieDaneZWojewodztw_paliwa.containsKey("PB+LPG (do 2003)")) {
                    long nowa_wartosc = (long)row.get(2)
                            + wszystkieDaneZWojewodztw_paliwa.get("PB+LPG (do 2003)");
                    wszystkieDaneZWojewodztw_paliwa.remove("PB+LPG (do 2003)");
                    wszystkieDaneZWojewodztw_paliwa.put("PB+LPG (do 2003)", nowa_wartosc);
                }
                else {
                    wszystkieDaneZWojewodztw_paliwa.put("PB+LPG (do 2003)", (long)row.get(2));
                }
            }
        }
        for(Row row : paliwa_gt2003leq2013) {
            if(row.get(1) == null) {
                dcd.setValue((long)row.get(2), (String)row.get(0), "2004 - 2013");
                if(row.get(0).equals("BENZYNA")) {
                    if(wszystkieDaneZWojewodztw_paliwa.containsKey("PB (2004 - 2013)")) {
                        long nowa_wartosc = (long)row.get(2)
                                + wszystkieDaneZWojewodztw_paliwa.get("PB (2004 - 2013)");
                        wszystkieDaneZWojewodztw_paliwa.remove("PB (2004 - 2013)");
                        wszystkieDaneZWojewodztw_paliwa.put("PB (2004 - 2013)", nowa_wartosc);
                    }
                    else {
                        wszystkieDaneZWojewodztw_paliwa.put("PB (2004 - 2013)", (long)row.get(2));
                    }
                }
                else if(row.get(0).equals("OLEJ NAPĘDOWY")){
                    if(wszystkieDaneZWojewodztw_paliwa.containsKey("ON (2004 - 2013)")) {
                        long nowa_wartosc = (long)row.get(2)
                                + wszystkieDaneZWojewodztw_paliwa.get("ON (2004 - 2013)");
                        wszystkieDaneZWojewodztw_paliwa.remove("ON (2004 - 2013)");
                        wszystkieDaneZWojewodztw_paliwa.put("ON (2004 - 2013)", nowa_wartosc);
                    }
                    else {
                        wszystkieDaneZWojewodztw_paliwa.put("ON (2004 - 2013)", (long)row.get(2));
                    }
                }
            }
            else {
                dcd.setValue((long)row.get(2), row.get(0) + " + LPG", "2004 - 2013");
                if(wszystkieDaneZWojewodztw_paliwa.containsKey("PB+LPG (2004 - 2013)")) {
                    long nowa_wartosc = (long)row.get(2)
                            + wszystkieDaneZWojewodztw_paliwa.get("PB+LPG (2004 - 2013)");
                    wszystkieDaneZWojewodztw_paliwa.remove("PB+LPG (2004 - 2013)");
                    wszystkieDaneZWojewodztw_paliwa.put("PB+LPG (2004 - 2013)", nowa_wartosc);
                }
                else {
                    wszystkieDaneZWojewodztw_paliwa.put("PB+LPG (2004 - 2013)", (long)row.get(2));
                }
            }
        }
        for(Row row : paliwa_gt2013) {
            if(row.get(1) == null) {
                dcd.setValue((long)row.get(2), (String)row.get(0), "od 2014");
                if(row.get(0).equals("BENZYNA")) {
                    if(wszystkieDaneZWojewodztw_paliwa.containsKey("PB (od 2014)")) {
                        long nowa_wartosc = (long)row.get(2)
                                + wszystkieDaneZWojewodztw_paliwa.get("PB (od 2014)");
                        wszystkieDaneZWojewodztw_paliwa.remove("PB (od 2014)");
                        wszystkieDaneZWojewodztw_paliwa.put("PB (od 2014)", nowa_wartosc);
                    }
                    else {
                        wszystkieDaneZWojewodztw_paliwa.put("PB (od 2014)", (long)row.get(2));
                    }
                }
                else if(row.get(0).equals("OLEJ NAPĘDOWY")){
                    if(wszystkieDaneZWojewodztw_paliwa.containsKey("ON (od 2014)")) {
                        long nowa_wartosc = (long)row.get(2)
                                + wszystkieDaneZWojewodztw_paliwa.get("ON (od 2014)");
                        wszystkieDaneZWojewodztw_paliwa.remove("ON (od 2014)");
                        wszystkieDaneZWojewodztw_paliwa.put("ON (od 2014)", nowa_wartosc);
                    }
                    else {
                        wszystkieDaneZWojewodztw_paliwa.put("ON (od 2014)", (long)row.get(2));
                    }
                }
            }
            else {
                dcd.setValue((long)row.get(2), row.get(0) + " + LPG", "od 2014");
                if(wszystkieDaneZWojewodztw_paliwa.containsKey("PB+LPG (od 2014)")) {
                    long nowa_wartosc = (long)row.get(2)
                            + wszystkieDaneZWojewodztw_paliwa.get("PB+LPG (od 2014)");
                    wszystkieDaneZWojewodztw_paliwa.remove("PB+LPG (od 2014)");
                    wszystkieDaneZWojewodztw_paliwa.put("PB+LPG (od 2014)", nowa_wartosc);
                }
                else {
                    wszystkieDaneZWojewodztw_paliwa.put("PB+LPG (od 2014)", (long)row.get(2));
                }
            }
        }

        JFreeChart chart2 = ChartFactory.createBarChart(
                "Podział samochodów z poszczególnych roczników względem rodzaju paliwa" +
                        " - województwo dolnośląskie",
                "Rok produkcji",
                "Ilość pojazdów",
                dcd,
                PlotOrientation.VERTICAL,
                true,
                true,
                false
        );

        CategoryPlot plot2 = (CategoryPlot) chart2.getPlot();
        plot2.setForegroundAlpha(0.7f);
        plot2.getRenderer().setSeriesPaint(0, Color.decode("#006ff9"));
        plot2.getRenderer().setSeriesPaint(1, Color.ORANGE);
        plot2.getRenderer().setSeriesPaint(2, Color.DARK_GRAY);
        plot2.setRangeGridlinePaint(Color.BLACK);
        plot2.setBackgroundPaint(Color.WHITE);
        BarRenderer renderer = (BarRenderer) plot2.getRenderer();
        renderer.setItemMargin(.1);

        try {
            ChartUtilities.saveChartAsPNG(
                    new File("F:\\II stopień\\Semestr IV\\Praca magisterska\\Analiza danych\\vehicle_vintages_survey\\dolnoslaskie_paliwa.png"),
                    chart2,
                    500,
                    400
            );
        } catch (IOException e) {
            System.out.println("Wystapil problem podczas tworzenia wykresu");
        }

        JFrame frame = new JFrame("Popularność poszczególnych roczników pojazdów względem" +
                " rodzaju paliwa - województwo dolnośląskie");
        frame.setLayout(new FlowLayout());
        frame.getContentPane().add(new ChartPanel(chart));
        frame.getContentPane().add(new ChartPanel(chart2));
        frame.pack();
        frame.setVisible(true);
    }

    public void popularity_of_vintages_of_vehicles_and_fuels_woj_kujawskopomorskie() {
        // Obliczanie ilosci samochodow osobowych w danych latach produkcji
        Dataset<Row> samochody_zarej_rp = cepikData.getWojewodztwo_kujawskopomorskie()
                .filter(col("rodzaj").equalTo("SAMOCHÓD OSOBOWY")
                        .and(col("data_wyrejestrowania").isNull()))
                .filter(col("rok_produkcji").isNotNull());

        Dataset<Row> pojazdy_leq2003 = samochody_zarej_rp
                .filter(col("rok_produkcji").leq(2003));

        Dataset<Row> pojazdy_gt2003leq2013 = samochody_zarej_rp
                .filter(col("rok_produkcji").gt(2003)
                        .and(col("rok_produkcji").leq(2013)));

        Dataset<Row> pojazdy_gt2013 = samochody_zarej_rp
                .filter(col("rok_produkcji").gt(2013));

        long liczba_leq2003 = pojazdy_leq2003.count();
        long liczba_gt2003leq2013 = pojazdy_gt2003leq2013.count();
        long liczba_gt2013 = pojazdy_gt2013.count();

        if(wszystkieDaneZWojewodztw_roczniki.containsKey("do 2003")) {
            long nowa_wartosc = liczba_leq2003 + wszystkieDaneZWojewodztw_roczniki.get("do 2003");
            wszystkieDaneZWojewodztw_roczniki.remove("do 2003");
            wszystkieDaneZWojewodztw_roczniki.put("do 2003", nowa_wartosc);
        }
        else {
            wszystkieDaneZWojewodztw_roczniki.put("do 2003", liczba_leq2003);
        }

        if(wszystkieDaneZWojewodztw_roczniki.containsKey("2004 - 2013")) {
            long nowa_wartosc = liczba_gt2003leq2013 + wszystkieDaneZWojewodztw_roczniki.get("2004 - 2013");
            wszystkieDaneZWojewodztw_roczniki.remove("2004 - 2013");
            wszystkieDaneZWojewodztw_roczniki.put("2004 - 2013", nowa_wartosc);
        }
        else {
            wszystkieDaneZWojewodztw_roczniki.put("2004 - 2013", liczba_gt2003leq2013);
        }

        if(wszystkieDaneZWojewodztw_roczniki.containsKey("od 2014")) {
            long nowa_wartosc = liczba_gt2013 + wszystkieDaneZWojewodztw_roczniki.get("od 2014");
            wszystkieDaneZWojewodztw_roczniki.remove("od 2014");
            wszystkieDaneZWojewodztw_roczniki.put("od 2014", nowa_wartosc);
        }
        else {
            wszystkieDaneZWojewodztw_roczniki.put("od 2014", liczba_gt2013);
        }

        // Tworzenie wykresu 1 - Porównanie popularności aut z poszczególnych roczników - województwo lubelskie
        DefaultPieDataset dpd = new DefaultPieDataset();
        dpd.setValue("do 2003", liczba_leq2003);
        dpd.setValue("2004 - 2013", liczba_gt2003leq2013);
        dpd.setValue("od 2014", liczba_gt2013);

        JFreeChart chart = ChartFactory.createPieChart(
                "Porównanie popularności aut z poszczególnych roczników" +
                        " - województwo kujawsko-pomorskie",
                dpd,
                true,
                true,
                false
        );

        PiePlot plot = (PiePlot) chart.getPlot();
        plot.setSectionPaint("do 2003", Color.decode("#0096ff"));
        plot.setSectionPaint("2004 - 2013", Color.BLACK);
        plot.setSectionPaint("od 2014", Color.YELLOW);
        plot.setForegroundAlpha(0.5f);
        plot.setExplodePercent("do 2003", 0.20);
        plot.setBackgroundPaint(Color.WHITE);
        plot.setSimpleLabels(true);

        PieSectionLabelGenerator gen = new StandardPieSectionLabelGenerator(
                "{0}: {1} ({2})", new DecimalFormat("0"), new DecimalFormat("0%"));
        plot.setLabelGenerator(gen);

        try {
            ChartUtilities.saveChartAsPNG(
                    new File("F:\\II stopień\\Semestr IV\\Praca magisterska\\Analiza danych\\vehicle_vintages_survey\\kujawskopomorskie_ilosc.png"),
                    chart,
                    500,
                    400);
        } catch (Exception e) {
            System.out.println("Wystapil problem podczas tworzenia wykresu");
        }

        // Obliczanie ilosci pojazdow na dany rodzaj paliwa w danym przedziale lat produkcji
        List<Row> paliwa_leq2003 = pojazdy_leq2003
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

        List<Row> paliwa_gt2003leq2013 = pojazdy_gt2003leq2013
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

        List<Row> paliwa_gt2013 = pojazdy_gt2013
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

        // Tworzenie wykresu 2 - Podział samochodów z poszczególnych roczników
        // względem rodzaju paliwa - województwo lubelskie
        DefaultCategoryDataset dcd = new DefaultCategoryDataset();

        for(Row row : paliwa_leq2003) {
            if(row.get(1) == null) {
                dcd.setValue((long)row.get(2), (String)row.get(0), "do 2003");
                if(row.get(0).equals("BENZYNA")) {
                    if(wszystkieDaneZWojewodztw_paliwa.containsKey("PB (do 2003)")) {
                        long nowa_wartosc = (long)row.get(2)
                                + wszystkieDaneZWojewodztw_paliwa.get("PB (do 2003)");
                        wszystkieDaneZWojewodztw_paliwa.remove("PB (do 2003)");
                        wszystkieDaneZWojewodztw_paliwa.put("PB (do 2003)", nowa_wartosc);
                    }
                    else {
                        wszystkieDaneZWojewodztw_paliwa.put("PB (do 2003)", (long)row.get(2));
                    }
                }
                else if(row.get(0).equals("OLEJ NAPĘDOWY")){
                    if(wszystkieDaneZWojewodztw_paliwa.containsKey("ON (do 2003)")) {
                        long nowa_wartosc = (long)row.get(2)
                                + wszystkieDaneZWojewodztw_paliwa.get("ON (do 2003)");
                        wszystkieDaneZWojewodztw_paliwa.remove("ON (do 2003)");
                        wszystkieDaneZWojewodztw_paliwa.put("ON (do 2003)", nowa_wartosc);
                    }
                    else {
                        wszystkieDaneZWojewodztw_paliwa.put("ON (do 2003)", (long)row.get(2));
                    }
                }
            }
            else {
                dcd.setValue((long)row.get(2), row.get(0) + " + LPG", "do 2003");
                if(wszystkieDaneZWojewodztw_paliwa.containsKey("PB+LPG (do 2003)")) {
                    long nowa_wartosc = (long)row.get(2)
                            + wszystkieDaneZWojewodztw_paliwa.get("PB+LPG (do 2003)");
                    wszystkieDaneZWojewodztw_paliwa.remove("PB+LPG (do 2003)");
                    wszystkieDaneZWojewodztw_paliwa.put("PB+LPG (do 2003)", nowa_wartosc);
                }
                else {
                    wszystkieDaneZWojewodztw_paliwa.put("PB+LPG (do 2003)", (long)row.get(2));
                }
            }
        }
        for(Row row : paliwa_gt2003leq2013) {
            if(row.get(1) == null) {
                dcd.setValue((long)row.get(2), (String)row.get(0), "2004 - 2013");
                if(row.get(0).equals("BENZYNA")) {
                    if(wszystkieDaneZWojewodztw_paliwa.containsKey("PB (2004 - 2013)")) {
                        long nowa_wartosc = (long)row.get(2)
                                + wszystkieDaneZWojewodztw_paliwa.get("PB (2004 - 2013)");
                        wszystkieDaneZWojewodztw_paliwa.remove("PB (2004 - 2013)");
                        wszystkieDaneZWojewodztw_paliwa.put("PB (2004 - 2013)", nowa_wartosc);
                    }
                    else {
                        wszystkieDaneZWojewodztw_paliwa.put("PB (2004 - 2013)", (long)row.get(2));
                    }
                }
                else if(row.get(0).equals("OLEJ NAPĘDOWY")){
                    if(wszystkieDaneZWojewodztw_paliwa.containsKey("ON (2004 - 2013)")) {
                        long nowa_wartosc = (long)row.get(2)
                                + wszystkieDaneZWojewodztw_paliwa.get("ON (2004 - 2013)");
                        wszystkieDaneZWojewodztw_paliwa.remove("ON (2004 - 2013)");
                        wszystkieDaneZWojewodztw_paliwa.put("ON (2004 - 2013)", nowa_wartosc);
                    }
                    else {
                        wszystkieDaneZWojewodztw_paliwa.put("ON (2004 - 2013)", (long)row.get(2));
                    }
                }
            }
            else {
                dcd.setValue((long)row.get(2), row.get(0) + " + LPG", "2004 - 2013");
                if(wszystkieDaneZWojewodztw_paliwa.containsKey("PB+LPG (2004 - 2013)")) {
                    long nowa_wartosc = (long)row.get(2)
                            + wszystkieDaneZWojewodztw_paliwa.get("PB+LPG (2004 - 2013)");
                    wszystkieDaneZWojewodztw_paliwa.remove("PB+LPG (2004 - 2013)");
                    wszystkieDaneZWojewodztw_paliwa.put("PB+LPG (2004 - 2013)", nowa_wartosc);
                }
                else {
                    wszystkieDaneZWojewodztw_paliwa.put("PB+LPG (2004 - 2013)", (long)row.get(2));
                }
            }
        }
        for(Row row : paliwa_gt2013) {
            if(row.get(1) == null) {
                dcd.setValue((long)row.get(2), (String)row.get(0), "od 2014");
                if(row.get(0).equals("BENZYNA")) {
                    if(wszystkieDaneZWojewodztw_paliwa.containsKey("PB (od 2014)")) {
                        long nowa_wartosc = (long)row.get(2)
                                + wszystkieDaneZWojewodztw_paliwa.get("PB (od 2014)");
                        wszystkieDaneZWojewodztw_paliwa.remove("PB (od 2014)");
                        wszystkieDaneZWojewodztw_paliwa.put("PB (od 2014)", nowa_wartosc);
                    }
                    else {
                        wszystkieDaneZWojewodztw_paliwa.put("PB (od 2014)", (long)row.get(2));
                    }
                }
                else if(row.get(0).equals("OLEJ NAPĘDOWY")){
                    if(wszystkieDaneZWojewodztw_paliwa.containsKey("ON (od 2014)")) {
                        long nowa_wartosc = (long)row.get(2)
                                + wszystkieDaneZWojewodztw_paliwa.get("ON (od 2014)");
                        wszystkieDaneZWojewodztw_paliwa.remove("ON (od 2014)");
                        wszystkieDaneZWojewodztw_paliwa.put("ON (od 2014)", nowa_wartosc);
                    }
                    else {
                        wszystkieDaneZWojewodztw_paliwa.put("ON (od 2014)", (long)row.get(2));
                    }
                }
            }
            else {
                dcd.setValue((long)row.get(2), row.get(0) + " + LPG", "od 2014");
                if(wszystkieDaneZWojewodztw_paliwa.containsKey("PB+LPG (od 2014)")) {
                    long nowa_wartosc = (long)row.get(2)
                            + wszystkieDaneZWojewodztw_paliwa.get("PB+LPG (od 2014)");
                    wszystkieDaneZWojewodztw_paliwa.remove("PB+LPG (od 2014)");
                    wszystkieDaneZWojewodztw_paliwa.put("PB+LPG (od 2014)", nowa_wartosc);
                }
                else {
                    wszystkieDaneZWojewodztw_paliwa.put("PB+LPG (od 2014)", (long)row.get(2));
                }
            }
        }

        JFreeChart chart2 = ChartFactory.createBarChart(
                "Podział samochodów z poszczególnych roczników względem rodzaju paliwa" +
                        " - województwo kujawsko-pomorskie",
                "Rok produkcji",
                "Ilość pojazdów",
                dcd,
                PlotOrientation.VERTICAL,
                true,
                true,
                false
        );

        CategoryPlot plot2 = (CategoryPlot) chart2.getPlot();
        plot2.setForegroundAlpha(0.7f);
        plot2.getRenderer().setSeriesPaint(0, Color.decode("#006ff9"));
        plot2.getRenderer().setSeriesPaint(1, Color.ORANGE);
        plot2.getRenderer().setSeriesPaint(2, Color.DARK_GRAY);
        plot2.setRangeGridlinePaint(Color.BLACK);
        plot2.setBackgroundPaint(Color.WHITE);
        BarRenderer renderer = (BarRenderer) plot2.getRenderer();
        renderer.setItemMargin(.1);

        try {
            ChartUtilities.saveChartAsPNG(
                    new File("F:\\II stopień\\Semestr IV\\Praca magisterska\\Analiza danych\\vehicle_vintages_survey\\kujawskopomorskie_paliwa.png"),
                    chart2,
                    500,
                    400
            );
        } catch (IOException e) {
            System.out.println("Wystapil problem podczas tworzenia wykresu");
        }

        JFrame frame = new JFrame("Popularność poszczególnych roczników pojazdów względem" +
                " rodzaju paliwa - województwo kujawsko-pomorskie");
        frame.setLayout(new FlowLayout());
        frame.getContentPane().add(new ChartPanel(chart));
        frame.getContentPane().add(new ChartPanel(chart2));
        frame.pack();
        frame.setVisible(true);
    }

    public void popularity_of_vintages_of_vehicles_and_fuels_woj_lubuskie() {
        // Obliczanie ilosci samochodow osobowych w danych latach produkcji
        Dataset<Row> samochody_zarej_rp = cepikData.getWojewodztwo_lubuskie()
                .filter(col("rodzaj").equalTo("SAMOCHÓD OSOBOWY")
                        .and(col("data_wyrejestrowania").isNull()))
                .filter(col("rok_produkcji").isNotNull());

        Dataset<Row> pojazdy_leq2003 = samochody_zarej_rp
                .filter(col("rok_produkcji").leq(2003));

        Dataset<Row> pojazdy_gt2003leq2013 = samochody_zarej_rp
                .filter(col("rok_produkcji").gt(2003)
                        .and(col("rok_produkcji").leq(2013)));

        Dataset<Row> pojazdy_gt2013 = samochody_zarej_rp
                .filter(col("rok_produkcji").gt(2013));

        long liczba_leq2003 = pojazdy_leq2003.count();
        long liczba_gt2003leq2013 = pojazdy_gt2003leq2013.count();
        long liczba_gt2013 = pojazdy_gt2013.count();

        if(wszystkieDaneZWojewodztw_roczniki.containsKey("do 2003")) {
            long nowa_wartosc = liczba_leq2003 + wszystkieDaneZWojewodztw_roczniki.get("do 2003");
            wszystkieDaneZWojewodztw_roczniki.remove("do 2003");
            wszystkieDaneZWojewodztw_roczniki.put("do 2003", nowa_wartosc);
        }
        else {
            wszystkieDaneZWojewodztw_roczniki.put("do 2003", liczba_leq2003);
        }

        if(wszystkieDaneZWojewodztw_roczniki.containsKey("2004 - 2013")) {
            long nowa_wartosc = liczba_gt2003leq2013 + wszystkieDaneZWojewodztw_roczniki.get("2004 - 2013");
            wszystkieDaneZWojewodztw_roczniki.remove("2004 - 2013");
            wszystkieDaneZWojewodztw_roczniki.put("2004 - 2013", nowa_wartosc);
        }
        else {
            wszystkieDaneZWojewodztw_roczniki.put("2004 - 2013", liczba_gt2003leq2013);
        }

        if(wszystkieDaneZWojewodztw_roczniki.containsKey("od 2014")) {
            long nowa_wartosc = liczba_gt2013 + wszystkieDaneZWojewodztw_roczniki.get("od 2014");
            wszystkieDaneZWojewodztw_roczniki.remove("od 2014");
            wszystkieDaneZWojewodztw_roczniki.put("od 2014", nowa_wartosc);
        }
        else {
            wszystkieDaneZWojewodztw_roczniki.put("od 2014", liczba_gt2013);
        }

        // Tworzenie wykresu 1 - Porównanie popularności aut z poszczególnych roczników - województwo lubelskie
        DefaultPieDataset dpd = new DefaultPieDataset();
        dpd.setValue("do 2003", liczba_leq2003);
        dpd.setValue("2004 - 2013", liczba_gt2003leq2013);
        dpd.setValue("od 2014", liczba_gt2013);

        JFreeChart chart = ChartFactory.createPieChart(
                "Porównanie popularności aut z poszczególnych roczników" +
                        " - województwo lubuskie",
                dpd,
                true,
                true,
                false
        );

        PiePlot plot = (PiePlot) chart.getPlot();
        plot.setSectionPaint("do 2003", Color.decode("#0096ff"));
        plot.setSectionPaint("2004 - 2013", Color.BLACK);
        plot.setSectionPaint("od 2014", Color.YELLOW);
        plot.setForegroundAlpha(0.5f);
        plot.setExplodePercent("do 2003", 0.20);
        plot.setBackgroundPaint(Color.WHITE);
        plot.setSimpleLabels(true);

        PieSectionLabelGenerator gen = new StandardPieSectionLabelGenerator(
                "{0}: {1} ({2})", new DecimalFormat("0"), new DecimalFormat("0%"));
        plot.setLabelGenerator(gen);

        try {
            ChartUtilities.saveChartAsPNG(
                    new File("F:\\II stopień\\Semestr IV\\Praca magisterska\\Analiza danych\\vehicle_vintages_survey\\lubuskie_ilosc.png"),
                    chart,
                    500,
                    400);
        } catch (Exception e) {
            System.out.println("Wystapil problem podczas tworzenia wykresu");
        }

        // Obliczanie ilosci pojazdow na dany rodzaj paliwa w danym przedziale lat produkcji
        List<Row> paliwa_leq2003 = pojazdy_leq2003
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

        List<Row> paliwa_gt2003leq2013 = pojazdy_gt2003leq2013
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

        List<Row> paliwa_gt2013 = pojazdy_gt2013
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

        // Tworzenie wykresu 2 - Podział samochodów z poszczególnych roczników
        // względem rodzaju paliwa - województwo lubelskie
        DefaultCategoryDataset dcd = new DefaultCategoryDataset();

        for(Row row : paliwa_leq2003) {
            if(row.get(1) == null) {
                dcd.setValue((long)row.get(2), (String)row.get(0), "do 2003");
                if(row.get(0).equals("BENZYNA")) {
                    if(wszystkieDaneZWojewodztw_paliwa.containsKey("PB (do 2003)")) {
                        long nowa_wartosc = (long)row.get(2)
                                + wszystkieDaneZWojewodztw_paliwa.get("PB (do 2003)");
                        wszystkieDaneZWojewodztw_paliwa.remove("PB (do 2003)");
                        wszystkieDaneZWojewodztw_paliwa.put("PB (do 2003)", nowa_wartosc);
                    }
                    else {
                        wszystkieDaneZWojewodztw_paliwa.put("PB (do 2003)", (long)row.get(2));
                    }
                }
                else if(row.get(0).equals("OLEJ NAPĘDOWY")){
                    if(wszystkieDaneZWojewodztw_paliwa.containsKey("ON (do 2003)")) {
                        long nowa_wartosc = (long)row.get(2)
                                + wszystkieDaneZWojewodztw_paliwa.get("ON (do 2003)");
                        wszystkieDaneZWojewodztw_paliwa.remove("ON (do 2003)");
                        wszystkieDaneZWojewodztw_paliwa.put("ON (do 2003)", nowa_wartosc);
                    }
                    else {
                        wszystkieDaneZWojewodztw_paliwa.put("ON (do 2003)", (long)row.get(2));
                    }
                }
            }
            else {
                dcd.setValue((long)row.get(2), row.get(0) + " + LPG", "do 2003");
                if(wszystkieDaneZWojewodztw_paliwa.containsKey("PB+LPG (do 2003)")) {
                    long nowa_wartosc = (long)row.get(2)
                            + wszystkieDaneZWojewodztw_paliwa.get("PB+LPG (do 2003)");
                    wszystkieDaneZWojewodztw_paliwa.remove("PB+LPG (do 2003)");
                    wszystkieDaneZWojewodztw_paliwa.put("PB+LPG (do 2003)", nowa_wartosc);
                }
                else {
                    wszystkieDaneZWojewodztw_paliwa.put("PB+LPG (do 2003)", (long)row.get(2));
                }
            }
        }
        for(Row row : paliwa_gt2003leq2013) {
            if(row.get(1) == null) {
                dcd.setValue((long)row.get(2), (String)row.get(0), "2004 - 2013");
                if(row.get(0).equals("BENZYNA")) {
                    if(wszystkieDaneZWojewodztw_paliwa.containsKey("PB (2004 - 2013)")) {
                        long nowa_wartosc = (long)row.get(2)
                                + wszystkieDaneZWojewodztw_paliwa.get("PB (2004 - 2013)");
                        wszystkieDaneZWojewodztw_paliwa.remove("PB (2004 - 2013)");
                        wszystkieDaneZWojewodztw_paliwa.put("PB (2004 - 2013)", nowa_wartosc);
                    }
                    else {
                        wszystkieDaneZWojewodztw_paliwa.put("PB (2004 - 2013)", (long)row.get(2));
                    }
                }
                else if(row.get(0).equals("OLEJ NAPĘDOWY")){
                    if(wszystkieDaneZWojewodztw_paliwa.containsKey("ON (2004 - 2013)")) {
                        long nowa_wartosc = (long)row.get(2)
                                + wszystkieDaneZWojewodztw_paliwa.get("ON (2004 - 2013)");
                        wszystkieDaneZWojewodztw_paliwa.remove("ON (2004 - 2013)");
                        wszystkieDaneZWojewodztw_paliwa.put("ON (2004 - 2013)", nowa_wartosc);
                    }
                    else {
                        wszystkieDaneZWojewodztw_paliwa.put("ON (2004 - 2013)", (long)row.get(2));
                    }
                }
            }
            else {
                dcd.setValue((long)row.get(2), row.get(0) + " + LPG", "2004 - 2013");
                if(wszystkieDaneZWojewodztw_paliwa.containsKey("PB+LPG (2004 - 2013)")) {
                    long nowa_wartosc = (long)row.get(2)
                            + wszystkieDaneZWojewodztw_paliwa.get("PB+LPG (2004 - 2013)");
                    wszystkieDaneZWojewodztw_paliwa.remove("PB+LPG (2004 - 2013)");
                    wszystkieDaneZWojewodztw_paliwa.put("PB+LPG (2004 - 2013)", nowa_wartosc);
                }
                else {
                    wszystkieDaneZWojewodztw_paliwa.put("PB+LPG (2004 - 2013)", (long)row.get(2));
                }
            }
        }
        for(Row row : paliwa_gt2013) {
            if(row.get(1) == null) {
                dcd.setValue((long)row.get(2), (String)row.get(0), "od 2014");
                if(row.get(0).equals("BENZYNA")) {
                    if(wszystkieDaneZWojewodztw_paliwa.containsKey("PB (od 2014)")) {
                        long nowa_wartosc = (long)row.get(2)
                                + wszystkieDaneZWojewodztw_paliwa.get("PB (od 2014)");
                        wszystkieDaneZWojewodztw_paliwa.remove("PB (od 2014)");
                        wszystkieDaneZWojewodztw_paliwa.put("PB (od 2014)", nowa_wartosc);
                    }
                    else {
                        wszystkieDaneZWojewodztw_paliwa.put("PB (od 2014)", (long)row.get(2));
                    }
                }
                else if(row.get(0).equals("OLEJ NAPĘDOWY")){
                    if(wszystkieDaneZWojewodztw_paliwa.containsKey("ON (od 2014)")) {
                        long nowa_wartosc = (long)row.get(2)
                                + wszystkieDaneZWojewodztw_paliwa.get("ON (od 2014)");
                        wszystkieDaneZWojewodztw_paliwa.remove("ON (od 2014)");
                        wszystkieDaneZWojewodztw_paliwa.put("ON (od 2014)", nowa_wartosc);
                    }
                    else {
                        wszystkieDaneZWojewodztw_paliwa.put("ON (od 2014)", (long)row.get(2));
                    }
                }
            }
            else {
                dcd.setValue((long)row.get(2), row.get(0) + " + LPG", "od 2014");
                if(wszystkieDaneZWojewodztw_paliwa.containsKey("PB+LPG (od 2014)")) {
                    long nowa_wartosc = (long)row.get(2)
                            + wszystkieDaneZWojewodztw_paliwa.get("PB+LPG (od 2014)");
                    wszystkieDaneZWojewodztw_paliwa.remove("PB+LPG (od 2014)");
                    wszystkieDaneZWojewodztw_paliwa.put("PB+LPG (od 2014)", nowa_wartosc);
                }
                else {
                    wszystkieDaneZWojewodztw_paliwa.put("PB+LPG (od 2014)", (long)row.get(2));
                }
            }
        }

        JFreeChart chart2 = ChartFactory.createBarChart(
                "Podział samochodów z poszczególnych roczników względem rodzaju paliwa" +
                        " - województwo lubuskie",
                "Rok produkcji",
                "Ilość pojazdów",
                dcd,
                PlotOrientation.VERTICAL,
                true,
                true,
                false
        );

        CategoryPlot plot2 = (CategoryPlot) chart2.getPlot();
        plot2.setForegroundAlpha(0.7f);
        plot2.getRenderer().setSeriesPaint(0, Color.decode("#006ff9"));
        plot2.getRenderer().setSeriesPaint(1, Color.ORANGE);
        plot2.getRenderer().setSeriesPaint(2, Color.DARK_GRAY);
        plot2.setRangeGridlinePaint(Color.BLACK);
        plot2.setBackgroundPaint(Color.WHITE);
        BarRenderer renderer = (BarRenderer) plot2.getRenderer();
        renderer.setItemMargin(.1);

        try {
            ChartUtilities.saveChartAsPNG(
                    new File("F:\\II stopień\\Semestr IV\\Praca magisterska\\Analiza danych\\vehicle_vintages_survey\\lubuskie_paliwa.png"),
                    chart2,
                    500,
                    400
            );
        } catch (IOException e) {
            System.out.println("Wystapil problem podczas tworzenia wykresu");
        }

        JFrame frame = new JFrame("Popularność poszczególnych roczników pojazdów względem" +
                " rodzaju paliwa - województwo lubuskie");
        frame.setLayout(new FlowLayout());
        frame.getContentPane().add(new ChartPanel(chart));
        frame.getContentPane().add(new ChartPanel(chart2));
        frame.pack();
        frame.setVisible(true);
    }

    public void popularity_of_vintages_of_vehicles_and_fuels_woj_lodzkie() {
        // Obliczanie ilosci samochodow osobowych w danych latach produkcji
        Dataset<Row> samochody_zarej_rp = cepikData.getWojewodztwo_lodzkie()
                .filter(col("rodzaj").equalTo("SAMOCHÓD OSOBOWY")
                        .and(col("data_wyrejestrowania").isNull()))
                .filter(col("rok_produkcji").isNotNull());

        Dataset<Row> pojazdy_leq2003 = samochody_zarej_rp
                .filter(col("rok_produkcji").leq(2003));

        Dataset<Row> pojazdy_gt2003leq2013 = samochody_zarej_rp
                .filter(col("rok_produkcji").gt(2003)
                        .and(col("rok_produkcji").leq(2013)));

        Dataset<Row> pojazdy_gt2013 = samochody_zarej_rp
                .filter(col("rok_produkcji").gt(2013));

        long liczba_leq2003 = pojazdy_leq2003.count();
        long liczba_gt2003leq2013 = pojazdy_gt2003leq2013.count();
        long liczba_gt2013 = pojazdy_gt2013.count();

        if(wszystkieDaneZWojewodztw_roczniki.containsKey("do 2003")) {
            long nowa_wartosc = liczba_leq2003 + wszystkieDaneZWojewodztw_roczniki.get("do 2003");
            wszystkieDaneZWojewodztw_roczniki.remove("do 2003");
            wszystkieDaneZWojewodztw_roczniki.put("do 2003", nowa_wartosc);
        }
        else {
            wszystkieDaneZWojewodztw_roczniki.put("do 2003", liczba_leq2003);
        }

        if(wszystkieDaneZWojewodztw_roczniki.containsKey("2004 - 2013")) {
            long nowa_wartosc = liczba_gt2003leq2013 + wszystkieDaneZWojewodztw_roczniki.get("2004 - 2013");
            wszystkieDaneZWojewodztw_roczniki.remove("2004 - 2013");
            wszystkieDaneZWojewodztw_roczniki.put("2004 - 2013", nowa_wartosc);
        }
        else {
            wszystkieDaneZWojewodztw_roczniki.put("2004 - 2013", liczba_gt2003leq2013);
        }

        if(wszystkieDaneZWojewodztw_roczniki.containsKey("od 2014")) {
            long nowa_wartosc = liczba_gt2013 + wszystkieDaneZWojewodztw_roczniki.get("od 2014");
            wszystkieDaneZWojewodztw_roczniki.remove("od 2014");
            wszystkieDaneZWojewodztw_roczniki.put("od 2014", nowa_wartosc);
        }
        else {
            wszystkieDaneZWojewodztw_roczniki.put("od 2014", liczba_gt2013);
        }

        // Tworzenie wykresu 1 - Porównanie popularności aut z poszczególnych roczników - województwo lubelskie
        DefaultPieDataset dpd = new DefaultPieDataset();
        dpd.setValue("do 2003", liczba_leq2003);
        dpd.setValue("2004 - 2013", liczba_gt2003leq2013);
        dpd.setValue("od 2014", liczba_gt2013);

        JFreeChart chart = ChartFactory.createPieChart(
                "Porównanie popularności aut z poszczególnych roczników" +
                        " - województwo łódzkie",
                dpd,
                true,
                true,
                false
        );

        PiePlot plot = (PiePlot) chart.getPlot();
        plot.setSectionPaint("do 2003", Color.decode("#0096ff"));
        plot.setSectionPaint("2004 - 2013", Color.BLACK);
        plot.setSectionPaint("od 2014", Color.YELLOW);
        plot.setForegroundAlpha(0.5f);
        plot.setExplodePercent("do 2003", 0.20);
        plot.setBackgroundPaint(Color.WHITE);
        plot.setSimpleLabels(true);

        PieSectionLabelGenerator gen = new StandardPieSectionLabelGenerator(
                "{0}: {1} ({2})", new DecimalFormat("0"), new DecimalFormat("0%"));
        plot.setLabelGenerator(gen);

        try {
            ChartUtilities.saveChartAsPNG(
                    new File("F:\\II stopień\\Semestr IV\\Praca magisterska\\Analiza danych\\vehicle_vintages_survey\\lodzkie_ilosc.png"),
                    chart,
                    500,
                    400);
        } catch (Exception e) {
            System.out.println("Wystapil problem podczas tworzenia wykresu");
        }

        // Obliczanie ilosci pojazdow na dany rodzaj paliwa w danym przedziale lat produkcji
        List<Row> paliwa_leq2003 = pojazdy_leq2003
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

        List<Row> paliwa_gt2003leq2013 = pojazdy_gt2003leq2013
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

        List<Row> paliwa_gt2013 = pojazdy_gt2013
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

        // Tworzenie wykresu 2 - Podział samochodów z poszczególnych roczników
        // względem rodzaju paliwa - województwo lubelskie
        DefaultCategoryDataset dcd = new DefaultCategoryDataset();

        for(Row row : paliwa_leq2003) {
            if(row.get(1) == null) {
                dcd.setValue((long)row.get(2), (String)row.get(0), "do 2003");
                if(row.get(0).equals("BENZYNA")) {
                    if(wszystkieDaneZWojewodztw_paliwa.containsKey("PB (do 2003)")) {
                        long nowa_wartosc = (long)row.get(2)
                                + wszystkieDaneZWojewodztw_paliwa.get("PB (do 2003)");
                        wszystkieDaneZWojewodztw_paliwa.remove("PB (do 2003)");
                        wszystkieDaneZWojewodztw_paliwa.put("PB (do 2003)", nowa_wartosc);
                    }
                    else {
                        wszystkieDaneZWojewodztw_paliwa.put("PB (do 2003)", (long)row.get(2));
                    }
                }
                else if(row.get(0).equals("OLEJ NAPĘDOWY")){
                    if(wszystkieDaneZWojewodztw_paliwa.containsKey("ON (do 2003)")) {
                        long nowa_wartosc = (long)row.get(2)
                                + wszystkieDaneZWojewodztw_paliwa.get("ON (do 2003)");
                        wszystkieDaneZWojewodztw_paliwa.remove("ON (do 2003)");
                        wszystkieDaneZWojewodztw_paliwa.put("ON (do 2003)", nowa_wartosc);
                    }
                    else {
                        wszystkieDaneZWojewodztw_paliwa.put("ON (do 2003)", (long)row.get(2));
                    }
                }
            }
            else {
                dcd.setValue((long)row.get(2), row.get(0) + " + LPG", "do 2003");
                if(wszystkieDaneZWojewodztw_paliwa.containsKey("PB+LPG (do 2003)")) {
                    long nowa_wartosc = (long)row.get(2)
                            + wszystkieDaneZWojewodztw_paliwa.get("PB+LPG (do 2003)");
                    wszystkieDaneZWojewodztw_paliwa.remove("PB+LPG (do 2003)");
                    wszystkieDaneZWojewodztw_paliwa.put("PB+LPG (do 2003)", nowa_wartosc);
                }
                else {
                    wszystkieDaneZWojewodztw_paliwa.put("PB+LPG (do 2003)", (long)row.get(2));
                }
            }
        }
        for(Row row : paliwa_gt2003leq2013) {
            if(row.get(1) == null) {
                dcd.setValue((long)row.get(2), (String)row.get(0), "2004 - 2013");
                if(row.get(0).equals("BENZYNA")) {
                    if(wszystkieDaneZWojewodztw_paliwa.containsKey("PB (2004 - 2013)")) {
                        long nowa_wartosc = (long)row.get(2)
                                + wszystkieDaneZWojewodztw_paliwa.get("PB (2004 - 2013)");
                        wszystkieDaneZWojewodztw_paliwa.remove("PB (2004 - 2013)");
                        wszystkieDaneZWojewodztw_paliwa.put("PB (2004 - 2013)", nowa_wartosc);
                    }
                    else {
                        wszystkieDaneZWojewodztw_paliwa.put("PB (2004 - 2013)", (long)row.get(2));
                    }
                }
                else if(row.get(0).equals("OLEJ NAPĘDOWY")){
                    if(wszystkieDaneZWojewodztw_paliwa.containsKey("ON (2004 - 2013)")) {
                        long nowa_wartosc = (long)row.get(2)
                                + wszystkieDaneZWojewodztw_paliwa.get("ON (2004 - 2013)");
                        wszystkieDaneZWojewodztw_paliwa.remove("ON (2004 - 2013)");
                        wszystkieDaneZWojewodztw_paliwa.put("ON (2004 - 2013)", nowa_wartosc);
                    }
                    else {
                        wszystkieDaneZWojewodztw_paliwa.put("ON (2004 - 2013)", (long)row.get(2));
                    }
                }
            }
            else {
                dcd.setValue((long)row.get(2), row.get(0) + " + LPG", "2004 - 2013");
                if(wszystkieDaneZWojewodztw_paliwa.containsKey("PB+LPG (2004 - 2013)")) {
                    long nowa_wartosc = (long)row.get(2)
                            + wszystkieDaneZWojewodztw_paliwa.get("PB+LPG (2004 - 2013)");
                    wszystkieDaneZWojewodztw_paliwa.remove("PB+LPG (2004 - 2013)");
                    wszystkieDaneZWojewodztw_paliwa.put("PB+LPG (2004 - 2013)", nowa_wartosc);
                }
                else {
                    wszystkieDaneZWojewodztw_paliwa.put("PB+LPG (2004 - 2013)", (long)row.get(2));
                }
            }
        }
        for(Row row : paliwa_gt2013) {
            if(row.get(1) == null) {
                dcd.setValue((long)row.get(2), (String)row.get(0), "od 2014");
                if(row.get(0).equals("BENZYNA")) {
                    if(wszystkieDaneZWojewodztw_paliwa.containsKey("PB (od 2014)")) {
                        long nowa_wartosc = (long)row.get(2)
                                + wszystkieDaneZWojewodztw_paliwa.get("PB (od 2014)");
                        wszystkieDaneZWojewodztw_paliwa.remove("PB (od 2014)");
                        wszystkieDaneZWojewodztw_paliwa.put("PB (od 2014)", nowa_wartosc);
                    }
                    else {
                        wszystkieDaneZWojewodztw_paliwa.put("PB (od 2014)", (long)row.get(2));
                    }
                }
                else if(row.get(0).equals("OLEJ NAPĘDOWY")){
                    if(wszystkieDaneZWojewodztw_paliwa.containsKey("ON (od 2014)")) {
                        long nowa_wartosc = (long)row.get(2)
                                + wszystkieDaneZWojewodztw_paliwa.get("ON (od 2014)");
                        wszystkieDaneZWojewodztw_paliwa.remove("ON (od 2014)");
                        wszystkieDaneZWojewodztw_paliwa.put("ON (od 2014)", nowa_wartosc);
                    }
                    else {
                        wszystkieDaneZWojewodztw_paliwa.put("ON (od 2014)", (long)row.get(2));
                    }
                }
            }
            else {
                dcd.setValue((long)row.get(2), row.get(0) + " + LPG", "od 2014");
                if(wszystkieDaneZWojewodztw_paliwa.containsKey("PB+LPG (od 2014)")) {
                    long nowa_wartosc = (long)row.get(2)
                            + wszystkieDaneZWojewodztw_paliwa.get("PB+LPG (od 2014)");
                    wszystkieDaneZWojewodztw_paliwa.remove("PB+LPG (od 2014)");
                    wszystkieDaneZWojewodztw_paliwa.put("PB+LPG (od 2014)", nowa_wartosc);
                }
                else {
                    wszystkieDaneZWojewodztw_paliwa.put("PB+LPG (od 2014)", (long)row.get(2));
                }
            }
        }

        JFreeChart chart2 = ChartFactory.createBarChart(
                "Podział samochodów z poszczególnych roczników względem rodzaju paliwa" +
                        " - województwo łódzkie",
                "Rok produkcji",
                "Ilość pojazdów",
                dcd,
                PlotOrientation.VERTICAL,
                true,
                true,
                false
        );

        CategoryPlot plot2 = (CategoryPlot) chart2.getPlot();
        plot2.setForegroundAlpha(0.7f);
        plot2.getRenderer().setSeriesPaint(0, Color.decode("#006ff9"));
        plot2.getRenderer().setSeriesPaint(1, Color.DARK_GRAY);  // zmiana dla tego wojewodztwa
        plot2.getRenderer().setSeriesPaint(2, Color.ORANGE);
        plot2.setRangeGridlinePaint(Color.BLACK);
        plot2.setBackgroundPaint(Color.WHITE);
        BarRenderer renderer = (BarRenderer) plot2.getRenderer();
        renderer.setItemMargin(.1);

        try {
            ChartUtilities.saveChartAsPNG(
                    new File("F:\\II stopień\\Semestr IV\\Praca magisterska\\Analiza danych\\vehicle_vintages_survey\\lodzkie_paliwa.png"),
                    chart2,
                    500,
                    400
            );
        } catch (IOException e) {
            System.out.println("Wystapil problem podczas tworzenia wykresu");
        }

        JFrame frame = new JFrame("Popularność poszczególnych roczników pojazdów względem" +
                " rodzaju paliwa - województwo łódzkie");
        frame.setLayout(new FlowLayout());
        frame.getContentPane().add(new ChartPanel(chart));
        frame.getContentPane().add(new ChartPanel(chart2));
        frame.pack();
        frame.setVisible(true);
    }

    public void popularity_of_vintages_of_vehicles_and_fuels_woj_malopolskie() {
        // Obliczanie ilosci samochodow osobowych w danych latach produkcji
        Dataset<Row> samochody_zarej_rp = cepikData.getWojewodztwo_malopolskie()
                .filter(col("rodzaj").equalTo("SAMOCHÓD OSOBOWY")
                        .and(col("data_wyrejestrowania").isNull()))
                .filter(col("rok_produkcji").isNotNull());

        Dataset<Row> pojazdy_leq2003 = samochody_zarej_rp
                .filter(col("rok_produkcji").leq(2003));

        Dataset<Row> pojazdy_gt2003leq2013 = samochody_zarej_rp
                .filter(col("rok_produkcji").gt(2003)
                        .and(col("rok_produkcji").leq(2013)));

        Dataset<Row> pojazdy_gt2013 = samochody_zarej_rp
                .filter(col("rok_produkcji").gt(2013));

        long liczba_leq2003 = pojazdy_leq2003.count();
        long liczba_gt2003leq2013 = pojazdy_gt2003leq2013.count();
        long liczba_gt2013 = pojazdy_gt2013.count();

        if(wszystkieDaneZWojewodztw_roczniki.containsKey("do 2003")) {
            long nowa_wartosc = liczba_leq2003 + wszystkieDaneZWojewodztw_roczniki.get("do 2003");
            wszystkieDaneZWojewodztw_roczniki.remove("do 2003");
            wszystkieDaneZWojewodztw_roczniki.put("do 2003", nowa_wartosc);
        }
        else {
            wszystkieDaneZWojewodztw_roczniki.put("do 2003", liczba_leq2003);
        }

        if(wszystkieDaneZWojewodztw_roczniki.containsKey("2004 - 2013")) {
            long nowa_wartosc = liczba_gt2003leq2013 + wszystkieDaneZWojewodztw_roczniki.get("2004 - 2013");
            wszystkieDaneZWojewodztw_roczniki.remove("2004 - 2013");
            wszystkieDaneZWojewodztw_roczniki.put("2004 - 2013", nowa_wartosc);
        }
        else {
            wszystkieDaneZWojewodztw_roczniki.put("2004 - 2013", liczba_gt2003leq2013);
        }

        if(wszystkieDaneZWojewodztw_roczniki.containsKey("od 2014")) {
            long nowa_wartosc = liczba_gt2013 + wszystkieDaneZWojewodztw_roczniki.get("od 2014");
            wszystkieDaneZWojewodztw_roczniki.remove("od 2014");
            wszystkieDaneZWojewodztw_roczniki.put("od 2014", nowa_wartosc);
        }
        else {
            wszystkieDaneZWojewodztw_roczniki.put("od 2014", liczba_gt2013);
        }

        // Tworzenie wykresu 1 - Porównanie popularności aut z poszczególnych roczników - województwo lubelskie
        DefaultPieDataset dpd = new DefaultPieDataset();
        dpd.setValue("do 2003", liczba_leq2003);
        dpd.setValue("2004 - 2013", liczba_gt2003leq2013);
        dpd.setValue("od 2014", liczba_gt2013);

        JFreeChart chart = ChartFactory.createPieChart(
                "Porównanie popularności aut z poszczególnych roczników" +
                        " - województwo małopolskie",
                dpd,
                true,
                true,
                false
        );

        PiePlot plot = (PiePlot) chart.getPlot();
        plot.setSectionPaint("do 2003", Color.decode("#0096ff"));
        plot.setSectionPaint("2004 - 2013", Color.BLACK);
        plot.setSectionPaint("od 2014", Color.YELLOW);
        plot.setForegroundAlpha(0.5f);
        plot.setExplodePercent("do 2003", 0.20);
        plot.setBackgroundPaint(Color.WHITE);
        plot.setSimpleLabels(true);

        PieSectionLabelGenerator gen = new StandardPieSectionLabelGenerator(
                "{0}: {1} ({2})", new DecimalFormat("0"), new DecimalFormat("0%"));
        plot.setLabelGenerator(gen);

        try {
            ChartUtilities.saveChartAsPNG(
                    new File("F:\\II stopień\\Semestr IV\\Praca magisterska\\Analiza danych\\vehicle_vintages_survey\\malopolskie_ilosc.png"),
                    chart,
                    500,
                    400);
        } catch (Exception e) {
            System.out.println("Wystapil problem podczas tworzenia wykresu");
        }

        // Obliczanie ilosci pojazdow na dany rodzaj paliwa w danym przedziale lat produkcji
        List<Row> paliwa_leq2003 = pojazdy_leq2003
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

        List<Row> paliwa_gt2003leq2013 = pojazdy_gt2003leq2013
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

        List<Row> paliwa_gt2013 = pojazdy_gt2013
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

        // Tworzenie wykresu 2 - Podział samochodów z poszczególnych roczników
        // względem rodzaju paliwa - województwo lubelskie
        DefaultCategoryDataset dcd = new DefaultCategoryDataset();

        for(Row row : paliwa_leq2003) {
            if(row.get(1) == null) {
                dcd.setValue((long)row.get(2), (String)row.get(0), "do 2003");
                if(row.get(0).equals("BENZYNA")) {
                    if(wszystkieDaneZWojewodztw_paliwa.containsKey("PB (do 2003)")) {
                        long nowa_wartosc = (long)row.get(2)
                                + wszystkieDaneZWojewodztw_paliwa.get("PB (do 2003)");
                        wszystkieDaneZWojewodztw_paliwa.remove("PB (do 2003)");
                        wszystkieDaneZWojewodztw_paliwa.put("PB (do 2003)", nowa_wartosc);
                    }
                    else {
                        wszystkieDaneZWojewodztw_paliwa.put("PB (do 2003)", (long)row.get(2));
                    }
                }
                else if(row.get(0).equals("OLEJ NAPĘDOWY")){
                    if(wszystkieDaneZWojewodztw_paliwa.containsKey("ON (do 2003)")) {
                        long nowa_wartosc = (long)row.get(2)
                                + wszystkieDaneZWojewodztw_paliwa.get("ON (do 2003)");
                        wszystkieDaneZWojewodztw_paliwa.remove("ON (do 2003)");
                        wszystkieDaneZWojewodztw_paliwa.put("ON (do 2003)", nowa_wartosc);
                    }
                    else {
                        wszystkieDaneZWojewodztw_paliwa.put("ON (do 2003)", (long)row.get(2));
                    }
                }
            }
            else {
                dcd.setValue((long)row.get(2), row.get(0) + " + LPG", "do 2003");
                if(wszystkieDaneZWojewodztw_paliwa.containsKey("PB+LPG (do 2003)")) {
                    long nowa_wartosc = (long)row.get(2)
                            + wszystkieDaneZWojewodztw_paliwa.get("PB+LPG (do 2003)");
                    wszystkieDaneZWojewodztw_paliwa.remove("PB+LPG (do 2003)");
                    wszystkieDaneZWojewodztw_paliwa.put("PB+LPG (do 2003)", nowa_wartosc);
                }
                else {
                    wszystkieDaneZWojewodztw_paliwa.put("PB+LPG (do 2003)", (long)row.get(2));
                }
            }
        }
        for(Row row : paliwa_gt2003leq2013) {
            if(row.get(1) == null) {
                dcd.setValue((long)row.get(2), (String)row.get(0), "2004 - 2013");
                if(row.get(0).equals("BENZYNA")) {
                    if(wszystkieDaneZWojewodztw_paliwa.containsKey("PB (2004 - 2013)")) {
                        long nowa_wartosc = (long)row.get(2)
                                + wszystkieDaneZWojewodztw_paliwa.get("PB (2004 - 2013)");
                        wszystkieDaneZWojewodztw_paliwa.remove("PB (2004 - 2013)");
                        wszystkieDaneZWojewodztw_paliwa.put("PB (2004 - 2013)", nowa_wartosc);
                    }
                    else {
                        wszystkieDaneZWojewodztw_paliwa.put("PB (2004 - 2013)", (long)row.get(2));
                    }
                }
                else if(row.get(0).equals("OLEJ NAPĘDOWY")){
                    if(wszystkieDaneZWojewodztw_paliwa.containsKey("ON (2004 - 2013)")) {
                        long nowa_wartosc = (long)row.get(2)
                                + wszystkieDaneZWojewodztw_paliwa.get("ON (2004 - 2013)");
                        wszystkieDaneZWojewodztw_paliwa.remove("ON (2004 - 2013)");
                        wszystkieDaneZWojewodztw_paliwa.put("ON (2004 - 2013)", nowa_wartosc);
                    }
                    else {
                        wszystkieDaneZWojewodztw_paliwa.put("ON (2004 - 2013)", (long)row.get(2));
                    }
                }
            }
            else {
                dcd.setValue((long)row.get(2), row.get(0) + " + LPG", "2004 - 2013");
                if(wszystkieDaneZWojewodztw_paliwa.containsKey("PB+LPG (2004 - 2013)")) {
                    long nowa_wartosc = (long)row.get(2)
                            + wszystkieDaneZWojewodztw_paliwa.get("PB+LPG (2004 - 2013)");
                    wszystkieDaneZWojewodztw_paliwa.remove("PB+LPG (2004 - 2013)");
                    wszystkieDaneZWojewodztw_paliwa.put("PB+LPG (2004 - 2013)", nowa_wartosc);
                }
                else {
                    wszystkieDaneZWojewodztw_paliwa.put("PB+LPG (2004 - 2013)", (long)row.get(2));
                }
            }
        }
        for(Row row : paliwa_gt2013) {
            if(row.get(1) == null) {
                dcd.setValue((long)row.get(2), (String)row.get(0), "od 2014");
                if(row.get(0).equals("BENZYNA")) {
                    if(wszystkieDaneZWojewodztw_paliwa.containsKey("PB (od 2014)")) {
                        long nowa_wartosc = (long)row.get(2)
                                + wszystkieDaneZWojewodztw_paliwa.get("PB (od 2014)");
                        wszystkieDaneZWojewodztw_paliwa.remove("PB (od 2014)");
                        wszystkieDaneZWojewodztw_paliwa.put("PB (od 2014)", nowa_wartosc);
                    }
                    else {
                        wszystkieDaneZWojewodztw_paliwa.put("PB (od 2014)", (long)row.get(2));
                    }
                }
                else if(row.get(0).equals("OLEJ NAPĘDOWY")){
                    if(wszystkieDaneZWojewodztw_paliwa.containsKey("ON (od 2014)")) {
                        long nowa_wartosc = (long)row.get(2)
                                + wszystkieDaneZWojewodztw_paliwa.get("ON (od 2014)");
                        wszystkieDaneZWojewodztw_paliwa.remove("ON (od 2014)");
                        wszystkieDaneZWojewodztw_paliwa.put("ON (od 2014)", nowa_wartosc);
                    }
                    else {
                        wszystkieDaneZWojewodztw_paliwa.put("ON (od 2014)", (long)row.get(2));
                    }
                }
            }
            else {
                dcd.setValue((long)row.get(2), row.get(0) + " + LPG", "od 2014");
                if(wszystkieDaneZWojewodztw_paliwa.containsKey("PB+LPG (od 2014)")) {
                    long nowa_wartosc = (long)row.get(2)
                            + wszystkieDaneZWojewodztw_paliwa.get("PB+LPG (od 2014)");
                    wszystkieDaneZWojewodztw_paliwa.remove("PB+LPG (od 2014)");
                    wszystkieDaneZWojewodztw_paliwa.put("PB+LPG (od 2014)", nowa_wartosc);
                }
                else {
                    wszystkieDaneZWojewodztw_paliwa.put("PB+LPG (od 2014)", (long)row.get(2));
                }
            }
        }

        JFreeChart chart2 = ChartFactory.createBarChart(
                "Podział samochodów z poszczególnych roczników względem rodzaju paliwa" +
                        " - województwo małopolskie",
                "Rok produkcji",
                "Ilość pojazdów",
                dcd,
                PlotOrientation.VERTICAL,
                true,
                true,
                false
        );

        CategoryPlot plot2 = (CategoryPlot) chart2.getPlot();
        plot2.setForegroundAlpha(0.7f);
        plot2.getRenderer().setSeriesPaint(0, Color.decode("#006ff9"));
        plot2.getRenderer().setSeriesPaint(1, Color.ORANGE);
        plot2.getRenderer().setSeriesPaint(2, Color.DARK_GRAY);
        plot2.setRangeGridlinePaint(Color.BLACK);
        plot2.setBackgroundPaint(Color.WHITE);
        BarRenderer renderer = (BarRenderer) plot2.getRenderer();
        renderer.setItemMargin(.1);

        try {
            ChartUtilities.saveChartAsPNG(
                    new File("F:\\II stopień\\Semestr IV\\Praca magisterska\\Analiza danych\\vehicle_vintages_survey\\malopolskie_paliwa.png"),
                    chart2,
                    500,
                    400
            );
        } catch (IOException e) {
            System.out.println("Wystapil problem podczas tworzenia wykresu");
        }

        JFrame frame = new JFrame("Popularność poszczególnych roczników pojazdów względem" +
                " rodzaju paliwa - województwo małopolskie");
        frame.setLayout(new FlowLayout());
        frame.getContentPane().add(new ChartPanel(chart));
        frame.getContentPane().add(new ChartPanel(chart2));
        frame.pack();
        frame.setVisible(true);
    }

    public void popularity_of_vintages_of_vehicles_and_fuels_woj_mazowieckie() {
        // Obliczanie ilosci samochodow osobowych w danych latach produkcji
        Dataset<Row> samochody_zarej_rp = cepikData.getWojewodztwo_mazowieckie()
                .filter(col("rodzaj").equalTo("SAMOCHÓD OSOBOWY")
                        .and(col("data_wyrejestrowania").isNull()))
                .filter(col("rok_produkcji").isNotNull());

        Dataset<Row> pojazdy_leq2003 = samochody_zarej_rp
                .filter(col("rok_produkcji").leq(2003));

        Dataset<Row> pojazdy_gt2003leq2013 = samochody_zarej_rp
                .filter(col("rok_produkcji").gt(2003)
                        .and(col("rok_produkcji").leq(2013)));

        Dataset<Row> pojazdy_gt2013 = samochody_zarej_rp
                .filter(col("rok_produkcji").gt(2013));

        long liczba_leq2003 = pojazdy_leq2003.count();
        long liczba_gt2003leq2013 = pojazdy_gt2003leq2013.count();
        long liczba_gt2013 = pojazdy_gt2013.count();

        if(wszystkieDaneZWojewodztw_roczniki.containsKey("do 2003")) {
            long nowa_wartosc = liczba_leq2003 + wszystkieDaneZWojewodztw_roczniki.get("do 2003");
            wszystkieDaneZWojewodztw_roczniki.remove("do 2003");
            wszystkieDaneZWojewodztw_roczniki.put("do 2003", nowa_wartosc);
        }
        else {
            wszystkieDaneZWojewodztw_roczniki.put("do 2003", liczba_leq2003);
        }

        if(wszystkieDaneZWojewodztw_roczniki.containsKey("2004 - 2013")) {
            long nowa_wartosc = liczba_gt2003leq2013 + wszystkieDaneZWojewodztw_roczniki.get("2004 - 2013");
            wszystkieDaneZWojewodztw_roczniki.remove("2004 - 2013");
            wszystkieDaneZWojewodztw_roczniki.put("2004 - 2013", nowa_wartosc);
        }
        else {
            wszystkieDaneZWojewodztw_roczniki.put("2004 - 2013", liczba_gt2003leq2013);
        }

        if(wszystkieDaneZWojewodztw_roczniki.containsKey("od 2014")) {
            long nowa_wartosc = liczba_gt2013 + wszystkieDaneZWojewodztw_roczniki.get("od 2014");
            wszystkieDaneZWojewodztw_roczniki.remove("od 2014");
            wszystkieDaneZWojewodztw_roczniki.put("od 2014", nowa_wartosc);
        }
        else {
            wszystkieDaneZWojewodztw_roczniki.put("od 2014", liczba_gt2013);
        }

        // Tworzenie wykresu 1 - Porównanie popularności aut z poszczególnych roczników - województwo lubelskie
        DefaultPieDataset dpd = new DefaultPieDataset();
        dpd.setValue("do 2003", liczba_leq2003);
        dpd.setValue("2004 - 2013", liczba_gt2003leq2013);
        dpd.setValue("od 2014", liczba_gt2013);

        JFreeChart chart = ChartFactory.createPieChart(
                "Porównanie popularności aut z poszczególnych roczników" +
                        " - województwo mazowieckie",
                dpd,
                true,
                true,
                false
        );

        PiePlot plot = (PiePlot) chart.getPlot();
        plot.setSectionPaint("do 2003", Color.decode("#0096ff"));
        plot.setSectionPaint("2004 - 2013", Color.BLACK);
        plot.setSectionPaint("od 2014", Color.YELLOW);
        plot.setForegroundAlpha(0.5f);
        plot.setExplodePercent("do 2003", 0.20);
        plot.setBackgroundPaint(Color.WHITE);
        plot.setSimpleLabels(true);

        PieSectionLabelGenerator gen = new StandardPieSectionLabelGenerator(
                "{0}: {1} ({2})", new DecimalFormat("0"), new DecimalFormat("0%"));
        plot.setLabelGenerator(gen);

        try {
            ChartUtilities.saveChartAsPNG(
                    new File("F:\\II stopień\\Semestr IV\\Praca magisterska\\Analiza danych\\vehicle_vintages_survey\\mazowieckie_ilosc.png"),
                    chart,
                    500,
                    400);
        } catch (Exception e) {
            System.out.println("Wystapil problem podczas tworzenia wykresu");
        }

        // Obliczanie ilosci pojazdow na dany rodzaj paliwa w danym przedziale lat produkcji
        List<Row> paliwa_leq2003 = pojazdy_leq2003
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

        List<Row> paliwa_gt2003leq2013 = pojazdy_gt2003leq2013
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

        List<Row> paliwa_gt2013 = pojazdy_gt2013
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

        // Tworzenie wykresu 2 - Podział samochodów z poszczególnych roczników
        // względem rodzaju paliwa - województwo lubelskie
        DefaultCategoryDataset dcd = new DefaultCategoryDataset();

        for(Row row : paliwa_leq2003) {
            if(row.get(1) == null) {
                dcd.setValue((long)row.get(2), (String)row.get(0), "do 2003");
                if(row.get(0).equals("BENZYNA")) {
                    if(wszystkieDaneZWojewodztw_paliwa.containsKey("PB (do 2003)")) {
                        long nowa_wartosc = (long)row.get(2)
                                + wszystkieDaneZWojewodztw_paliwa.get("PB (do 2003)");
                        wszystkieDaneZWojewodztw_paliwa.remove("PB (do 2003)");
                        wszystkieDaneZWojewodztw_paliwa.put("PB (do 2003)", nowa_wartosc);
                    }
                    else {
                        wszystkieDaneZWojewodztw_paliwa.put("PB (do 2003)", (long)row.get(2));
                    }
                }
                else if(row.get(0).equals("OLEJ NAPĘDOWY")){
                    if(wszystkieDaneZWojewodztw_paliwa.containsKey("ON (do 2003)")) {
                        long nowa_wartosc = (long)row.get(2)
                                + wszystkieDaneZWojewodztw_paliwa.get("ON (do 2003)");
                        wszystkieDaneZWojewodztw_paliwa.remove("ON (do 2003)");
                        wszystkieDaneZWojewodztw_paliwa.put("ON (do 2003)", nowa_wartosc);
                    }
                    else {
                        wszystkieDaneZWojewodztw_paliwa.put("ON (do 2003)", (long)row.get(2));
                    }
                }
            }
            else {
                dcd.setValue((long)row.get(2), row.get(0) + " + LPG", "do 2003");
                if(wszystkieDaneZWojewodztw_paliwa.containsKey("PB+LPG (do 2003)")) {
                    long nowa_wartosc = (long)row.get(2)
                            + wszystkieDaneZWojewodztw_paliwa.get("PB+LPG (do 2003)");
                    wszystkieDaneZWojewodztw_paliwa.remove("PB+LPG (do 2003)");
                    wszystkieDaneZWojewodztw_paliwa.put("PB+LPG (do 2003)", nowa_wartosc);
                }
                else {
                    wszystkieDaneZWojewodztw_paliwa.put("PB+LPG (do 2003)", (long)row.get(2));
                }
            }
        }
        for(Row row : paliwa_gt2003leq2013) {
            if(row.get(1) == null) {
                dcd.setValue((long)row.get(2), (String)row.get(0), "2004 - 2013");
                if(row.get(0).equals("BENZYNA")) {
                    if(wszystkieDaneZWojewodztw_paliwa.containsKey("PB (2004 - 2013)")) {
                        long nowa_wartosc = (long)row.get(2)
                                + wszystkieDaneZWojewodztw_paliwa.get("PB (2004 - 2013)");
                        wszystkieDaneZWojewodztw_paliwa.remove("PB (2004 - 2013)");
                        wszystkieDaneZWojewodztw_paliwa.put("PB (2004 - 2013)", nowa_wartosc);
                    }
                    else {
                        wszystkieDaneZWojewodztw_paliwa.put("PB (2004 - 2013)", (long)row.get(2));
                    }
                }
                else if(row.get(0).equals("OLEJ NAPĘDOWY")){
                    if(wszystkieDaneZWojewodztw_paliwa.containsKey("ON (2004 - 2013)")) {
                        long nowa_wartosc = (long)row.get(2)
                                + wszystkieDaneZWojewodztw_paliwa.get("ON (2004 - 2013)");
                        wszystkieDaneZWojewodztw_paliwa.remove("ON (2004 - 2013)");
                        wszystkieDaneZWojewodztw_paliwa.put("ON (2004 - 2013)", nowa_wartosc);
                    }
                    else {
                        wszystkieDaneZWojewodztw_paliwa.put("ON (2004 - 2013)", (long)row.get(2));
                    }
                }
            }
            else {
                dcd.setValue((long)row.get(2), row.get(0) + " + LPG", "2004 - 2013");
                if(wszystkieDaneZWojewodztw_paliwa.containsKey("PB+LPG (2004 - 2013)")) {
                    long nowa_wartosc = (long)row.get(2)
                            + wszystkieDaneZWojewodztw_paliwa.get("PB+LPG (2004 - 2013)");
                    wszystkieDaneZWojewodztw_paliwa.remove("PB+LPG (2004 - 2013)");
                    wszystkieDaneZWojewodztw_paliwa.put("PB+LPG (2004 - 2013)", nowa_wartosc);
                }
                else {
                    wszystkieDaneZWojewodztw_paliwa.put("PB+LPG (2004 - 2013)", (long)row.get(2));
                }
            }
        }
        for(Row row : paliwa_gt2013) {
            if(row.get(1) == null) {
                dcd.setValue((long)row.get(2), (String)row.get(0), "od 2014");
                if(row.get(0).equals("BENZYNA")) {
                    if(wszystkieDaneZWojewodztw_paliwa.containsKey("PB (od 2014)")) {
                        long nowa_wartosc = (long)row.get(2)
                                + wszystkieDaneZWojewodztw_paliwa.get("PB (od 2014)");
                        wszystkieDaneZWojewodztw_paliwa.remove("PB (od 2014)");
                        wszystkieDaneZWojewodztw_paliwa.put("PB (od 2014)", nowa_wartosc);
                    }
                    else {
                        wszystkieDaneZWojewodztw_paliwa.put("PB (od 2014)", (long)row.get(2));
                    }
                }
                else if(row.get(0).equals("OLEJ NAPĘDOWY")){
                    if(wszystkieDaneZWojewodztw_paliwa.containsKey("ON (od 2014)")) {
                        long nowa_wartosc = (long)row.get(2)
                                + wszystkieDaneZWojewodztw_paliwa.get("ON (od 2014)");
                        wszystkieDaneZWojewodztw_paliwa.remove("ON (od 2014)");
                        wszystkieDaneZWojewodztw_paliwa.put("ON (od 2014)", nowa_wartosc);
                    }
                    else {
                        wszystkieDaneZWojewodztw_paliwa.put("ON (od 2014)", (long)row.get(2));
                    }
                }
            }
            else {
                dcd.setValue((long)row.get(2), row.get(0) + " + LPG", "od 2014");
                if(wszystkieDaneZWojewodztw_paliwa.containsKey("PB+LPG (od 2014)")) {
                    long nowa_wartosc = (long)row.get(2)
                            + wszystkieDaneZWojewodztw_paliwa.get("PB+LPG (od 2014)");
                    wszystkieDaneZWojewodztw_paliwa.remove("PB+LPG (od 2014)");
                    wszystkieDaneZWojewodztw_paliwa.put("PB+LPG (od 2014)", nowa_wartosc);
                }
                else {
                    wszystkieDaneZWojewodztw_paliwa.put("PB+LPG (od 2014)", (long)row.get(2));
                }
            }
        }

        JFreeChart chart2 = ChartFactory.createBarChart(
                "Podział samochodów z poszczególnych roczników względem rodzaju paliwa" +
                        " - województwo mazowieckie",
                "Rok produkcji",
                "Ilość pojazdów",
                dcd,
                PlotOrientation.VERTICAL,
                true,
                true,
                false
        );

        CategoryPlot plot2 = (CategoryPlot) chart2.getPlot();
        plot2.setForegroundAlpha(0.7f);
        plot2.getRenderer().setSeriesPaint(0, Color.decode("#006ff9"));
        plot2.getRenderer().setSeriesPaint(1, Color.DARK_GRAY);
        plot2.getRenderer().setSeriesPaint(2, Color.ORANGE);
        plot2.setRangeGridlinePaint(Color.BLACK);
        plot2.setBackgroundPaint(Color.WHITE);
        BarRenderer renderer = (BarRenderer) plot2.getRenderer();
        renderer.setItemMargin(.1);

        try {
            ChartUtilities.saveChartAsPNG(
                    new File("F:\\II stopień\\Semestr IV\\Praca magisterska\\Analiza danych\\vehicle_vintages_survey\\mazowieckie_paliwa.png"),
                    chart2,
                    500,
                    400
            );
        } catch (IOException e) {
            System.out.println("Wystapil problem podczas tworzenia wykresu");
        }

        JFrame frame = new JFrame("Popularność poszczególnych roczników pojazdów względem" +
                " rodzaju paliwa - województwo mazowieckie");
        frame.setLayout(new FlowLayout());
        frame.getContentPane().add(new ChartPanel(chart));
        frame.getContentPane().add(new ChartPanel(chart2));
        frame.pack();
        frame.setVisible(true);
    }

    public void popularity_of_vintages_of_vehicles_and_fuels_woj_opolskie() {
        // Obliczanie ilosci samochodow osobowych w danych latach produkcji
        Dataset<Row> samochody_zarej_rp = cepikData.getWojewodztwo_opolskie()
                .filter(col("rodzaj").equalTo("SAMOCHÓD OSOBOWY")
                        .and(col("data_wyrejestrowania").isNull()))
                .filter(col("rok_produkcji").isNotNull());

        Dataset<Row> pojazdy_leq2003 = samochody_zarej_rp
                .filter(col("rok_produkcji").leq(2003));

        Dataset<Row> pojazdy_gt2003leq2013 = samochody_zarej_rp
                .filter(col("rok_produkcji").gt(2003)
                        .and(col("rok_produkcji").leq(2013)));

        Dataset<Row> pojazdy_gt2013 = samochody_zarej_rp
                .filter(col("rok_produkcji").gt(2013));

        long liczba_leq2003 = pojazdy_leq2003.count();
        long liczba_gt2003leq2013 = pojazdy_gt2003leq2013.count();
        long liczba_gt2013 = pojazdy_gt2013.count();

        if(wszystkieDaneZWojewodztw_roczniki.containsKey("do 2003")) {
            long nowa_wartosc = liczba_leq2003 + wszystkieDaneZWojewodztw_roczniki.get("do 2003");
            wszystkieDaneZWojewodztw_roczniki.remove("do 2003");
            wszystkieDaneZWojewodztw_roczniki.put("do 2003", nowa_wartosc);
        }
        else {
            wszystkieDaneZWojewodztw_roczniki.put("do 2003", liczba_leq2003);
        }

        if(wszystkieDaneZWojewodztw_roczniki.containsKey("2004 - 2013")) {
            long nowa_wartosc = liczba_gt2003leq2013 + wszystkieDaneZWojewodztw_roczniki.get("2004 - 2013");
            wszystkieDaneZWojewodztw_roczniki.remove("2004 - 2013");
            wszystkieDaneZWojewodztw_roczniki.put("2004 - 2013", nowa_wartosc);
        }
        else {
            wszystkieDaneZWojewodztw_roczniki.put("2004 - 2013", liczba_gt2003leq2013);
        }

        if(wszystkieDaneZWojewodztw_roczniki.containsKey("od 2014")) {
            long nowa_wartosc = liczba_gt2013 + wszystkieDaneZWojewodztw_roczniki.get("od 2014");
            wszystkieDaneZWojewodztw_roczniki.remove("od 2014");
            wszystkieDaneZWojewodztw_roczniki.put("od 2014", nowa_wartosc);
        }
        else {
            wszystkieDaneZWojewodztw_roczniki.put("od 2014", liczba_gt2013);
        }

        // Tworzenie wykresu 1 - Porównanie popularności aut z poszczególnych roczników - województwo lubelskie
        DefaultPieDataset dpd = new DefaultPieDataset();
        dpd.setValue("do 2003", liczba_leq2003);
        dpd.setValue("2004 - 2013", liczba_gt2003leq2013);
        dpd.setValue("od 2014", liczba_gt2013);

        JFreeChart chart = ChartFactory.createPieChart(
                "Porównanie popularności aut z poszczególnych roczników" +
                        " - województwo opolskie",
                dpd,
                true,
                true,
                false
        );

        PiePlot plot = (PiePlot) chart.getPlot();
        plot.setSectionPaint("do 2003", Color.decode("#0096ff"));
        plot.setSectionPaint("2004 - 2013", Color.BLACK);
        plot.setSectionPaint("od 2014", Color.YELLOW);
        plot.setForegroundAlpha(0.5f);
        plot.setExplodePercent("do 2003", 0.20);
        plot.setBackgroundPaint(Color.WHITE);
        plot.setSimpleLabels(true);

        PieSectionLabelGenerator gen = new StandardPieSectionLabelGenerator(
                "{0}: {1} ({2})", new DecimalFormat("0"), new DecimalFormat("0%"));
        plot.setLabelGenerator(gen);

        try {
            ChartUtilities.saveChartAsPNG(
                    new File("F:\\II stopień\\Semestr IV\\Praca magisterska\\Analiza danych\\vehicle_vintages_survey\\opolskie_ilosc.png"),
                    chart,
                    500,
                    400);
        } catch (Exception e) {
            System.out.println("Wystapil problem podczas tworzenia wykresu");
        }

        // Obliczanie ilosci pojazdow na dany rodzaj paliwa w danym przedziale lat produkcji
        List<Row> paliwa_leq2003 = pojazdy_leq2003
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

        List<Row> paliwa_gt2003leq2013 = pojazdy_gt2003leq2013
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

        List<Row> paliwa_gt2013 = pojazdy_gt2013
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

        // Tworzenie wykresu 2 - Podział samochodów z poszczególnych roczników
        // względem rodzaju paliwa - województwo lubelskie
        DefaultCategoryDataset dcd = new DefaultCategoryDataset();

        for(Row row : paliwa_leq2003) {
            if(row.get(1) == null) {
                dcd.setValue((long)row.get(2), (String)row.get(0), "do 2003");
                if(row.get(0).equals("BENZYNA")) {
                    if(wszystkieDaneZWojewodztw_paliwa.containsKey("PB (do 2003)")) {
                        long nowa_wartosc = (long)row.get(2)
                                + wszystkieDaneZWojewodztw_paliwa.get("PB (do 2003)");
                        wszystkieDaneZWojewodztw_paliwa.remove("PB (do 2003)");
                        wszystkieDaneZWojewodztw_paliwa.put("PB (do 2003)", nowa_wartosc);
                    }
                    else {
                        wszystkieDaneZWojewodztw_paliwa.put("PB (do 2003)", (long)row.get(2));
                    }
                }
                else if(row.get(0).equals("OLEJ NAPĘDOWY")){
                    if(wszystkieDaneZWojewodztw_paliwa.containsKey("ON (do 2003)")) {
                        long nowa_wartosc = (long)row.get(2)
                                + wszystkieDaneZWojewodztw_paliwa.get("ON (do 2003)");
                        wszystkieDaneZWojewodztw_paliwa.remove("ON (do 2003)");
                        wszystkieDaneZWojewodztw_paliwa.put("ON (do 2003)", nowa_wartosc);
                    }
                    else {
                        wszystkieDaneZWojewodztw_paliwa.put("ON (do 2003)", (long)row.get(2));
                    }
                }
            }
            else {
                dcd.setValue((long)row.get(2), row.get(0) + " + LPG", "do 2003");
                if(wszystkieDaneZWojewodztw_paliwa.containsKey("PB+LPG (do 2003)")) {
                    long nowa_wartosc = (long)row.get(2)
                            + wszystkieDaneZWojewodztw_paliwa.get("PB+LPG (do 2003)");
                    wszystkieDaneZWojewodztw_paliwa.remove("PB+LPG (do 2003)");
                    wszystkieDaneZWojewodztw_paliwa.put("PB+LPG (do 2003)", nowa_wartosc);
                }
                else {
                    wszystkieDaneZWojewodztw_paliwa.put("PB+LPG (do 2003)", (long)row.get(2));
                }
            }
        }
        for(Row row : paliwa_gt2003leq2013) {
            if(row.get(1) == null) {
                dcd.setValue((long)row.get(2), (String)row.get(0), "2004 - 2013");
                if(row.get(0).equals("BENZYNA")) {
                    if(wszystkieDaneZWojewodztw_paliwa.containsKey("PB (2004 - 2013)")) {
                        long nowa_wartosc = (long)row.get(2)
                                + wszystkieDaneZWojewodztw_paliwa.get("PB (2004 - 2013)");
                        wszystkieDaneZWojewodztw_paliwa.remove("PB (2004 - 2013)");
                        wszystkieDaneZWojewodztw_paliwa.put("PB (2004 - 2013)", nowa_wartosc);
                    }
                    else {
                        wszystkieDaneZWojewodztw_paliwa.put("PB (2004 - 2013)", (long)row.get(2));
                    }
                }
                else if(row.get(0).equals("OLEJ NAPĘDOWY")){
                    if(wszystkieDaneZWojewodztw_paliwa.containsKey("ON (2004 - 2013)")) {
                        long nowa_wartosc = (long)row.get(2)
                                + wszystkieDaneZWojewodztw_paliwa.get("ON (2004 - 2013)");
                        wszystkieDaneZWojewodztw_paliwa.remove("ON (2004 - 2013)");
                        wszystkieDaneZWojewodztw_paliwa.put("ON (2004 - 2013)", nowa_wartosc);
                    }
                    else {
                        wszystkieDaneZWojewodztw_paliwa.put("ON (2004 - 2013)", (long)row.get(2));
                    }
                }
            }
            else {
                dcd.setValue((long)row.get(2), row.get(0) + " + LPG", "2004 - 2013");
                if(wszystkieDaneZWojewodztw_paliwa.containsKey("PB+LPG (2004 - 2013)")) {
                    long nowa_wartosc = (long)row.get(2)
                            + wszystkieDaneZWojewodztw_paliwa.get("PB+LPG (2004 - 2013)");
                    wszystkieDaneZWojewodztw_paliwa.remove("PB+LPG (2004 - 2013)");
                    wszystkieDaneZWojewodztw_paliwa.put("PB+LPG (2004 - 2013)", nowa_wartosc);
                }
                else {
                    wszystkieDaneZWojewodztw_paliwa.put("PB+LPG (2004 - 2013)", (long)row.get(2));
                }
            }
        }
        for(Row row : paliwa_gt2013) {
            if(row.get(1) == null) {
                dcd.setValue((long)row.get(2), (String)row.get(0), "od 2014");
                if(row.get(0).equals("BENZYNA")) {
                    if(wszystkieDaneZWojewodztw_paliwa.containsKey("PB (od 2014)")) {
                        long nowa_wartosc = (long)row.get(2)
                                + wszystkieDaneZWojewodztw_paliwa.get("PB (od 2014)");
                        wszystkieDaneZWojewodztw_paliwa.remove("PB (od 2014)");
                        wszystkieDaneZWojewodztw_paliwa.put("PB (od 2014)", nowa_wartosc);
                    }
                    else {
                        wszystkieDaneZWojewodztw_paliwa.put("PB (od 2014)", (long)row.get(2));
                    }
                }
                else if(row.get(0).equals("OLEJ NAPĘDOWY")){
                    if(wszystkieDaneZWojewodztw_paliwa.containsKey("ON (od 2014)")) {
                        long nowa_wartosc = (long)row.get(2)
                                + wszystkieDaneZWojewodztw_paliwa.get("ON (od 2014)");
                        wszystkieDaneZWojewodztw_paliwa.remove("ON (od 2014)");
                        wszystkieDaneZWojewodztw_paliwa.put("ON (od 2014)", nowa_wartosc);
                    }
                    else {
                        wszystkieDaneZWojewodztw_paliwa.put("ON (od 2014)", (long)row.get(2));
                    }
                }
            }
            else {
                dcd.setValue((long)row.get(2), row.get(0) + " + LPG", "od 2014");
                if(wszystkieDaneZWojewodztw_paliwa.containsKey("PB+LPG (od 2014)")) {
                    long nowa_wartosc = (long)row.get(2)
                            + wszystkieDaneZWojewodztw_paliwa.get("PB+LPG (od 2014)");
                    wszystkieDaneZWojewodztw_paliwa.remove("PB+LPG (od 2014)");
                    wszystkieDaneZWojewodztw_paliwa.put("PB+LPG (od 2014)", nowa_wartosc);
                }
                else {
                    wszystkieDaneZWojewodztw_paliwa.put("PB+LPG (od 2014)", (long)row.get(2));
                }
            }
        }

        JFreeChart chart2 = ChartFactory.createBarChart(
                "Podział samochodów z poszczególnych roczników względem rodzaju paliwa" +
                        " - województwo opolskie",
                "Rok produkcji",
                "Ilość pojazdów",
                dcd,
                PlotOrientation.VERTICAL,
                true,
                true,
                false
        );

        CategoryPlot plot2 = (CategoryPlot) chart2.getPlot();
        plot2.setForegroundAlpha(0.7f);
        plot2.getRenderer().setSeriesPaint(0, Color.decode("#006ff9"));
        plot2.getRenderer().setSeriesPaint(1, Color.ORANGE);
        plot2.getRenderer().setSeriesPaint(2, Color.DARK_GRAY);
        plot2.setRangeGridlinePaint(Color.BLACK);
        plot2.setBackgroundPaint(Color.WHITE);
        BarRenderer renderer = (BarRenderer) plot2.getRenderer();
        renderer.setItemMargin(.1);

        try {
            ChartUtilities.saveChartAsPNG(
                    new File("F:\\II stopień\\Semestr IV\\Praca magisterska\\Analiza danych\\vehicle_vintages_survey\\opolskie_paliwa.png"),
                    chart2,
                    500,
                    400
            );
        } catch (IOException e) {
            System.out.println("Wystapil problem podczas tworzenia wykresu");
        }

        JFrame frame = new JFrame("Popularność poszczególnych roczników pojazdów względem" +
                " rodzaju paliwa - województwo opolskie");
        frame.setLayout(new FlowLayout());
        frame.getContentPane().add(new ChartPanel(chart));
        frame.getContentPane().add(new ChartPanel(chart2));
        frame.pack();
        frame.setVisible(true);
    }

    public void popularity_of_vintages_of_vehicles_and_fuels_woj_podkarpackie() {
        // Obliczanie ilosci samochodow osobowych w danych latach produkcji
        Dataset<Row> samochody_zarej_rp = cepikData.getWojewodztwo_podkarpackie()
                .filter(col("rodzaj").equalTo("SAMOCHÓD OSOBOWY")
                        .and(col("data_wyrejestrowania").isNull()))
                .filter(col("rok_produkcji").isNotNull());

        Dataset<Row> pojazdy_leq2003 = samochody_zarej_rp
                .filter(col("rok_produkcji").leq(2003));

        Dataset<Row> pojazdy_gt2003leq2013 = samochody_zarej_rp
                .filter(col("rok_produkcji").gt(2003)
                        .and(col("rok_produkcji").leq(2013)));

        Dataset<Row> pojazdy_gt2013 = samochody_zarej_rp
                .filter(col("rok_produkcji").gt(2013));

        long liczba_leq2003 = pojazdy_leq2003.count();
        long liczba_gt2003leq2013 = pojazdy_gt2003leq2013.count();
        long liczba_gt2013 = pojazdy_gt2013.count();

        if(wszystkieDaneZWojewodztw_roczniki.containsKey("do 2003")) {
            long nowa_wartosc = liczba_leq2003 + wszystkieDaneZWojewodztw_roczniki.get("do 2003");
            wszystkieDaneZWojewodztw_roczniki.remove("do 2003");
            wszystkieDaneZWojewodztw_roczniki.put("do 2003", nowa_wartosc);
        }
        else {
            wszystkieDaneZWojewodztw_roczniki.put("do 2003", liczba_leq2003);
        }

        if(wszystkieDaneZWojewodztw_roczniki.containsKey("2004 - 2013")) {
            long nowa_wartosc = liczba_gt2003leq2013 + wszystkieDaneZWojewodztw_roczniki.get("2004 - 2013");
            wszystkieDaneZWojewodztw_roczniki.remove("2004 - 2013");
            wszystkieDaneZWojewodztw_roczniki.put("2004 - 2013", nowa_wartosc);
        }
        else {
            wszystkieDaneZWojewodztw_roczniki.put("2004 - 2013", liczba_gt2003leq2013);
        }

        if(wszystkieDaneZWojewodztw_roczniki.containsKey("od 2014")) {
            long nowa_wartosc = liczba_gt2013 + wszystkieDaneZWojewodztw_roczniki.get("od 2014");
            wszystkieDaneZWojewodztw_roczniki.remove("od 2014");
            wszystkieDaneZWojewodztw_roczniki.put("od 2014", nowa_wartosc);
        }
        else {
            wszystkieDaneZWojewodztw_roczniki.put("od 2014", liczba_gt2013);
        }

        // Tworzenie wykresu 1 - Porównanie popularności aut z poszczególnych roczników - województwo lubelskie
        DefaultPieDataset dpd = new DefaultPieDataset();
        dpd.setValue("do 2003", liczba_leq2003);
        dpd.setValue("2004 - 2013", liczba_gt2003leq2013);
        dpd.setValue("od 2014", liczba_gt2013);

        JFreeChart chart = ChartFactory.createPieChart(
                "Porównanie popularności aut z poszczególnych roczników" +
                        " - województwo podkarpackie",
                dpd,
                true,
                true,
                false
        );

        PiePlot plot = (PiePlot) chart.getPlot();
        plot.setSectionPaint("do 2003", Color.decode("#0096ff"));
        plot.setSectionPaint("2004 - 2013", Color.BLACK);
        plot.setSectionPaint("od 2014", Color.YELLOW);
        plot.setForegroundAlpha(0.5f);
        plot.setExplodePercent("do 2003", 0.20);
        plot.setBackgroundPaint(Color.WHITE);
        plot.setSimpleLabels(true);

        PieSectionLabelGenerator gen = new StandardPieSectionLabelGenerator(
                "{0}: {1} ({2})", new DecimalFormat("0"), new DecimalFormat("0%"));
        plot.setLabelGenerator(gen);

        try {
            ChartUtilities.saveChartAsPNG(
                    new File("F:\\II stopień\\Semestr IV\\Praca magisterska\\Analiza danych\\vehicle_vintages_survey\\podkarpackie_ilosc.png"),
                    chart,
                    500,
                    400);
        } catch (Exception e) {
            System.out.println("Wystapil problem podczas tworzenia wykresu");
        }

        // Obliczanie ilosci pojazdow na dany rodzaj paliwa w danym przedziale lat produkcji
        List<Row> paliwa_leq2003 = pojazdy_leq2003
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

        List<Row> paliwa_gt2003leq2013 = pojazdy_gt2003leq2013
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

        List<Row> paliwa_gt2013 = pojazdy_gt2013
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

        // Tworzenie wykresu 2 - Podział samochodów z poszczególnych roczników
        // względem rodzaju paliwa - województwo lubelskie
        DefaultCategoryDataset dcd = new DefaultCategoryDataset();

        for(Row row : paliwa_leq2003) {
            if(row.get(1) == null) {
                dcd.setValue((long)row.get(2), (String)row.get(0), "do 2003");
                if(row.get(0).equals("BENZYNA")) {
                    if(wszystkieDaneZWojewodztw_paliwa.containsKey("PB (do 2003)")) {
                        long nowa_wartosc = (long)row.get(2)
                                + wszystkieDaneZWojewodztw_paliwa.get("PB (do 2003)");
                        wszystkieDaneZWojewodztw_paliwa.remove("PB (do 2003)");
                        wszystkieDaneZWojewodztw_paliwa.put("PB (do 2003)", nowa_wartosc);
                    }
                    else {
                        wszystkieDaneZWojewodztw_paliwa.put("PB (do 2003)", (long)row.get(2));
                    }
                }
                else if(row.get(0).equals("OLEJ NAPĘDOWY")){
                    if(wszystkieDaneZWojewodztw_paliwa.containsKey("ON (do 2003)")) {
                        long nowa_wartosc = (long)row.get(2)
                                + wszystkieDaneZWojewodztw_paliwa.get("ON (do 2003)");
                        wszystkieDaneZWojewodztw_paliwa.remove("ON (do 2003)");
                        wszystkieDaneZWojewodztw_paliwa.put("ON (do 2003)", nowa_wartosc);
                    }
                    else {
                        wszystkieDaneZWojewodztw_paliwa.put("ON (do 2003)", (long)row.get(2));
                    }
                }
            }
            else {
                dcd.setValue((long)row.get(2), row.get(0) + " + LPG", "do 2003");
                if(wszystkieDaneZWojewodztw_paliwa.containsKey("PB+LPG (do 2003)")) {
                    long nowa_wartosc = (long)row.get(2)
                            + wszystkieDaneZWojewodztw_paliwa.get("PB+LPG (do 2003)");
                    wszystkieDaneZWojewodztw_paliwa.remove("PB+LPG (do 2003)");
                    wszystkieDaneZWojewodztw_paliwa.put("PB+LPG (do 2003)", nowa_wartosc);
                }
                else {
                    wszystkieDaneZWojewodztw_paliwa.put("PB+LPG (do 2003)", (long)row.get(2));
                }
            }
        }
        for(Row row : paliwa_gt2003leq2013) {
            if(row.get(1) == null) {
                dcd.setValue((long)row.get(2), (String)row.get(0), "2004 - 2013");
                if(row.get(0).equals("BENZYNA")) {
                    if(wszystkieDaneZWojewodztw_paliwa.containsKey("PB (2004 - 2013)")) {
                        long nowa_wartosc = (long)row.get(2)
                                + wszystkieDaneZWojewodztw_paliwa.get("PB (2004 - 2013)");
                        wszystkieDaneZWojewodztw_paliwa.remove("PB (2004 - 2013)");
                        wszystkieDaneZWojewodztw_paliwa.put("PB (2004 - 2013)", nowa_wartosc);
                    }
                    else {
                        wszystkieDaneZWojewodztw_paliwa.put("PB (2004 - 2013)", (long)row.get(2));
                    }
                }
                else if(row.get(0).equals("OLEJ NAPĘDOWY")){
                    if(wszystkieDaneZWojewodztw_paliwa.containsKey("ON (2004 - 2013)")) {
                        long nowa_wartosc = (long)row.get(2)
                                + wszystkieDaneZWojewodztw_paliwa.get("ON (2004 - 2013)");
                        wszystkieDaneZWojewodztw_paliwa.remove("ON (2004 - 2013)");
                        wszystkieDaneZWojewodztw_paliwa.put("ON (2004 - 2013)", nowa_wartosc);
                    }
                    else {
                        wszystkieDaneZWojewodztw_paliwa.put("ON (2004 - 2013)", (long)row.get(2));
                    }
                }
            }
            else {
                dcd.setValue((long)row.get(2), row.get(0) + " + LPG", "2004 - 2013");
                if(wszystkieDaneZWojewodztw_paliwa.containsKey("PB+LPG (2004 - 2013)")) {
                    long nowa_wartosc = (long)row.get(2)
                            + wszystkieDaneZWojewodztw_paliwa.get("PB+LPG (2004 - 2013)");
                    wszystkieDaneZWojewodztw_paliwa.remove("PB+LPG (2004 - 2013)");
                    wszystkieDaneZWojewodztw_paliwa.put("PB+LPG (2004 - 2013)", nowa_wartosc);
                }
                else {
                    wszystkieDaneZWojewodztw_paliwa.put("PB+LPG (2004 - 2013)", (long)row.get(2));
                }
            }
        }
        for(Row row : paliwa_gt2013) {
            if(row.get(1) == null) {
                dcd.setValue((long)row.get(2), (String)row.get(0), "od 2014");
                if(row.get(0).equals("BENZYNA")) {
                    if(wszystkieDaneZWojewodztw_paliwa.containsKey("PB (od 2014)")) {
                        long nowa_wartosc = (long)row.get(2)
                                + wszystkieDaneZWojewodztw_paliwa.get("PB (od 2014)");
                        wszystkieDaneZWojewodztw_paliwa.remove("PB (od 2014)");
                        wszystkieDaneZWojewodztw_paliwa.put("PB (od 2014)", nowa_wartosc);
                    }
                    else {
                        wszystkieDaneZWojewodztw_paliwa.put("PB (od 2014)", (long)row.get(2));
                    }
                }
                else if(row.get(0).equals("OLEJ NAPĘDOWY")){
                    if(wszystkieDaneZWojewodztw_paliwa.containsKey("ON (od 2014)")) {
                        long nowa_wartosc = (long)row.get(2)
                                + wszystkieDaneZWojewodztw_paliwa.get("ON (od 2014)");
                        wszystkieDaneZWojewodztw_paliwa.remove("ON (od 2014)");
                        wszystkieDaneZWojewodztw_paliwa.put("ON (od 2014)", nowa_wartosc);
                    }
                    else {
                        wszystkieDaneZWojewodztw_paliwa.put("ON (od 2014)", (long)row.get(2));
                    }
                }
            }
            else {
                dcd.setValue((long)row.get(2), row.get(0) + " + LPG", "od 2014");
                if(wszystkieDaneZWojewodztw_paliwa.containsKey("PB+LPG (od 2014)")) {
                    long nowa_wartosc = (long)row.get(2)
                            + wszystkieDaneZWojewodztw_paliwa.get("PB+LPG (od 2014)");
                    wszystkieDaneZWojewodztw_paliwa.remove("PB+LPG (od 2014)");
                    wszystkieDaneZWojewodztw_paliwa.put("PB+LPG (od 2014)", nowa_wartosc);
                }
                else {
                    wszystkieDaneZWojewodztw_paliwa.put("PB+LPG (od 2014)", (long)row.get(2));
                }
            }
        }

        JFreeChart chart2 = ChartFactory.createBarChart(
                "Podział samochodów z poszczególnych roczników względem rodzaju paliwa" +
                        " - województwo podkarpackie",
                "Rok produkcji",
                "Ilość pojazdów",
                dcd,
                PlotOrientation.VERTICAL,
                true,
                true,
                false
        );

        CategoryPlot plot2 = (CategoryPlot) chart2.getPlot();
        plot2.setForegroundAlpha(0.7f);
        plot2.getRenderer().setSeriesPaint(0, Color.decode("#006ff9"));
        plot2.getRenderer().setSeriesPaint(1, Color.ORANGE);
        plot2.getRenderer().setSeriesPaint(2, Color.DARK_GRAY);
        plot2.setRangeGridlinePaint(Color.BLACK);
        plot2.setBackgroundPaint(Color.WHITE);
        BarRenderer renderer = (BarRenderer) plot2.getRenderer();
        renderer.setItemMargin(.1);

        try {
            ChartUtilities.saveChartAsPNG(
                    new File("F:\\II stopień\\Semestr IV\\Praca magisterska\\Analiza danych\\vehicle_vintages_survey\\podkarpackie_paliwa.png"),
                    chart2,
                    500,
                    400
            );
        } catch (IOException e) {
            System.out.println("Wystapil problem podczas tworzenia wykresu");
        }

        JFrame frame = new JFrame("Popularność poszczególnych roczników pojazdów względem" +
                " rodzaju paliwa - województwo podkarpackie");
        frame.setLayout(new FlowLayout());
        frame.getContentPane().add(new ChartPanel(chart));
        frame.getContentPane().add(new ChartPanel(chart2));
        frame.pack();
        frame.setVisible(true);
    }

    public void popularity_of_vintages_of_vehicles_and_fuels_woj_podlaskie() {
        // Obliczanie ilosci samochodow osobowych w danych latach produkcji
        Dataset<Row> samochody_zarej_rp = cepikData.getWojewodztwo_podlaskie()
                .filter(col("rodzaj").equalTo("SAMOCHÓD OSOBOWY")
                        .and(col("data_wyrejestrowania").isNull()))
                .filter(col("rok_produkcji").isNotNull());

        Dataset<Row> pojazdy_leq2003 = samochody_zarej_rp
                .filter(col("rok_produkcji").leq(2003));

        Dataset<Row> pojazdy_gt2003leq2013 = samochody_zarej_rp
                .filter(col("rok_produkcji").gt(2003)
                        .and(col("rok_produkcji").leq(2013)));

        Dataset<Row> pojazdy_gt2013 = samochody_zarej_rp
                .filter(col("rok_produkcji").gt(2013));

        long liczba_leq2003 = pojazdy_leq2003.count();
        long liczba_gt2003leq2013 = pojazdy_gt2003leq2013.count();
        long liczba_gt2013 = pojazdy_gt2013.count();

        if(wszystkieDaneZWojewodztw_roczniki.containsKey("do 2003")) {
            long nowa_wartosc = liczba_leq2003 + wszystkieDaneZWojewodztw_roczniki.get("do 2003");
            wszystkieDaneZWojewodztw_roczniki.remove("do 2003");
            wszystkieDaneZWojewodztw_roczniki.put("do 2003", nowa_wartosc);
        }
        else {
            wszystkieDaneZWojewodztw_roczniki.put("do 2003", liczba_leq2003);
        }

        if(wszystkieDaneZWojewodztw_roczniki.containsKey("2004 - 2013")) {
            long nowa_wartosc = liczba_gt2003leq2013 + wszystkieDaneZWojewodztw_roczniki.get("2004 - 2013");
            wszystkieDaneZWojewodztw_roczniki.remove("2004 - 2013");
            wszystkieDaneZWojewodztw_roczniki.put("2004 - 2013", nowa_wartosc);
        }
        else {
            wszystkieDaneZWojewodztw_roczniki.put("2004 - 2013", liczba_gt2003leq2013);
        }

        if(wszystkieDaneZWojewodztw_roczniki.containsKey("od 2014")) {
            long nowa_wartosc = liczba_gt2013 + wszystkieDaneZWojewodztw_roczniki.get("od 2014");
            wszystkieDaneZWojewodztw_roczniki.remove("od 2014");
            wszystkieDaneZWojewodztw_roczniki.put("od 2014", nowa_wartosc);
        }
        else {
            wszystkieDaneZWojewodztw_roczniki.put("od 2014", liczba_gt2013);
        }

        // Tworzenie wykresu 1 - Porównanie popularności aut z poszczególnych roczników - województwo lubelskie
        DefaultPieDataset dpd = new DefaultPieDataset();
        dpd.setValue("do 2003", liczba_leq2003);
        dpd.setValue("2004 - 2013", liczba_gt2003leq2013);
        dpd.setValue("od 2014", liczba_gt2013);

        JFreeChart chart = ChartFactory.createPieChart(
                "Porównanie popularności aut z poszczególnych roczników" +
                        " - województwo podlaskie",
                dpd,
                true,
                true,
                false
        );

        PiePlot plot = (PiePlot) chart.getPlot();
        plot.setSectionPaint("do 2003", Color.decode("#0096ff"));
        plot.setSectionPaint("2004 - 2013", Color.BLACK);
        plot.setSectionPaint("od 2014", Color.YELLOW);
        plot.setForegroundAlpha(0.5f);
        plot.setExplodePercent("do 2003", 0.20);
        plot.setBackgroundPaint(Color.WHITE);
        plot.setSimpleLabels(true);

        PieSectionLabelGenerator gen = new StandardPieSectionLabelGenerator(
                "{0}: {1} ({2})", new DecimalFormat("0"), new DecimalFormat("0%"));
        plot.setLabelGenerator(gen);

        try {
            ChartUtilities.saveChartAsPNG(
                    new File("F:\\II stopień\\Semestr IV\\Praca magisterska\\Analiza danych\\vehicle_vintages_survey\\podlaskie_ilosc.png"),
                    chart,
                    500,
                    400);
        } catch (Exception e) {
            System.out.println("Wystapil problem podczas tworzenia wykresu");
        }

        // Obliczanie ilosci pojazdow na dany rodzaj paliwa w danym przedziale lat produkcji
        List<Row> paliwa_leq2003 = pojazdy_leq2003
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

        List<Row> paliwa_gt2003leq2013 = pojazdy_gt2003leq2013
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

        List<Row> paliwa_gt2013 = pojazdy_gt2013
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

        // Tworzenie wykresu 2 - Podział samochodów z poszczególnych roczników
        // względem rodzaju paliwa - województwo lubelskie
        DefaultCategoryDataset dcd = new DefaultCategoryDataset();

        for(Row row : paliwa_leq2003) {
            if(row.get(1) == null) {
                dcd.setValue((long)row.get(2), (String)row.get(0), "do 2003");
                if(row.get(0).equals("BENZYNA")) {
                    if(wszystkieDaneZWojewodztw_paliwa.containsKey("PB (do 2003)")) {
                        long nowa_wartosc = (long)row.get(2)
                                + wszystkieDaneZWojewodztw_paliwa.get("PB (do 2003)");
                        wszystkieDaneZWojewodztw_paliwa.remove("PB (do 2003)");
                        wszystkieDaneZWojewodztw_paliwa.put("PB (do 2003)", nowa_wartosc);
                    }
                    else {
                        wszystkieDaneZWojewodztw_paliwa.put("PB (do 2003)", (long)row.get(2));
                    }
                }
                else if(row.get(0).equals("OLEJ NAPĘDOWY")){
                    if(wszystkieDaneZWojewodztw_paliwa.containsKey("ON (do 2003)")) {
                        long nowa_wartosc = (long)row.get(2)
                                + wszystkieDaneZWojewodztw_paliwa.get("ON (do 2003)");
                        wszystkieDaneZWojewodztw_paliwa.remove("ON (do 2003)");
                        wszystkieDaneZWojewodztw_paliwa.put("ON (do 2003)", nowa_wartosc);
                    }
                    else {
                        wszystkieDaneZWojewodztw_paliwa.put("ON (do 2003)", (long)row.get(2));
                    }
                }
            }
            else {
                dcd.setValue((long)row.get(2), row.get(0) + " + LPG", "do 2003");
                if(wszystkieDaneZWojewodztw_paliwa.containsKey("PB+LPG (do 2003)")) {
                    long nowa_wartosc = (long)row.get(2)
                            + wszystkieDaneZWojewodztw_paliwa.get("PB+LPG (do 2003)");
                    wszystkieDaneZWojewodztw_paliwa.remove("PB+LPG (do 2003)");
                    wszystkieDaneZWojewodztw_paliwa.put("PB+LPG (do 2003)", nowa_wartosc);
                }
                else {
                    wszystkieDaneZWojewodztw_paliwa.put("PB+LPG (do 2003)", (long)row.get(2));
                }
            }
        }
        for(Row row : paliwa_gt2003leq2013) {
            if(row.get(1) == null) {
                dcd.setValue((long)row.get(2), (String)row.get(0), "2004 - 2013");
                if(row.get(0).equals("BENZYNA")) {
                    if(wszystkieDaneZWojewodztw_paliwa.containsKey("PB (2004 - 2013)")) {
                        long nowa_wartosc = (long)row.get(2)
                                + wszystkieDaneZWojewodztw_paliwa.get("PB (2004 - 2013)");
                        wszystkieDaneZWojewodztw_paliwa.remove("PB (2004 - 2013)");
                        wszystkieDaneZWojewodztw_paliwa.put("PB (2004 - 2013)", nowa_wartosc);
                    }
                    else {
                        wszystkieDaneZWojewodztw_paliwa.put("PB (2004 - 2013)", (long)row.get(2));
                    }
                }
                else if(row.get(0).equals("OLEJ NAPĘDOWY")){
                    if(wszystkieDaneZWojewodztw_paliwa.containsKey("ON (2004 - 2013)")) {
                        long nowa_wartosc = (long)row.get(2)
                                + wszystkieDaneZWojewodztw_paliwa.get("ON (2004 - 2013)");
                        wszystkieDaneZWojewodztw_paliwa.remove("ON (2004 - 2013)");
                        wszystkieDaneZWojewodztw_paliwa.put("ON (2004 - 2013)", nowa_wartosc);
                    }
                    else {
                        wszystkieDaneZWojewodztw_paliwa.put("ON (2004 - 2013)", (long)row.get(2));
                    }
                }
            }
            else {
                dcd.setValue((long)row.get(2), row.get(0) + " + LPG", "2004 - 2013");
                if(wszystkieDaneZWojewodztw_paliwa.containsKey("PB+LPG (2004 - 2013)")) {
                    long nowa_wartosc = (long)row.get(2)
                            + wszystkieDaneZWojewodztw_paliwa.get("PB+LPG (2004 - 2013)");
                    wszystkieDaneZWojewodztw_paliwa.remove("PB+LPG (2004 - 2013)");
                    wszystkieDaneZWojewodztw_paliwa.put("PB+LPG (2004 - 2013)", nowa_wartosc);
                }
                else {
                    wszystkieDaneZWojewodztw_paliwa.put("PB+LPG (2004 - 2013)", (long)row.get(2));
                }
            }
        }
        for(Row row : paliwa_gt2013) {
            if(row.get(1) == null) {
                dcd.setValue((long)row.get(2), (String)row.get(0), "od 2014");
                if(row.get(0).equals("BENZYNA")) {
                    if(wszystkieDaneZWojewodztw_paliwa.containsKey("PB (od 2014)")) {
                        long nowa_wartosc = (long)row.get(2)
                                + wszystkieDaneZWojewodztw_paliwa.get("PB (od 2014)");
                        wszystkieDaneZWojewodztw_paliwa.remove("PB (od 2014)");
                        wszystkieDaneZWojewodztw_paliwa.put("PB (od 2014)", nowa_wartosc);
                    }
                    else {
                        wszystkieDaneZWojewodztw_paliwa.put("PB (od 2014)", (long)row.get(2));
                    }
                }
                else if(row.get(0).equals("OLEJ NAPĘDOWY")){
                    if(wszystkieDaneZWojewodztw_paliwa.containsKey("ON (od 2014)")) {
                        long nowa_wartosc = (long)row.get(2)
                                + wszystkieDaneZWojewodztw_paliwa.get("ON (od 2014)");
                        wszystkieDaneZWojewodztw_paliwa.remove("ON (od 2014)");
                        wszystkieDaneZWojewodztw_paliwa.put("ON (od 2014)", nowa_wartosc);
                    }
                    else {
                        wszystkieDaneZWojewodztw_paliwa.put("ON (od 2014)", (long)row.get(2));
                    }
                }
            }
            else {
                dcd.setValue((long)row.get(2), row.get(0) + " + LPG", "od 2014");
                if(wszystkieDaneZWojewodztw_paliwa.containsKey("PB+LPG (od 2014)")) {
                    long nowa_wartosc = (long)row.get(2)
                            + wszystkieDaneZWojewodztw_paliwa.get("PB+LPG (od 2014)");
                    wszystkieDaneZWojewodztw_paliwa.remove("PB+LPG (od 2014)");
                    wszystkieDaneZWojewodztw_paliwa.put("PB+LPG (od 2014)", nowa_wartosc);
                }
                else {
                    wszystkieDaneZWojewodztw_paliwa.put("PB+LPG (od 2014)", (long)row.get(2));
                }
            }
        }

        JFreeChart chart2 = ChartFactory.createBarChart(
                "Podział samochodów z poszczególnych roczników względem rodzaju paliwa" +
                        " - województwo podlaskie",
                "Rok produkcji",
                "Ilość pojazdów",
                dcd,
                PlotOrientation.VERTICAL,
                true,
                true,
                false
        );

        CategoryPlot plot2 = (CategoryPlot) chart2.getPlot();
        plot2.setForegroundAlpha(0.7f);
        plot2.getRenderer().setSeriesPaint(0, Color.decode("#006ff9"));
        plot2.getRenderer().setSeriesPaint(1, Color.ORANGE);
        plot2.getRenderer().setSeriesPaint(2, Color.DARK_GRAY);
        plot2.setRangeGridlinePaint(Color.BLACK);
        plot2.setBackgroundPaint(Color.WHITE);
        BarRenderer renderer = (BarRenderer) plot2.getRenderer();
        renderer.setItemMargin(.1);

        try {
            ChartUtilities.saveChartAsPNG(
                    new File("F:\\II stopień\\Semestr IV\\Praca magisterska\\Analiza danych\\vehicle_vintages_survey\\podlaskie_paliwa.png"),
                    chart2,
                    500,
                    400
            );
        } catch (IOException e) {
            System.out.println("Wystapil problem podczas tworzenia wykresu");
        }

        JFrame frame = new JFrame("Popularność poszczególnych roczników pojazdów względem" +
                " rodzaju paliwa - województwo podlaskie");
        frame.setLayout(new FlowLayout());
        frame.getContentPane().add(new ChartPanel(chart));
        frame.getContentPane().add(new ChartPanel(chart2));
        frame.pack();
        frame.setVisible(true);
    }

    public void popularity_of_vintages_of_vehicles_and_fuels_woj_pomorskie() {
        // Obliczanie ilosci samochodow osobowych w danych latach produkcji
        Dataset<Row> samochody_zarej_rp = cepikData.getWojewodztwo_pomorskie()
                .filter(col("rodzaj").equalTo("SAMOCHÓD OSOBOWY")
                        .and(col("data_wyrejestrowania").isNull()))
                .filter(col("rok_produkcji").isNotNull());

        Dataset<Row> pojazdy_leq2003 = samochody_zarej_rp
                .filter(col("rok_produkcji").leq(2003));

        Dataset<Row> pojazdy_gt2003leq2013 = samochody_zarej_rp
                .filter(col("rok_produkcji").gt(2003)
                        .and(col("rok_produkcji").leq(2013)));

        Dataset<Row> pojazdy_gt2013 = samochody_zarej_rp
                .filter(col("rok_produkcji").gt(2013));

        long liczba_leq2003 = pojazdy_leq2003.count();
        long liczba_gt2003leq2013 = pojazdy_gt2003leq2013.count();
        long liczba_gt2013 = pojazdy_gt2013.count();

        if(wszystkieDaneZWojewodztw_roczniki.containsKey("do 2003")) {
            long nowa_wartosc = liczba_leq2003 + wszystkieDaneZWojewodztw_roczniki.get("do 2003");
            wszystkieDaneZWojewodztw_roczniki.remove("do 2003");
            wszystkieDaneZWojewodztw_roczniki.put("do 2003", nowa_wartosc);
        }
        else {
            wszystkieDaneZWojewodztw_roczniki.put("do 2003", liczba_leq2003);
        }

        if(wszystkieDaneZWojewodztw_roczniki.containsKey("2004 - 2013")) {
            long nowa_wartosc = liczba_gt2003leq2013 + wszystkieDaneZWojewodztw_roczniki.get("2004 - 2013");
            wszystkieDaneZWojewodztw_roczniki.remove("2004 - 2013");
            wszystkieDaneZWojewodztw_roczniki.put("2004 - 2013", nowa_wartosc);
        }
        else {
            wszystkieDaneZWojewodztw_roczniki.put("2004 - 2013", liczba_gt2003leq2013);
        }

        if(wszystkieDaneZWojewodztw_roczniki.containsKey("od 2014")) {
            long nowa_wartosc = liczba_gt2013 + wszystkieDaneZWojewodztw_roczniki.get("od 2014");
            wszystkieDaneZWojewodztw_roczniki.remove("od 2014");
            wszystkieDaneZWojewodztw_roczniki.put("od 2014", nowa_wartosc);
        }
        else {
            wszystkieDaneZWojewodztw_roczniki.put("od 2014", liczba_gt2013);
        }

        // Tworzenie wykresu 1 - Porównanie popularności aut z poszczególnych roczników - województwo lubelskie
        DefaultPieDataset dpd = new DefaultPieDataset();
        dpd.setValue("do 2003", liczba_leq2003);
        dpd.setValue("2004 - 2013", liczba_gt2003leq2013);
        dpd.setValue("od 2014", liczba_gt2013);

        JFreeChart chart = ChartFactory.createPieChart(
                "Porównanie popularności aut z poszczególnych roczników" +
                        " - województwo pomorskie",
                dpd,
                true,
                true,
                false
        );

        PiePlot plot = (PiePlot) chart.getPlot();
        plot.setSectionPaint("do 2003", Color.decode("#0096ff"));
        plot.setSectionPaint("2004 - 2013", Color.BLACK);
        plot.setSectionPaint("od 2014", Color.YELLOW);
        plot.setForegroundAlpha(0.5f);
        plot.setExplodePercent("do 2003", 0.20);
        plot.setBackgroundPaint(Color.WHITE);
        plot.setSimpleLabels(true);

        PieSectionLabelGenerator gen = new StandardPieSectionLabelGenerator(
                "{0}: {1} ({2})", new DecimalFormat("0"), new DecimalFormat("0%"));
        plot.setLabelGenerator(gen);

        try {
            ChartUtilities.saveChartAsPNG(
                    new File("F:\\II stopień\\Semestr IV\\Praca magisterska\\Analiza danych\\vehicle_vintages_survey\\pomorskie_ilosc.png"),
                    chart,
                    500,
                    400);
        } catch (Exception e) {
            System.out.println("Wystapil problem podczas tworzenia wykresu");
        }

        // Obliczanie ilosci pojazdow na dany rodzaj paliwa w danym przedziale lat produkcji
        List<Row> paliwa_leq2003 = pojazdy_leq2003
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

        List<Row> paliwa_gt2003leq2013 = pojazdy_gt2003leq2013
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

        List<Row> paliwa_gt2013 = pojazdy_gt2013
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

        // Tworzenie wykresu 2 - Podział samochodów z poszczególnych roczników
        // względem rodzaju paliwa - województwo lubelskie
        DefaultCategoryDataset dcd = new DefaultCategoryDataset();

        for(Row row : paliwa_leq2003) {
            if(row.get(1) == null) {
                dcd.setValue((long)row.get(2), (String)row.get(0), "do 2003");
                if(row.get(0).equals("BENZYNA")) {
                    if(wszystkieDaneZWojewodztw_paliwa.containsKey("PB (do 2003)")) {
                        long nowa_wartosc = (long)row.get(2)
                                + wszystkieDaneZWojewodztw_paliwa.get("PB (do 2003)");
                        wszystkieDaneZWojewodztw_paliwa.remove("PB (do 2003)");
                        wszystkieDaneZWojewodztw_paliwa.put("PB (do 2003)", nowa_wartosc);
                    }
                    else {
                        wszystkieDaneZWojewodztw_paliwa.put("PB (do 2003)", (long)row.get(2));
                    }
                }
                else if(row.get(0).equals("OLEJ NAPĘDOWY")){
                    if(wszystkieDaneZWojewodztw_paliwa.containsKey("ON (do 2003)")) {
                        long nowa_wartosc = (long)row.get(2)
                                + wszystkieDaneZWojewodztw_paliwa.get("ON (do 2003)");
                        wszystkieDaneZWojewodztw_paliwa.remove("ON (do 2003)");
                        wszystkieDaneZWojewodztw_paliwa.put("ON (do 2003)", nowa_wartosc);
                    }
                    else {
                        wszystkieDaneZWojewodztw_paliwa.put("ON (do 2003)", (long)row.get(2));
                    }
                }
            }
            else {
                dcd.setValue((long)row.get(2), row.get(0) + " + LPG", "do 2003");
                if(wszystkieDaneZWojewodztw_paliwa.containsKey("PB+LPG (do 2003)")) {
                    long nowa_wartosc = (long)row.get(2)
                            + wszystkieDaneZWojewodztw_paliwa.get("PB+LPG (do 2003)");
                    wszystkieDaneZWojewodztw_paliwa.remove("PB+LPG (do 2003)");
                    wszystkieDaneZWojewodztw_paliwa.put("PB+LPG (do 2003)", nowa_wartosc);
                }
                else {
                    wszystkieDaneZWojewodztw_paliwa.put("PB+LPG (do 2003)", (long)row.get(2));
                }
            }
        }
        for(Row row : paliwa_gt2003leq2013) {
            if(row.get(1) == null) {
                dcd.setValue((long)row.get(2), (String)row.get(0), "2004 - 2013");
                if(row.get(0).equals("BENZYNA")) {
                    if(wszystkieDaneZWojewodztw_paliwa.containsKey("PB (2004 - 2013)")) {
                        long nowa_wartosc = (long)row.get(2)
                                + wszystkieDaneZWojewodztw_paliwa.get("PB (2004 - 2013)");
                        wszystkieDaneZWojewodztw_paliwa.remove("PB (2004 - 2013)");
                        wszystkieDaneZWojewodztw_paliwa.put("PB (2004 - 2013)", nowa_wartosc);
                    }
                    else {
                        wszystkieDaneZWojewodztw_paliwa.put("PB (2004 - 2013)", (long)row.get(2));
                    }
                }
                else if(row.get(0).equals("OLEJ NAPĘDOWY")){
                    if(wszystkieDaneZWojewodztw_paliwa.containsKey("ON (2004 - 2013)")) {
                        long nowa_wartosc = (long)row.get(2)
                                + wszystkieDaneZWojewodztw_paliwa.get("ON (2004 - 2013)");
                        wszystkieDaneZWojewodztw_paliwa.remove("ON (2004 - 2013)");
                        wszystkieDaneZWojewodztw_paliwa.put("ON (2004 - 2013)", nowa_wartosc);
                    }
                    else {
                        wszystkieDaneZWojewodztw_paliwa.put("ON (2004 - 2013)", (long)row.get(2));
                    }
                }
            }
            else {
                dcd.setValue((long)row.get(2), row.get(0) + " + LPG", "2004 - 2013");
                if(wszystkieDaneZWojewodztw_paliwa.containsKey("PB+LPG (2004 - 2013)")) {
                    long nowa_wartosc = (long)row.get(2)
                            + wszystkieDaneZWojewodztw_paliwa.get("PB+LPG (2004 - 2013)");
                    wszystkieDaneZWojewodztw_paliwa.remove("PB+LPG (2004 - 2013)");
                    wszystkieDaneZWojewodztw_paliwa.put("PB+LPG (2004 - 2013)", nowa_wartosc);
                }
                else {
                    wszystkieDaneZWojewodztw_paliwa.put("PB+LPG (2004 - 2013)", (long)row.get(2));
                }
            }
        }
        for(Row row : paliwa_gt2013) {
            if(row.get(1) == null) {
                dcd.setValue((long)row.get(2), (String)row.get(0), "od 2014");
                if(row.get(0).equals("BENZYNA")) {
                    if(wszystkieDaneZWojewodztw_paliwa.containsKey("PB (od 2014)")) {
                        long nowa_wartosc = (long)row.get(2)
                                + wszystkieDaneZWojewodztw_paliwa.get("PB (od 2014)");
                        wszystkieDaneZWojewodztw_paliwa.remove("PB (od 2014)");
                        wszystkieDaneZWojewodztw_paliwa.put("PB (od 2014)", nowa_wartosc);
                    }
                    else {
                        wszystkieDaneZWojewodztw_paliwa.put("PB (od 2014)", (long)row.get(2));
                    }
                }
                else if(row.get(0).equals("OLEJ NAPĘDOWY")){
                    if(wszystkieDaneZWojewodztw_paliwa.containsKey("ON (od 2014)")) {
                        long nowa_wartosc = (long)row.get(2)
                                + wszystkieDaneZWojewodztw_paliwa.get("ON (od 2014)");
                        wszystkieDaneZWojewodztw_paliwa.remove("ON (od 2014)");
                        wszystkieDaneZWojewodztw_paliwa.put("ON (od 2014)", nowa_wartosc);
                    }
                    else {
                        wszystkieDaneZWojewodztw_paliwa.put("ON (od 2014)", (long)row.get(2));
                    }
                }
            }
            else {
                dcd.setValue((long)row.get(2), row.get(0) + " + LPG", "od 2014");
                if(wszystkieDaneZWojewodztw_paliwa.containsKey("PB+LPG (od 2014)")) {
                    long nowa_wartosc = (long)row.get(2)
                            + wszystkieDaneZWojewodztw_paliwa.get("PB+LPG (od 2014)");
                    wszystkieDaneZWojewodztw_paliwa.remove("PB+LPG (od 2014)");
                    wszystkieDaneZWojewodztw_paliwa.put("PB+LPG (od 2014)", nowa_wartosc);
                }
                else {
                    wszystkieDaneZWojewodztw_paliwa.put("PB+LPG (od 2014)", (long)row.get(2));
                }
            }
        }

        JFreeChart chart2 = ChartFactory.createBarChart(
                "Podział samochodów z poszczególnych roczników względem rodzaju paliwa" +
                        " - województwo pomorskie",
                "Rok produkcji",
                "Ilość pojazdów",
                dcd,
                PlotOrientation.VERTICAL,
                true,
                true,
                false
        );

        CategoryPlot plot2 = (CategoryPlot) chart2.getPlot();
        plot2.setForegroundAlpha(0.7f);
        plot2.getRenderer().setSeriesPaint(0, Color.decode("#006ff9"));
        plot2.getRenderer().setSeriesPaint(1, Color.ORANGE);
        plot2.getRenderer().setSeriesPaint(2, Color.DARK_GRAY);
        plot2.setRangeGridlinePaint(Color.BLACK);
        plot2.setBackgroundPaint(Color.WHITE);
        BarRenderer renderer = (BarRenderer) plot2.getRenderer();
        renderer.setItemMargin(.1);

        try {
            ChartUtilities.saveChartAsPNG(
                    new File("F:\\II stopień\\Semestr IV\\Praca magisterska\\Analiza danych\\vehicle_vintages_survey\\pomorskie_paliwa.png"),
                    chart2,
                    500,
                    400
            );
        } catch (IOException e) {
            System.out.println("Wystapil problem podczas tworzenia wykresu");
        }

        JFrame frame = new JFrame("Popularność poszczególnych roczników pojazdów względem" +
                " rodzaju paliwa - województwo pomorskie");
        frame.setLayout(new FlowLayout());
        frame.getContentPane().add(new ChartPanel(chart));
        frame.getContentPane().add(new ChartPanel(chart2));
        frame.pack();
        frame.setVisible(true);
    }

    public void popularity_of_vintages_of_vehicles_and_fuels_woj_slaskie() {
        // Obliczanie ilosci samochodow osobowych w danych latach produkcji
        Dataset<Row> samochody_zarej_rp = cepikData.getWojewodztwo_slaskie()
                .filter(col("rodzaj").equalTo("SAMOCHÓD OSOBOWY")
                        .and(col("data_wyrejestrowania").isNull()))
                .filter(col("rok_produkcji").isNotNull());

        Dataset<Row> pojazdy_leq2003 = samochody_zarej_rp
                .filter(col("rok_produkcji").leq(2003));

        Dataset<Row> pojazdy_gt2003leq2013 = samochody_zarej_rp
                .filter(col("rok_produkcji").gt(2003)
                        .and(col("rok_produkcji").leq(2013)));

        Dataset<Row> pojazdy_gt2013 = samochody_zarej_rp
                .filter(col("rok_produkcji").gt(2013));

        long liczba_leq2003 = pojazdy_leq2003.count();
        long liczba_gt2003leq2013 = pojazdy_gt2003leq2013.count();
        long liczba_gt2013 = pojazdy_gt2013.count();

        if(wszystkieDaneZWojewodztw_roczniki.containsKey("do 2003")) {
            long nowa_wartosc = liczba_leq2003 + wszystkieDaneZWojewodztw_roczniki.get("do 2003");
            wszystkieDaneZWojewodztw_roczniki.remove("do 2003");
            wszystkieDaneZWojewodztw_roczniki.put("do 2003", nowa_wartosc);
        }
        else {
            wszystkieDaneZWojewodztw_roczniki.put("do 2003", liczba_leq2003);
        }

        if(wszystkieDaneZWojewodztw_roczniki.containsKey("2004 - 2013")) {
            long nowa_wartosc = liczba_gt2003leq2013 + wszystkieDaneZWojewodztw_roczniki.get("2004 - 2013");
            wszystkieDaneZWojewodztw_roczniki.remove("2004 - 2013");
            wszystkieDaneZWojewodztw_roczniki.put("2004 - 2013", nowa_wartosc);
        }
        else {
            wszystkieDaneZWojewodztw_roczniki.put("2004 - 2013", liczba_gt2003leq2013);
        }

        if(wszystkieDaneZWojewodztw_roczniki.containsKey("od 2014")) {
            long nowa_wartosc = liczba_gt2013 + wszystkieDaneZWojewodztw_roczniki.get("od 2014");
            wszystkieDaneZWojewodztw_roczniki.remove("od 2014");
            wszystkieDaneZWojewodztw_roczniki.put("od 2014", nowa_wartosc);
        }
        else {
            wszystkieDaneZWojewodztw_roczniki.put("od 2014", liczba_gt2013);
        }

        // Tworzenie wykresu 1 - Porównanie popularności aut z poszczególnych roczników - województwo lubelskie
        DefaultPieDataset dpd = new DefaultPieDataset();
        dpd.setValue("do 2003", liczba_leq2003);
        dpd.setValue("2004 - 2013", liczba_gt2003leq2013);
        dpd.setValue("od 2014", liczba_gt2013);

        JFreeChart chart = ChartFactory.createPieChart(
                "Porównanie popularności aut z poszczególnych roczników" +
                        " - województwo śląskie",
                dpd,
                true,
                true,
                false
        );

        PiePlot plot = (PiePlot) chart.getPlot();
        plot.setSectionPaint("do 2003", Color.decode("#0096ff"));
        plot.setSectionPaint("2004 - 2013", Color.BLACK);
        plot.setSectionPaint("od 2014", Color.YELLOW);
        plot.setForegroundAlpha(0.5f);
        plot.setExplodePercent("do 2003", 0.20);
        plot.setBackgroundPaint(Color.WHITE);
        plot.setSimpleLabels(true);

        PieSectionLabelGenerator gen = new StandardPieSectionLabelGenerator(
                "{0}: {1} ({2})", new DecimalFormat("0"), new DecimalFormat("0%"));
        plot.setLabelGenerator(gen);

        try {
            ChartUtilities.saveChartAsPNG(
                    new File("F:\\II stopień\\Semestr IV\\Praca magisterska\\Analiza danych\\vehicle_vintages_survey\\slaskie_ilosc.png"),
                    chart,
                    500,
                    400);
        } catch (Exception e) {
            System.out.println("Wystapil problem podczas tworzenia wykresu");
        }

        // Obliczanie ilosci pojazdow na dany rodzaj paliwa w danym przedziale lat produkcji
        List<Row> paliwa_leq2003 = pojazdy_leq2003
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

        List<Row> paliwa_gt2003leq2013 = pojazdy_gt2003leq2013
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

        List<Row> paliwa_gt2013 = pojazdy_gt2013
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

        // Tworzenie wykresu 2 - Podział samochodów z poszczególnych roczników
        // względem rodzaju paliwa - województwo lubelskie
        DefaultCategoryDataset dcd = new DefaultCategoryDataset();

        for(Row row : paliwa_leq2003) {
            if(row.get(1) == null) {
                dcd.setValue((long)row.get(2), (String)row.get(0), "do 2003");
                if(row.get(0).equals("BENZYNA")) {
                    if(wszystkieDaneZWojewodztw_paliwa.containsKey("PB (do 2003)")) {
                        long nowa_wartosc = (long)row.get(2)
                                + wszystkieDaneZWojewodztw_paliwa.get("PB (do 2003)");
                        wszystkieDaneZWojewodztw_paliwa.remove("PB (do 2003)");
                        wszystkieDaneZWojewodztw_paliwa.put("PB (do 2003)", nowa_wartosc);
                    }
                    else {
                        wszystkieDaneZWojewodztw_paliwa.put("PB (do 2003)", (long)row.get(2));
                    }
                }
                else if(row.get(0).equals("OLEJ NAPĘDOWY")){
                    if(wszystkieDaneZWojewodztw_paliwa.containsKey("ON (do 2003)")) {
                        long nowa_wartosc = (long)row.get(2)
                                + wszystkieDaneZWojewodztw_paliwa.get("ON (do 2003)");
                        wszystkieDaneZWojewodztw_paliwa.remove("ON (do 2003)");
                        wszystkieDaneZWojewodztw_paliwa.put("ON (do 2003)", nowa_wartosc);
                    }
                    else {
                        wszystkieDaneZWojewodztw_paliwa.put("ON (do 2003)", (long)row.get(2));
                    }
                }
            }
            else {
                dcd.setValue((long)row.get(2), row.get(0) + " + LPG", "do 2003");
                if(wszystkieDaneZWojewodztw_paliwa.containsKey("PB+LPG (do 2003)")) {
                    long nowa_wartosc = (long)row.get(2)
                            + wszystkieDaneZWojewodztw_paliwa.get("PB+LPG (do 2003)");
                    wszystkieDaneZWojewodztw_paliwa.remove("PB+LPG (do 2003)");
                    wszystkieDaneZWojewodztw_paliwa.put("PB+LPG (do 2003)", nowa_wartosc);
                }
                else {
                    wszystkieDaneZWojewodztw_paliwa.put("PB+LPG (do 2003)", (long)row.get(2));
                }
            }
        }
        for(Row row : paliwa_gt2003leq2013) {
            if(row.get(1) == null) {
                dcd.setValue((long)row.get(2), (String)row.get(0), "2004 - 2013");
                if(row.get(0).equals("BENZYNA")) {
                    if(wszystkieDaneZWojewodztw_paliwa.containsKey("PB (2004 - 2013)")) {
                        long nowa_wartosc = (long)row.get(2)
                                + wszystkieDaneZWojewodztw_paliwa.get("PB (2004 - 2013)");
                        wszystkieDaneZWojewodztw_paliwa.remove("PB (2004 - 2013)");
                        wszystkieDaneZWojewodztw_paliwa.put("PB (2004 - 2013)", nowa_wartosc);
                    }
                    else {
                        wszystkieDaneZWojewodztw_paliwa.put("PB (2004 - 2013)", (long)row.get(2));
                    }
                }
                else if(row.get(0).equals("OLEJ NAPĘDOWY")){
                    if(wszystkieDaneZWojewodztw_paliwa.containsKey("ON (2004 - 2013)")) {
                        long nowa_wartosc = (long)row.get(2)
                                + wszystkieDaneZWojewodztw_paliwa.get("ON (2004 - 2013)");
                        wszystkieDaneZWojewodztw_paliwa.remove("ON (2004 - 2013)");
                        wszystkieDaneZWojewodztw_paliwa.put("ON (2004 - 2013)", nowa_wartosc);
                    }
                    else {
                        wszystkieDaneZWojewodztw_paliwa.put("ON (2004 - 2013)", (long)row.get(2));
                    }
                }
            }
            else {
                dcd.setValue((long)row.get(2), row.get(0) + " + LPG", "2004 - 2013");
                if(wszystkieDaneZWojewodztw_paliwa.containsKey("PB+LPG (2004 - 2013)")) {
                    long nowa_wartosc = (long)row.get(2)
                            + wszystkieDaneZWojewodztw_paliwa.get("PB+LPG (2004 - 2013)");
                    wszystkieDaneZWojewodztw_paliwa.remove("PB+LPG (2004 - 2013)");
                    wszystkieDaneZWojewodztw_paliwa.put("PB+LPG (2004 - 2013)", nowa_wartosc);
                }
                else {
                    wszystkieDaneZWojewodztw_paliwa.put("PB+LPG (2004 - 2013)", (long)row.get(2));
                }
            }
        }
        for(Row row : paliwa_gt2013) {
            if(row.get(1) == null) {
                dcd.setValue((long)row.get(2), (String)row.get(0), "od 2014");
                if(row.get(0).equals("BENZYNA")) {
                    if(wszystkieDaneZWojewodztw_paliwa.containsKey("PB (od 2014)")) {
                        long nowa_wartosc = (long)row.get(2)
                                + wszystkieDaneZWojewodztw_paliwa.get("PB (od 2014)");
                        wszystkieDaneZWojewodztw_paliwa.remove("PB (od 2014)");
                        wszystkieDaneZWojewodztw_paliwa.put("PB (od 2014)", nowa_wartosc);
                    }
                    else {
                        wszystkieDaneZWojewodztw_paliwa.put("PB (od 2014)", (long)row.get(2));
                    }
                }
                else if(row.get(0).equals("OLEJ NAPĘDOWY")){
                    if(wszystkieDaneZWojewodztw_paliwa.containsKey("ON (od 2014)")) {
                        long nowa_wartosc = (long)row.get(2)
                                + wszystkieDaneZWojewodztw_paliwa.get("ON (od 2014)");
                        wszystkieDaneZWojewodztw_paliwa.remove("ON (od 2014)");
                        wszystkieDaneZWojewodztw_paliwa.put("ON (od 2014)", nowa_wartosc);
                    }
                    else {
                        wszystkieDaneZWojewodztw_paliwa.put("ON (od 2014)", (long)row.get(2));
                    }
                }
            }
            else {
                dcd.setValue((long)row.get(2), row.get(0) + " + LPG", "od 2014");
                if(wszystkieDaneZWojewodztw_paliwa.containsKey("PB+LPG (od 2014)")) {
                    long nowa_wartosc = (long)row.get(2)
                            + wszystkieDaneZWojewodztw_paliwa.get("PB+LPG (od 2014)");
                    wszystkieDaneZWojewodztw_paliwa.remove("PB+LPG (od 2014)");
                    wszystkieDaneZWojewodztw_paliwa.put("PB+LPG (od 2014)", nowa_wartosc);
                }
                else {
                    wszystkieDaneZWojewodztw_paliwa.put("PB+LPG (od 2014)", (long)row.get(2));
                }
            }
        }

        JFreeChart chart2 = ChartFactory.createBarChart(
                "Podział samochodów z poszczególnych roczników względem rodzaju paliwa" +
                        " - województwo śląskie",
                "Rok produkcji",
                "Ilość pojazdów",
                dcd,
                PlotOrientation.VERTICAL,
                true,
                true,
                false
        );

        CategoryPlot plot2 = (CategoryPlot) chart2.getPlot();
        plot2.setForegroundAlpha(0.7f);
        plot2.getRenderer().setSeriesPaint(0, Color.decode("#006ff9"));
        plot2.getRenderer().setSeriesPaint(1, Color.ORANGE);
        plot2.getRenderer().setSeriesPaint(2, Color.DARK_GRAY);
        plot2.setRangeGridlinePaint(Color.BLACK);
        plot2.setBackgroundPaint(Color.WHITE);
        BarRenderer renderer = (BarRenderer) plot2.getRenderer();
        renderer.setItemMargin(.1);

        try {
            ChartUtilities.saveChartAsPNG(
                    new File("F:\\II stopień\\Semestr IV\\Praca magisterska\\Analiza danych\\vehicle_vintages_survey\\slaskie_paliwa.png"),
                    chart2,
                    500,
                    400
            );
        } catch (IOException e) {
            System.out.println("Wystapil problem podczas tworzenia wykresu");
        }

        JFrame frame = new JFrame("Popularność poszczególnych roczników pojazdów względem" +
                " rodzaju paliwa - województwo śląskie");
        frame.setLayout(new FlowLayout());
        frame.getContentPane().add(new ChartPanel(chart));
        frame.getContentPane().add(new ChartPanel(chart2));
        frame.pack();
        frame.setVisible(true);
    }

    public void popularity_of_vintages_of_vehicles_and_fuels_woj_swietokrzyskie() {
        // Obliczanie ilosci samochodow osobowych w danych latach produkcji
        Dataset<Row> samochody_zarej_rp = cepikData.getWojewodztwo_swietokrzyskie()
                .filter(col("rodzaj").equalTo("SAMOCHÓD OSOBOWY")
                        .and(col("data_wyrejestrowania").isNull()))
                .filter(col("rok_produkcji").isNotNull());

        Dataset<Row> pojazdy_leq2003 = samochody_zarej_rp
                .filter(col("rok_produkcji").leq(2003));

        Dataset<Row> pojazdy_gt2003leq2013 = samochody_zarej_rp
                .filter(col("rok_produkcji").gt(2003)
                        .and(col("rok_produkcji").leq(2013)));

        Dataset<Row> pojazdy_gt2013 = samochody_zarej_rp
                .filter(col("rok_produkcji").gt(2013));

        long liczba_leq2003 = pojazdy_leq2003.count();
        long liczba_gt2003leq2013 = pojazdy_gt2003leq2013.count();
        long liczba_gt2013 = pojazdy_gt2013.count();

        if(wszystkieDaneZWojewodztw_roczniki.containsKey("do 2003")) {
            long nowa_wartosc = liczba_leq2003 + wszystkieDaneZWojewodztw_roczniki.get("do 2003");
            wszystkieDaneZWojewodztw_roczniki.remove("do 2003");
            wszystkieDaneZWojewodztw_roczniki.put("do 2003", nowa_wartosc);
        }
        else {
            wszystkieDaneZWojewodztw_roczniki.put("do 2003", liczba_leq2003);
        }

        if(wszystkieDaneZWojewodztw_roczniki.containsKey("2004 - 2013")) {
            long nowa_wartosc = liczba_gt2003leq2013 + wszystkieDaneZWojewodztw_roczniki.get("2004 - 2013");
            wszystkieDaneZWojewodztw_roczniki.remove("2004 - 2013");
            wszystkieDaneZWojewodztw_roczniki.put("2004 - 2013", nowa_wartosc);
        }
        else {
            wszystkieDaneZWojewodztw_roczniki.put("2004 - 2013", liczba_gt2003leq2013);
        }

        if(wszystkieDaneZWojewodztw_roczniki.containsKey("od 2014")) {
            long nowa_wartosc = liczba_gt2013 + wszystkieDaneZWojewodztw_roczniki.get("od 2014");
            wszystkieDaneZWojewodztw_roczniki.remove("od 2014");
            wszystkieDaneZWojewodztw_roczniki.put("od 2014", nowa_wartosc);
        }
        else {
            wszystkieDaneZWojewodztw_roczniki.put("od 2014", liczba_gt2013);
        }

        // Tworzenie wykresu 1 - Porównanie popularności aut z poszczególnych roczników - województwo lubelskie
        DefaultPieDataset dpd = new DefaultPieDataset();
        dpd.setValue("do 2003", liczba_leq2003);
        dpd.setValue("2004 - 2013", liczba_gt2003leq2013);
        dpd.setValue("od 2014", liczba_gt2013);

        JFreeChart chart = ChartFactory.createPieChart(
                "Porównanie popularności aut z poszczególnych roczników" +
                        " - województwo świętokrzyskie",
                dpd,
                true,
                true,
                false
        );

        PiePlot plot = (PiePlot) chart.getPlot();
        plot.setSectionPaint("do 2003", Color.decode("#0096ff"));
        plot.setSectionPaint("2004 - 2013", Color.BLACK);
        plot.setSectionPaint("od 2014", Color.YELLOW);
        plot.setForegroundAlpha(0.5f);
        plot.setExplodePercent("do 2003", 0.20);
        plot.setBackgroundPaint(Color.WHITE);
        plot.setSimpleLabels(true);

        PieSectionLabelGenerator gen = new StandardPieSectionLabelGenerator(
                "{0}: {1} ({2})", new DecimalFormat("0"), new DecimalFormat("0%"));
        plot.setLabelGenerator(gen);

        try {
            ChartUtilities.saveChartAsPNG(
                    new File("F:\\II stopień\\Semestr IV\\Praca magisterska\\Analiza danych\\vehicle_vintages_survey\\swietokrzyskie_ilosc.png"),
                    chart,
                    500,
                    400);
        } catch (Exception e) {
            System.out.println("Wystapil problem podczas tworzenia wykresu");
        }

        // Obliczanie ilosci pojazdow na dany rodzaj paliwa w danym przedziale lat produkcji
        List<Row> paliwa_leq2003 = pojazdy_leq2003
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

        List<Row> paliwa_gt2003leq2013 = pojazdy_gt2003leq2013
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

        List<Row> paliwa_gt2013 = pojazdy_gt2013
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

        // Tworzenie wykresu 2 - Podział samochodów z poszczególnych roczników
        // względem rodzaju paliwa - województwo lubelskie
        DefaultCategoryDataset dcd = new DefaultCategoryDataset();

        for(Row row : paliwa_leq2003) {
            if(row.get(1) == null) {
                dcd.setValue((long)row.get(2), (String)row.get(0), "do 2003");
                if(row.get(0).equals("BENZYNA")) {
                    if(wszystkieDaneZWojewodztw_paliwa.containsKey("PB (do 2003)")) {
                        long nowa_wartosc = (long)row.get(2)
                                + wszystkieDaneZWojewodztw_paliwa.get("PB (do 2003)");
                        wszystkieDaneZWojewodztw_paliwa.remove("PB (do 2003)");
                        wszystkieDaneZWojewodztw_paliwa.put("PB (do 2003)", nowa_wartosc);
                    }
                    else {
                        wszystkieDaneZWojewodztw_paliwa.put("PB (do 2003)", (long)row.get(2));
                    }
                }
                else if(row.get(0).equals("OLEJ NAPĘDOWY")){
                    if(wszystkieDaneZWojewodztw_paliwa.containsKey("ON (do 2003)")) {
                        long nowa_wartosc = (long)row.get(2)
                                + wszystkieDaneZWojewodztw_paliwa.get("ON (do 2003)");
                        wszystkieDaneZWojewodztw_paliwa.remove("ON (do 2003)");
                        wszystkieDaneZWojewodztw_paliwa.put("ON (do 2003)", nowa_wartosc);
                    }
                    else {
                        wszystkieDaneZWojewodztw_paliwa.put("ON (do 2003)", (long)row.get(2));
                    }
                }
            }
            else {
                dcd.setValue((long)row.get(2), row.get(0) + " + LPG", "do 2003");
                if(wszystkieDaneZWojewodztw_paliwa.containsKey("PB+LPG (do 2003)")) {
                    long nowa_wartosc = (long)row.get(2)
                            + wszystkieDaneZWojewodztw_paliwa.get("PB+LPG (do 2003)");
                    wszystkieDaneZWojewodztw_paliwa.remove("PB+LPG (do 2003)");
                    wszystkieDaneZWojewodztw_paliwa.put("PB+LPG (do 2003)", nowa_wartosc);
                }
                else {
                    wszystkieDaneZWojewodztw_paliwa.put("PB+LPG (do 2003)", (long)row.get(2));
                }
            }
        }
        for(Row row : paliwa_gt2003leq2013) {
            if(row.get(1) == null) {
                dcd.setValue((long)row.get(2), (String)row.get(0), "2004 - 2013");
                if(row.get(0).equals("BENZYNA")) {
                    if(wszystkieDaneZWojewodztw_paliwa.containsKey("PB (2004 - 2013)")) {
                        long nowa_wartosc = (long)row.get(2)
                                + wszystkieDaneZWojewodztw_paliwa.get("PB (2004 - 2013)");
                        wszystkieDaneZWojewodztw_paliwa.remove("PB (2004 - 2013)");
                        wszystkieDaneZWojewodztw_paliwa.put("PB (2004 - 2013)", nowa_wartosc);
                    }
                    else {
                        wszystkieDaneZWojewodztw_paliwa.put("PB (2004 - 2013)", (long)row.get(2));
                    }
                }
                else if(row.get(0).equals("OLEJ NAPĘDOWY")){
                    if(wszystkieDaneZWojewodztw_paliwa.containsKey("ON (2004 - 2013)")) {
                        long nowa_wartosc = (long)row.get(2)
                                + wszystkieDaneZWojewodztw_paliwa.get("ON (2004 - 2013)");
                        wszystkieDaneZWojewodztw_paliwa.remove("ON (2004 - 2013)");
                        wszystkieDaneZWojewodztw_paliwa.put("ON (2004 - 2013)", nowa_wartosc);
                    }
                    else {
                        wszystkieDaneZWojewodztw_paliwa.put("ON (2004 - 2013)", (long)row.get(2));
                    }
                }
            }
            else {
                dcd.setValue((long)row.get(2), row.get(0) + " + LPG", "2004 - 2013");
                if(wszystkieDaneZWojewodztw_paliwa.containsKey("PB+LPG (2004 - 2013)")) {
                    long nowa_wartosc = (long)row.get(2)
                            + wszystkieDaneZWojewodztw_paliwa.get("PB+LPG (2004 - 2013)");
                    wszystkieDaneZWojewodztw_paliwa.remove("PB+LPG (2004 - 2013)");
                    wszystkieDaneZWojewodztw_paliwa.put("PB+LPG (2004 - 2013)", nowa_wartosc);
                }
                else {
                    wszystkieDaneZWojewodztw_paliwa.put("PB+LPG (2004 - 2013)", (long)row.get(2));
                }
            }
        }
        for(Row row : paliwa_gt2013) {
            if(row.get(1) == null) {
                dcd.setValue((long)row.get(2), (String)row.get(0), "od 2014");
                if(row.get(0).equals("BENZYNA")) {
                    if(wszystkieDaneZWojewodztw_paliwa.containsKey("PB (od 2014)")) {
                        long nowa_wartosc = (long)row.get(2)
                                + wszystkieDaneZWojewodztw_paliwa.get("PB (od 2014)");
                        wszystkieDaneZWojewodztw_paliwa.remove("PB (od 2014)");
                        wszystkieDaneZWojewodztw_paliwa.put("PB (od 2014)", nowa_wartosc);
                    }
                    else {
                        wszystkieDaneZWojewodztw_paliwa.put("PB (od 2014)", (long)row.get(2));
                    }
                }
                else if(row.get(0).equals("OLEJ NAPĘDOWY")){
                    if(wszystkieDaneZWojewodztw_paliwa.containsKey("ON (od 2014)")) {
                        long nowa_wartosc = (long)row.get(2)
                                + wszystkieDaneZWojewodztw_paliwa.get("ON (od 2014)");
                        wszystkieDaneZWojewodztw_paliwa.remove("ON (od 2014)");
                        wszystkieDaneZWojewodztw_paliwa.put("ON (od 2014)", nowa_wartosc);
                    }
                    else {
                        wszystkieDaneZWojewodztw_paliwa.put("ON (od 2014)", (long)row.get(2));
                    }
                }
            }
            else {
                dcd.setValue((long)row.get(2), row.get(0) + " + LPG", "od 2014");
                if(wszystkieDaneZWojewodztw_paliwa.containsKey("PB+LPG (od 2014)")) {
                    long nowa_wartosc = (long)row.get(2)
                            + wszystkieDaneZWojewodztw_paliwa.get("PB+LPG (od 2014)");
                    wszystkieDaneZWojewodztw_paliwa.remove("PB+LPG (od 2014)");
                    wszystkieDaneZWojewodztw_paliwa.put("PB+LPG (od 2014)", nowa_wartosc);
                }
                else {
                    wszystkieDaneZWojewodztw_paliwa.put("PB+LPG (od 2014)", (long)row.get(2));
                }
            }
        }

        JFreeChart chart2 = ChartFactory.createBarChart(
                "Podział samochodów z poszczególnych roczników względem rodzaju paliwa" +
                        " - województwo świętokrzyskie",
                "Rok produkcji",
                "Ilość pojazdów",
                dcd,
                PlotOrientation.VERTICAL,
                true,
                true,
                false
        );

        CategoryPlot plot2 = (CategoryPlot) chart2.getPlot();
        plot2.setForegroundAlpha(0.7f);
        plot2.getRenderer().setSeriesPaint(0, Color.decode("#006ff9"));
        plot2.getRenderer().setSeriesPaint(1, Color.ORANGE);
        plot2.getRenderer().setSeriesPaint(2, Color.DARK_GRAY);
        plot2.setRangeGridlinePaint(Color.BLACK);
        plot2.setBackgroundPaint(Color.WHITE);
        BarRenderer renderer = (BarRenderer) plot2.getRenderer();
        renderer.setItemMargin(.1);

        try {
            ChartUtilities.saveChartAsPNG(
                    new File("F:\\II stopień\\Semestr IV\\Praca magisterska\\Analiza danych\\vehicle_vintages_survey\\swietokrzyskie_paliwa.png"),
                    chart2,
                    500,
                    400
            );
        } catch (IOException e) {
            System.out.println("Wystapil problem podczas tworzenia wykresu");
        }

        JFrame frame = new JFrame("Popularność poszczególnych roczników pojazdów względem" +
                " rodzaju paliwa - województwo świętokrzyskie");
        frame.setLayout(new FlowLayout());
        frame.getContentPane().add(new ChartPanel(chart));
        frame.getContentPane().add(new ChartPanel(chart2));
        frame.pack();
        frame.setVisible(true);
    }

    public void popularity_of_vintages_of_vehicles_and_fuels_woj_warminskomazurskie() {
        // Obliczanie ilosci samochodow osobowych w danych latach produkcji
        Dataset<Row> samochody_zarej_rp = cepikData.getWojewodztwo_warminskomazurskie()
                .filter(col("rodzaj").equalTo("SAMOCHÓD OSOBOWY")
                        .and(col("data_wyrejestrowania").isNull()))
                .filter(col("rok_produkcji").isNotNull());

        Dataset<Row> pojazdy_leq2003 = samochody_zarej_rp
                .filter(col("rok_produkcji").leq(2003));

        Dataset<Row> pojazdy_gt2003leq2013 = samochody_zarej_rp
                .filter(col("rok_produkcji").gt(2003)
                        .and(col("rok_produkcji").leq(2013)));

        Dataset<Row> pojazdy_gt2013 = samochody_zarej_rp
                .filter(col("rok_produkcji").gt(2013));

        long liczba_leq2003 = pojazdy_leq2003.count();
        long liczba_gt2003leq2013 = pojazdy_gt2003leq2013.count();
        long liczba_gt2013 = pojazdy_gt2013.count();

        if(wszystkieDaneZWojewodztw_roczniki.containsKey("do 2003")) {
            long nowa_wartosc = liczba_leq2003 + wszystkieDaneZWojewodztw_roczniki.get("do 2003");
            wszystkieDaneZWojewodztw_roczniki.remove("do 2003");
            wszystkieDaneZWojewodztw_roczniki.put("do 2003", nowa_wartosc);
        }
        else {
            wszystkieDaneZWojewodztw_roczniki.put("do 2003", liczba_leq2003);
        }

        if(wszystkieDaneZWojewodztw_roczniki.containsKey("2004 - 2013")) {
            long nowa_wartosc = liczba_gt2003leq2013 + wszystkieDaneZWojewodztw_roczniki.get("2004 - 2013");
            wszystkieDaneZWojewodztw_roczniki.remove("2004 - 2013");
            wszystkieDaneZWojewodztw_roczniki.put("2004 - 2013", nowa_wartosc);
        }
        else {
            wszystkieDaneZWojewodztw_roczniki.put("2004 - 2013", liczba_gt2003leq2013);
        }

        if(wszystkieDaneZWojewodztw_roczniki.containsKey("od 2014")) {
            long nowa_wartosc = liczba_gt2013 + wszystkieDaneZWojewodztw_roczniki.get("od 2014");
            wszystkieDaneZWojewodztw_roczniki.remove("od 2014");
            wszystkieDaneZWojewodztw_roczniki.put("od 2014", nowa_wartosc);
        }
        else {
            wszystkieDaneZWojewodztw_roczniki.put("od 2014", liczba_gt2013);
        }

        // Tworzenie wykresu 1 - Porównanie popularności aut z poszczególnych roczników - województwo lubelskie
        DefaultPieDataset dpd = new DefaultPieDataset();
        dpd.setValue("do 2003", liczba_leq2003);
        dpd.setValue("2004 - 2013", liczba_gt2003leq2013);
        dpd.setValue("od 2014", liczba_gt2013);

        JFreeChart chart = ChartFactory.createPieChart(
                "Porównanie popularności aut z poszczególnych roczników" +
                        " - województwo warmińsko-mazurskie",
                dpd,
                true,
                true,
                false
        );

        PiePlot plot = (PiePlot) chart.getPlot();
        plot.setSectionPaint("do 2003", Color.decode("#0096ff"));
        plot.setSectionPaint("2004 - 2013", Color.BLACK);
        plot.setSectionPaint("od 2014", Color.YELLOW);
        plot.setForegroundAlpha(0.5f);
        plot.setExplodePercent("do 2003", 0.20);
        plot.setBackgroundPaint(Color.WHITE);
        plot.setSimpleLabels(true);

        PieSectionLabelGenerator gen = new StandardPieSectionLabelGenerator(
                "{0}: {1} ({2})", new DecimalFormat("0"), new DecimalFormat("0%"));
        plot.setLabelGenerator(gen);

        try {
            ChartUtilities.saveChartAsPNG(
                    new File("F:\\II stopień\\Semestr IV\\Praca magisterska\\Analiza danych\\vehicle_vintages_survey\\warminskomazurskie_ilosc.png"),
                    chart,
                    500,
                    400);
        } catch (Exception e) {
            System.out.println("Wystapil problem podczas tworzenia wykresu");
        }

        // Obliczanie ilosci pojazdow na dany rodzaj paliwa w danym przedziale lat produkcji
        List<Row> paliwa_leq2003 = pojazdy_leq2003
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

        List<Row> paliwa_gt2003leq2013 = pojazdy_gt2003leq2013
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

        List<Row> paliwa_gt2013 = pojazdy_gt2013
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

        // Tworzenie wykresu 2 - Podział samochodów z poszczególnych roczników
        // względem rodzaju paliwa - województwo lubelskie
        DefaultCategoryDataset dcd = new DefaultCategoryDataset();

        for(Row row : paliwa_leq2003) {
            if(row.get(1) == null) {
                dcd.setValue((long)row.get(2), (String)row.get(0), "do 2003");
                if(row.get(0).equals("BENZYNA")) {
                    if(wszystkieDaneZWojewodztw_paliwa.containsKey("PB (do 2003)")) {
                        long nowa_wartosc = (long)row.get(2)
                                + wszystkieDaneZWojewodztw_paliwa.get("PB (do 2003)");
                        wszystkieDaneZWojewodztw_paliwa.remove("PB (do 2003)");
                        wszystkieDaneZWojewodztw_paliwa.put("PB (do 2003)", nowa_wartosc);
                    }
                    else {
                        wszystkieDaneZWojewodztw_paliwa.put("PB (do 2003)", (long)row.get(2));
                    }
                }
                else if(row.get(0).equals("OLEJ NAPĘDOWY")){
                    if(wszystkieDaneZWojewodztw_paliwa.containsKey("ON (do 2003)")) {
                        long nowa_wartosc = (long)row.get(2)
                                + wszystkieDaneZWojewodztw_paliwa.get("ON (do 2003)");
                        wszystkieDaneZWojewodztw_paliwa.remove("ON (do 2003)");
                        wszystkieDaneZWojewodztw_paliwa.put("ON (do 2003)", nowa_wartosc);
                    }
                    else {
                        wszystkieDaneZWojewodztw_paliwa.put("ON (do 2003)", (long)row.get(2));
                    }
                }
            }
            else {
                dcd.setValue((long)row.get(2), row.get(0) + " + LPG", "do 2003");
                if(wszystkieDaneZWojewodztw_paliwa.containsKey("PB+LPG (do 2003)")) {
                    long nowa_wartosc = (long)row.get(2)
                            + wszystkieDaneZWojewodztw_paliwa.get("PB+LPG (do 2003)");
                    wszystkieDaneZWojewodztw_paliwa.remove("PB+LPG (do 2003)");
                    wszystkieDaneZWojewodztw_paliwa.put("PB+LPG (do 2003)", nowa_wartosc);
                }
                else {
                    wszystkieDaneZWojewodztw_paliwa.put("PB+LPG (do 2003)", (long)row.get(2));
                }
            }
        }
        for(Row row : paliwa_gt2003leq2013) {
            if(row.get(1) == null) {
                dcd.setValue((long)row.get(2), (String)row.get(0), "2004 - 2013");
                if(row.get(0).equals("BENZYNA")) {
                    if(wszystkieDaneZWojewodztw_paliwa.containsKey("PB (2004 - 2013)")) {
                        long nowa_wartosc = (long)row.get(2)
                                + wszystkieDaneZWojewodztw_paliwa.get("PB (2004 - 2013)");
                        wszystkieDaneZWojewodztw_paliwa.remove("PB (2004 - 2013)");
                        wszystkieDaneZWojewodztw_paliwa.put("PB (2004 - 2013)", nowa_wartosc);
                    }
                    else {
                        wszystkieDaneZWojewodztw_paliwa.put("PB (2004 - 2013)", (long)row.get(2));
                    }
                }
                else if(row.get(0).equals("OLEJ NAPĘDOWY")){
                    if(wszystkieDaneZWojewodztw_paliwa.containsKey("ON (2004 - 2013)")) {
                        long nowa_wartosc = (long)row.get(2)
                                + wszystkieDaneZWojewodztw_paliwa.get("ON (2004 - 2013)");
                        wszystkieDaneZWojewodztw_paliwa.remove("ON (2004 - 2013)");
                        wszystkieDaneZWojewodztw_paliwa.put("ON (2004 - 2013)", nowa_wartosc);
                    }
                    else {
                        wszystkieDaneZWojewodztw_paliwa.put("ON (2004 - 2013)", (long)row.get(2));
                    }
                }
            }
            else {
                dcd.setValue((long)row.get(2), row.get(0) + " + LPG", "2004 - 2013");
                if(wszystkieDaneZWojewodztw_paliwa.containsKey("PB+LPG (2004 - 2013)")) {
                    long nowa_wartosc = (long)row.get(2)
                            + wszystkieDaneZWojewodztw_paliwa.get("PB+LPG (2004 - 2013)");
                    wszystkieDaneZWojewodztw_paliwa.remove("PB+LPG (2004 - 2013)");
                    wszystkieDaneZWojewodztw_paliwa.put("PB+LPG (2004 - 2013)", nowa_wartosc);
                }
                else {
                    wszystkieDaneZWojewodztw_paliwa.put("PB+LPG (2004 - 2013)", (long)row.get(2));
                }
            }
        }
        for(Row row : paliwa_gt2013) {
            if(row.get(1) == null) {
                dcd.setValue((long)row.get(2), (String)row.get(0), "od 2014");
                if(row.get(0).equals("BENZYNA")) {
                    if(wszystkieDaneZWojewodztw_paliwa.containsKey("PB (od 2014)")) {
                        long nowa_wartosc = (long)row.get(2)
                                + wszystkieDaneZWojewodztw_paliwa.get("PB (od 2014)");
                        wszystkieDaneZWojewodztw_paliwa.remove("PB (od 2014)");
                        wszystkieDaneZWojewodztw_paliwa.put("PB (od 2014)", nowa_wartosc);
                    }
                    else {
                        wszystkieDaneZWojewodztw_paliwa.put("PB (od 2014)", (long)row.get(2));
                    }
                }
                else if(row.get(0).equals("OLEJ NAPĘDOWY")){
                    if(wszystkieDaneZWojewodztw_paliwa.containsKey("ON (od 2014)")) {
                        long nowa_wartosc = (long)row.get(2)
                                + wszystkieDaneZWojewodztw_paliwa.get("ON (od 2014)");
                        wszystkieDaneZWojewodztw_paliwa.remove("ON (od 2014)");
                        wszystkieDaneZWojewodztw_paliwa.put("ON (od 2014)", nowa_wartosc);
                    }
                    else {
                        wszystkieDaneZWojewodztw_paliwa.put("ON (od 2014)", (long)row.get(2));
                    }
                }
            }
            else {
                dcd.setValue((long)row.get(2), row.get(0) + " + LPG", "od 2014");
                if(wszystkieDaneZWojewodztw_paliwa.containsKey("PB+LPG (od 2014)")) {
                    long nowa_wartosc = (long)row.get(2)
                            + wszystkieDaneZWojewodztw_paliwa.get("PB+LPG (od 2014)");
                    wszystkieDaneZWojewodztw_paliwa.remove("PB+LPG (od 2014)");
                    wszystkieDaneZWojewodztw_paliwa.put("PB+LPG (od 2014)", nowa_wartosc);
                }
                else {
                    wszystkieDaneZWojewodztw_paliwa.put("PB+LPG (od 2014)", (long)row.get(2));
                }
            }
        }

        JFreeChart chart2 = ChartFactory.createBarChart(
                "Podział samochodów z poszczególnych roczników względem rodzaju paliwa" +
                        " - województwo warmińsko-mazurskie",
                "Rok produkcji",
                "Ilość pojazdów",
                dcd,
                PlotOrientation.VERTICAL,
                true,
                true,
                false
        );

        CategoryPlot plot2 = (CategoryPlot) chart2.getPlot();
        plot2.setForegroundAlpha(0.7f);
        plot2.getRenderer().setSeriesPaint(0, Color.decode("#006ff9"));
        plot2.getRenderer().setSeriesPaint(1, Color.ORANGE);
        plot2.getRenderer().setSeriesPaint(2, Color.DARK_GRAY);
        plot2.setRangeGridlinePaint(Color.BLACK);
        plot2.setBackgroundPaint(Color.WHITE);
        BarRenderer renderer = (BarRenderer) plot2.getRenderer();
        renderer.setItemMargin(.1);

        try {
            ChartUtilities.saveChartAsPNG(
                    new File("F:\\II stopień\\Semestr IV\\Praca magisterska\\Analiza danych\\vehicle_vintages_survey\\warminskomazurskie_paliwa.png"),
                    chart2,
                    500,
                    400
            );
        } catch (IOException e) {
            System.out.println("Wystapil problem podczas tworzenia wykresu");
        }

        JFrame frame = new JFrame("Popularność poszczególnych roczników pojazdów względem" +
                " rodzaju paliwa - województwo warmińsko-mazurskie");
        frame.setLayout(new FlowLayout());
        frame.getContentPane().add(new ChartPanel(chart));
        frame.getContentPane().add(new ChartPanel(chart2));
        frame.pack();
        frame.setVisible(true);
    }

    public void popularity_of_vintages_of_vehicles_and_fuels_woj_wielkopolskie() {
        // Obliczanie ilosci samochodow osobowych w danych latach produkcji
        Dataset<Row> samochody_zarej_rp = cepikData.getWojewodztwo_wielkopolskie()
                .filter(col("rodzaj").equalTo("SAMOCHÓD OSOBOWY")
                        .and(col("data_wyrejestrowania").isNull()))
                .filter(col("rok_produkcji").isNotNull());

        Dataset<Row> pojazdy_leq2003 = samochody_zarej_rp
                .filter(col("rok_produkcji").leq(2003));

        Dataset<Row> pojazdy_gt2003leq2013 = samochody_zarej_rp
                .filter(col("rok_produkcji").gt(2003)
                        .and(col("rok_produkcji").leq(2013)));

        Dataset<Row> pojazdy_gt2013 = samochody_zarej_rp
                .filter(col("rok_produkcji").gt(2013));

        long liczba_leq2003 = pojazdy_leq2003.count();
        long liczba_gt2003leq2013 = pojazdy_gt2003leq2013.count();
        long liczba_gt2013 = pojazdy_gt2013.count();

        if(wszystkieDaneZWojewodztw_roczniki.containsKey("do 2003")) {
            long nowa_wartosc = liczba_leq2003 + wszystkieDaneZWojewodztw_roczniki.get("do 2003");
            wszystkieDaneZWojewodztw_roczniki.remove("do 2003");
            wszystkieDaneZWojewodztw_roczniki.put("do 2003", nowa_wartosc);
        }
        else {
            wszystkieDaneZWojewodztw_roczniki.put("do 2003", liczba_leq2003);
        }

        if(wszystkieDaneZWojewodztw_roczniki.containsKey("2004 - 2013")) {
            long nowa_wartosc = liczba_gt2003leq2013 + wszystkieDaneZWojewodztw_roczniki.get("2004 - 2013");
            wszystkieDaneZWojewodztw_roczniki.remove("2004 - 2013");
            wszystkieDaneZWojewodztw_roczniki.put("2004 - 2013", nowa_wartosc);
        }
        else {
            wszystkieDaneZWojewodztw_roczniki.put("2004 - 2013", liczba_gt2003leq2013);
        }

        if(wszystkieDaneZWojewodztw_roczniki.containsKey("od 2014")) {
            long nowa_wartosc = liczba_gt2013 + wszystkieDaneZWojewodztw_roczniki.get("od 2014");
            wszystkieDaneZWojewodztw_roczniki.remove("od 2014");
            wszystkieDaneZWojewodztw_roczniki.put("od 2014", nowa_wartosc);
        }
        else {
            wszystkieDaneZWojewodztw_roczniki.put("od 2014", liczba_gt2013);
        }

        // Tworzenie wykresu 1 - Porównanie popularności aut z poszczególnych roczników - województwo lubelskie
        DefaultPieDataset dpd = new DefaultPieDataset();
        dpd.setValue("do 2003", liczba_leq2003);
        dpd.setValue("2004 - 2013", liczba_gt2003leq2013);
        dpd.setValue("od 2014", liczba_gt2013);

        JFreeChart chart = ChartFactory.createPieChart(
                "Porównanie popularności aut z poszczególnych roczników" +
                        " - województwo wielkopolskie",
                dpd,
                true,
                true,
                false
        );

        PiePlot plot = (PiePlot) chart.getPlot();
        plot.setSectionPaint("do 2003", Color.decode("#0096ff"));
        plot.setSectionPaint("2004 - 2013", Color.BLACK);
        plot.setSectionPaint("od 2014", Color.YELLOW);
        plot.setForegroundAlpha(0.5f);
        plot.setExplodePercent("do 2003", 0.20);
        plot.setBackgroundPaint(Color.WHITE);
        plot.setSimpleLabels(true);

        PieSectionLabelGenerator gen = new StandardPieSectionLabelGenerator(
                "{0}: {1} ({2})", new DecimalFormat("0"), new DecimalFormat("0%"));
        plot.setLabelGenerator(gen);

        try {
            ChartUtilities.saveChartAsPNG(
                    new File("F:\\II stopień\\Semestr IV\\Praca magisterska\\Analiza danych\\vehicle_vintages_survey\\wielkopolskie_ilosc.png"),
                    chart,
                    500,
                    400);
        } catch (Exception e) {
            System.out.println("Wystapil problem podczas tworzenia wykresu");
        }

        // Obliczanie ilosci pojazdow na dany rodzaj paliwa w danym przedziale lat produkcji
        List<Row> paliwa_leq2003 = pojazdy_leq2003
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

        List<Row> paliwa_gt2003leq2013 = pojazdy_gt2003leq2013
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

        List<Row> paliwa_gt2013 = pojazdy_gt2013
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

        // Tworzenie wykresu 2 - Podział samochodów z poszczególnych roczników
        // względem rodzaju paliwa - województwo lubelskie
        DefaultCategoryDataset dcd = new DefaultCategoryDataset();

        for(Row row : paliwa_leq2003) {
            if(row.get(1) == null) {
                dcd.setValue((long)row.get(2), (String)row.get(0), "do 2003");
                if(row.get(0).equals("BENZYNA")) {
                    if(wszystkieDaneZWojewodztw_paliwa.containsKey("PB (do 2003)")) {
                        long nowa_wartosc = (long)row.get(2)
                                + wszystkieDaneZWojewodztw_paliwa.get("PB (do 2003)");
                        wszystkieDaneZWojewodztw_paliwa.remove("PB (do 2003)");
                        wszystkieDaneZWojewodztw_paliwa.put("PB (do 2003)", nowa_wartosc);
                    }
                    else {
                        wszystkieDaneZWojewodztw_paliwa.put("PB (do 2003)", (long)row.get(2));
                    }
                }
                else if(row.get(0).equals("OLEJ NAPĘDOWY")){
                    if(wszystkieDaneZWojewodztw_paliwa.containsKey("ON (do 2003)")) {
                        long nowa_wartosc = (long)row.get(2)
                                + wszystkieDaneZWojewodztw_paliwa.get("ON (do 2003)");
                        wszystkieDaneZWojewodztw_paliwa.remove("ON (do 2003)");
                        wszystkieDaneZWojewodztw_paliwa.put("ON (do 2003)", nowa_wartosc);
                    }
                    else {
                        wszystkieDaneZWojewodztw_paliwa.put("ON (do 2003)", (long)row.get(2));
                    }
                }
            }
            else {
                dcd.setValue((long)row.get(2), row.get(0) + " + LPG", "do 2003");
                if(wszystkieDaneZWojewodztw_paliwa.containsKey("PB+LPG (do 2003)")) {
                    long nowa_wartosc = (long)row.get(2)
                            + wszystkieDaneZWojewodztw_paliwa.get("PB+LPG (do 2003)");
                    wszystkieDaneZWojewodztw_paliwa.remove("PB+LPG (do 2003)");
                    wszystkieDaneZWojewodztw_paliwa.put("PB+LPG (do 2003)", nowa_wartosc);
                }
                else {
                    wszystkieDaneZWojewodztw_paliwa.put("PB+LPG (do 2003)", (long)row.get(2));
                }
            }
        }
        for(Row row : paliwa_gt2003leq2013) {
            if(row.get(1) == null) {
                dcd.setValue((long)row.get(2), (String)row.get(0), "2004 - 2013");
                if(row.get(0).equals("BENZYNA")) {
                    if(wszystkieDaneZWojewodztw_paliwa.containsKey("PB (2004 - 2013)")) {
                        long nowa_wartosc = (long)row.get(2)
                                + wszystkieDaneZWojewodztw_paliwa.get("PB (2004 - 2013)");
                        wszystkieDaneZWojewodztw_paliwa.remove("PB (2004 - 2013)");
                        wszystkieDaneZWojewodztw_paliwa.put("PB (2004 - 2013)", nowa_wartosc);
                    }
                    else {
                        wszystkieDaneZWojewodztw_paliwa.put("PB (2004 - 2013)", (long)row.get(2));
                    }
                }
                else if(row.get(0).equals("OLEJ NAPĘDOWY")){
                    if(wszystkieDaneZWojewodztw_paliwa.containsKey("ON (2004 - 2013)")) {
                        long nowa_wartosc = (long)row.get(2)
                                + wszystkieDaneZWojewodztw_paliwa.get("ON (2004 - 2013)");
                        wszystkieDaneZWojewodztw_paliwa.remove("ON (2004 - 2013)");
                        wszystkieDaneZWojewodztw_paliwa.put("ON (2004 - 2013)", nowa_wartosc);
                    }
                    else {
                        wszystkieDaneZWojewodztw_paliwa.put("ON (2004 - 2013)", (long)row.get(2));
                    }
                }
            }
            else {
                dcd.setValue((long)row.get(2), row.get(0) + " + LPG", "2004 - 2013");
                if(wszystkieDaneZWojewodztw_paliwa.containsKey("PB+LPG (2004 - 2013)")) {
                    long nowa_wartosc = (long)row.get(2)
                            + wszystkieDaneZWojewodztw_paliwa.get("PB+LPG (2004 - 2013)");
                    wszystkieDaneZWojewodztw_paliwa.remove("PB+LPG (2004 - 2013)");
                    wszystkieDaneZWojewodztw_paliwa.put("PB+LPG (2004 - 2013)", nowa_wartosc);
                }
                else {
                    wszystkieDaneZWojewodztw_paliwa.put("PB+LPG (2004 - 2013)", (long)row.get(2));
                }
            }
        }
        for(Row row : paliwa_gt2013) {
            if(row.get(1) == null) {
                dcd.setValue((long)row.get(2), (String)row.get(0), "od 2014");
                if(row.get(0).equals("BENZYNA")) {
                    if(wszystkieDaneZWojewodztw_paliwa.containsKey("PB (od 2014)")) {
                        long nowa_wartosc = (long)row.get(2)
                                + wszystkieDaneZWojewodztw_paliwa.get("PB (od 2014)");
                        wszystkieDaneZWojewodztw_paliwa.remove("PB (od 2014)");
                        wszystkieDaneZWojewodztw_paliwa.put("PB (od 2014)", nowa_wartosc);
                    }
                    else {
                        wszystkieDaneZWojewodztw_paliwa.put("PB (od 2014)", (long)row.get(2));
                    }
                }
                else if(row.get(0).equals("OLEJ NAPĘDOWY")){
                    if(wszystkieDaneZWojewodztw_paliwa.containsKey("ON (od 2014)")) {
                        long nowa_wartosc = (long)row.get(2)
                                + wszystkieDaneZWojewodztw_paliwa.get("ON (od 2014)");
                        wszystkieDaneZWojewodztw_paliwa.remove("ON (od 2014)");
                        wszystkieDaneZWojewodztw_paliwa.put("ON (od 2014)", nowa_wartosc);
                    }
                    else {
                        wszystkieDaneZWojewodztw_paliwa.put("ON (od 2014)", (long)row.get(2));
                    }
                }
            }
            else {
                dcd.setValue((long)row.get(2), row.get(0) + " + LPG", "od 2014");
                if(wszystkieDaneZWojewodztw_paliwa.containsKey("PB+LPG (od 2014)")) {
                    long nowa_wartosc = (long)row.get(2)
                            + wszystkieDaneZWojewodztw_paliwa.get("PB+LPG (od 2014)");
                    wszystkieDaneZWojewodztw_paliwa.remove("PB+LPG (od 2014)");
                    wszystkieDaneZWojewodztw_paliwa.put("PB+LPG (od 2014)", nowa_wartosc);
                }
                else {
                    wszystkieDaneZWojewodztw_paliwa.put("PB+LPG (od 2014)", (long)row.get(2));
                }
            }
        }

        JFreeChart chart2 = ChartFactory.createBarChart(
                "Podział samochodów z poszczególnych roczników względem rodzaju paliwa" +
                        " - województwo wielkopolskie",
                "Rok produkcji",
                "Ilość pojazdów",
                dcd,
                PlotOrientation.VERTICAL,
                true,
                true,
                false
        );

        CategoryPlot plot2 = (CategoryPlot) chart2.getPlot();
        plot2.setForegroundAlpha(0.7f);
        plot2.getRenderer().setSeriesPaint(0, Color.decode("#006ff9"));
        plot2.getRenderer().setSeriesPaint(1, Color.ORANGE);
        plot2.getRenderer().setSeriesPaint(2, Color.DARK_GRAY);
        plot2.setRangeGridlinePaint(Color.BLACK);
        plot2.setBackgroundPaint(Color.WHITE);
        BarRenderer renderer = (BarRenderer) plot2.getRenderer();
        renderer.setItemMargin(.1);

        try {
            ChartUtilities.saveChartAsPNG(
                    new File("F:\\II stopień\\Semestr IV\\Praca magisterska\\Analiza danych\\vehicle_vintages_survey\\wielkopolskie_paliwa.png"),
                    chart2,
                    500,
                    400
            );
        } catch (IOException e) {
            System.out.println("Wystapil problem podczas tworzenia wykresu");
        }

        JFrame frame = new JFrame("Popularność poszczególnych roczników pojazdów względem" +
                " rodzaju paliwa - województwo wielkopolskie");
        frame.setLayout(new FlowLayout());
        frame.getContentPane().add(new ChartPanel(chart));
        frame.getContentPane().add(new ChartPanel(chart2));
        frame.pack();
        frame.setVisible(true);
    }

    public void popularity_of_vintages_of_vehicles_and_fuels_woj_zachodniopomorskie() {
        // Obliczanie ilosci samochodow osobowych w danych latach produkcji
        Dataset<Row> samochody_zarej_rp = cepikData.getWojewodztwo_zachodniopomorskie()
                .filter(col("rodzaj").equalTo("SAMOCHÓD OSOBOWY")
                        .and(col("data_wyrejestrowania").isNull()))
                .filter(col("rok_produkcji").isNotNull());

        Dataset<Row> pojazdy_leq2003 = samochody_zarej_rp
                .filter(col("rok_produkcji").leq(2003));

        Dataset<Row> pojazdy_gt2003leq2013 = samochody_zarej_rp
                .filter(col("rok_produkcji").gt(2003)
                        .and(col("rok_produkcji").leq(2013)));

        Dataset<Row> pojazdy_gt2013 = samochody_zarej_rp
                .filter(col("rok_produkcji").gt(2013));

        long liczba_leq2003 = pojazdy_leq2003.count();
        long liczba_gt2003leq2013 = pojazdy_gt2003leq2013.count();
        long liczba_gt2013 = pojazdy_gt2013.count();

        if(wszystkieDaneZWojewodztw_roczniki.containsKey("do 2003")) {
            long nowa_wartosc = liczba_leq2003 + wszystkieDaneZWojewodztw_roczniki.get("do 2003");
            wszystkieDaneZWojewodztw_roczniki.remove("do 2003");
            wszystkieDaneZWojewodztw_roczniki.put("do 2003", nowa_wartosc);
        }
        else {
            wszystkieDaneZWojewodztw_roczniki.put("do 2003", liczba_leq2003);
        }

        if(wszystkieDaneZWojewodztw_roczniki.containsKey("2004 - 2013")) {
            long nowa_wartosc = liczba_gt2003leq2013 + wszystkieDaneZWojewodztw_roczniki.get("2004 - 2013");
            wszystkieDaneZWojewodztw_roczniki.remove("2004 - 2013");
            wszystkieDaneZWojewodztw_roczniki.put("2004 - 2013", nowa_wartosc);
        }
        else {
            wszystkieDaneZWojewodztw_roczniki.put("2004 - 2013", liczba_gt2003leq2013);
        }

        if(wszystkieDaneZWojewodztw_roczniki.containsKey("od 2014")) {
            long nowa_wartosc = liczba_gt2013 + wszystkieDaneZWojewodztw_roczniki.get("od 2014");
            wszystkieDaneZWojewodztw_roczniki.remove("od 2014");
            wszystkieDaneZWojewodztw_roczniki.put("od 2014", nowa_wartosc);
        }
        else {
            wszystkieDaneZWojewodztw_roczniki.put("od 2014", liczba_gt2013);
        }

        // Tworzenie wykresu 1 - Porównanie popularności aut z poszczególnych roczników - województwo lubelskie
        DefaultPieDataset dpd = new DefaultPieDataset();
        dpd.setValue("do 2003", liczba_leq2003);
        dpd.setValue("2004 - 2013", liczba_gt2003leq2013);
        dpd.setValue("od 2014", liczba_gt2013);

        JFreeChart chart = ChartFactory.createPieChart(
                "Porównanie popularności aut z poszczególnych roczników" +
                        " - województwo zachodniopomorskie",
                dpd,
                true,
                true,
                false
        );

        PiePlot plot = (PiePlot) chart.getPlot();
        plot.setSectionPaint("do 2003", Color.decode("#0096ff"));
        plot.setSectionPaint("2004 - 2013", Color.BLACK);
        plot.setSectionPaint("od 2014", Color.YELLOW);
        plot.setForegroundAlpha(0.5f);
        plot.setExplodePercent("do 2003", 0.20);
        plot.setBackgroundPaint(Color.WHITE);
        plot.setSimpleLabels(true);

        PieSectionLabelGenerator gen = new StandardPieSectionLabelGenerator(
                "{0}: {1} ({2})", new DecimalFormat("0"), new DecimalFormat("0%"));
        plot.setLabelGenerator(gen);

        try {
            ChartUtilities.saveChartAsPNG(
                    new File("F:\\II stopień\\Semestr IV\\Praca magisterska\\Analiza danych\\vehicle_vintages_survey\\zachodniopomorskie_ilosc.png"),
                    chart,
                    500,
                    400);
        } catch (Exception e) {
            System.out.println("Wystapil problem podczas tworzenia wykresu");
        }

        // Obliczanie ilosci pojazdow na dany rodzaj paliwa w danym przedziale lat produkcji
        List<Row> paliwa_leq2003 = pojazdy_leq2003
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

        List<Row> paliwa_gt2003leq2013 = pojazdy_gt2003leq2013
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

        List<Row> paliwa_gt2013 = pojazdy_gt2013
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

        // Tworzenie wykresu 2 - Podział samochodów z poszczególnych roczników
        // względem rodzaju paliwa - województwo lubelskie
        DefaultCategoryDataset dcd = new DefaultCategoryDataset();

        for(Row row : paliwa_leq2003) {
            if(row.get(1) == null) {
                dcd.setValue((long)row.get(2), (String)row.get(0), "do 2003");
                if(row.get(0).equals("BENZYNA")) {
                    if(wszystkieDaneZWojewodztw_paliwa.containsKey("PB (do 2003)")) {
                        long nowa_wartosc = (long)row.get(2)
                                + wszystkieDaneZWojewodztw_paliwa.get("PB (do 2003)");
                        wszystkieDaneZWojewodztw_paliwa.remove("PB (do 2003)");
                        wszystkieDaneZWojewodztw_paliwa.put("PB (do 2003)", nowa_wartosc);
                    }
                    else {
                        wszystkieDaneZWojewodztw_paliwa.put("PB (do 2003)", (long)row.get(2));
                    }
                }
                else if(row.get(0).equals("OLEJ NAPĘDOWY")){
                    if(wszystkieDaneZWojewodztw_paliwa.containsKey("ON (do 2003)")) {
                        long nowa_wartosc = (long)row.get(2)
                                + wszystkieDaneZWojewodztw_paliwa.get("ON (do 2003)");
                        wszystkieDaneZWojewodztw_paliwa.remove("ON (do 2003)");
                        wszystkieDaneZWojewodztw_paliwa.put("ON (do 2003)", nowa_wartosc);
                    }
                    else {
                        wszystkieDaneZWojewodztw_paliwa.put("ON (do 2003)", (long)row.get(2));
                    }
                }
            }
            else {
                dcd.setValue((long)row.get(2), row.get(0) + " + LPG", "do 2003");
                if(wszystkieDaneZWojewodztw_paliwa.containsKey("PB+LPG (do 2003)")) {
                    long nowa_wartosc = (long)row.get(2)
                            + wszystkieDaneZWojewodztw_paliwa.get("PB+LPG (do 2003)");
                    wszystkieDaneZWojewodztw_paliwa.remove("PB+LPG (do 2003)");
                    wszystkieDaneZWojewodztw_paliwa.put("PB+LPG (do 2003)", nowa_wartosc);
                }
                else {
                    wszystkieDaneZWojewodztw_paliwa.put("PB+LPG (do 2003)", (long)row.get(2));
                }
            }
        }
        for(Row row : paliwa_gt2003leq2013) {
            if(row.get(1) == null) {
                dcd.setValue((long)row.get(2), (String)row.get(0), "2004 - 2013");
                if(row.get(0).equals("BENZYNA")) {
                    if(wszystkieDaneZWojewodztw_paliwa.containsKey("PB (2004 - 2013)")) {
                        long nowa_wartosc = (long)row.get(2)
                                + wszystkieDaneZWojewodztw_paliwa.get("PB (2004 - 2013)");
                        wszystkieDaneZWojewodztw_paliwa.remove("PB (2004 - 2013)");
                        wszystkieDaneZWojewodztw_paliwa.put("PB (2004 - 2013)", nowa_wartosc);
                    }
                    else {
                        wszystkieDaneZWojewodztw_paliwa.put("PB (2004 - 2013)", (long)row.get(2));
                    }
                }
                else if(row.get(0).equals("OLEJ NAPĘDOWY")){
                    if(wszystkieDaneZWojewodztw_paliwa.containsKey("ON (2004 - 2013)")) {
                        long nowa_wartosc = (long)row.get(2)
                                + wszystkieDaneZWojewodztw_paliwa.get("ON (2004 - 2013)");
                        wszystkieDaneZWojewodztw_paliwa.remove("ON (2004 - 2013)");
                        wszystkieDaneZWojewodztw_paliwa.put("ON (2004 - 2013)", nowa_wartosc);
                    }
                    else {
                        wszystkieDaneZWojewodztw_paliwa.put("ON (2004 - 2013)", (long)row.get(2));
                    }
                }
            }
            else {
                dcd.setValue((long)row.get(2), row.get(0) + " + LPG", "2004 - 2013");
                if(wszystkieDaneZWojewodztw_paliwa.containsKey("PB+LPG (2004 - 2013)")) {
                    long nowa_wartosc = (long)row.get(2)
                            + wszystkieDaneZWojewodztw_paliwa.get("PB+LPG (2004 - 2013)");
                    wszystkieDaneZWojewodztw_paliwa.remove("PB+LPG (2004 - 2013)");
                    wszystkieDaneZWojewodztw_paliwa.put("PB+LPG (2004 - 2013)", nowa_wartosc);
                }
                else {
                    wszystkieDaneZWojewodztw_paliwa.put("PB+LPG (2004 - 2013)", (long)row.get(2));
                }
            }
        }
        for(Row row : paliwa_gt2013) {
            if(row.get(1) == null) {
                dcd.setValue((long)row.get(2), (String)row.get(0), "od 2014");
                if(row.get(0).equals("BENZYNA")) {
                    if(wszystkieDaneZWojewodztw_paliwa.containsKey("PB (od 2014)")) {
                        long nowa_wartosc = (long)row.get(2)
                                + wszystkieDaneZWojewodztw_paliwa.get("PB (od 2014)");
                        wszystkieDaneZWojewodztw_paliwa.remove("PB (od 2014)");
                        wszystkieDaneZWojewodztw_paliwa.put("PB (od 2014)", nowa_wartosc);
                    }
                    else {
                        wszystkieDaneZWojewodztw_paliwa.put("PB (od 2014)", (long)row.get(2));
                    }
                }
                else if(row.get(0).equals("OLEJ NAPĘDOWY")){
                    if(wszystkieDaneZWojewodztw_paliwa.containsKey("ON (od 2014)")) {
                        long nowa_wartosc = (long)row.get(2)
                                + wszystkieDaneZWojewodztw_paliwa.get("ON (od 2014)");
                        wszystkieDaneZWojewodztw_paliwa.remove("ON (od 2014)");
                        wszystkieDaneZWojewodztw_paliwa.put("ON (od 2014)", nowa_wartosc);
                    }
                    else {
                        wszystkieDaneZWojewodztw_paliwa.put("ON (od 2014)", (long)row.get(2));
                    }
                }
            }
            else {
                dcd.setValue((long)row.get(2), row.get(0) + " + LPG", "od 2014");
                if(wszystkieDaneZWojewodztw_paliwa.containsKey("PB+LPG (od 2014)")) {
                    long nowa_wartosc = (long)row.get(2)
                            + wszystkieDaneZWojewodztw_paliwa.get("PB+LPG (od 2014)");
                    wszystkieDaneZWojewodztw_paliwa.remove("PB+LPG (od 2014)");
                    wszystkieDaneZWojewodztw_paliwa.put("PB+LPG (od 2014)", nowa_wartosc);
                }
                else {
                    wszystkieDaneZWojewodztw_paliwa.put("PB+LPG (od 2014)", (long)row.get(2));
                }
            }
        }

        JFreeChart chart2 = ChartFactory.createBarChart(
                "Podział samochodów z poszczególnych roczników względem rodzaju paliwa" +
                        " - województwo zachodniopomorskie",
                "Rok produkcji",
                "Ilość pojazdów",
                dcd,
                PlotOrientation.VERTICAL,
                true,
                true,
                false
        );

        CategoryPlot plot2 = (CategoryPlot) chart2.getPlot();
        plot2.setForegroundAlpha(0.7f);
        plot2.getRenderer().setSeriesPaint(0, Color.decode("#006ff9"));
        plot2.getRenderer().setSeriesPaint(1, Color.ORANGE);
        plot2.getRenderer().setSeriesPaint(2, Color.DARK_GRAY);
        plot2.setRangeGridlinePaint(Color.BLACK);
        plot2.setBackgroundPaint(Color.WHITE);
        BarRenderer renderer = (BarRenderer) plot2.getRenderer();
        renderer.setItemMargin(.1);

        try {
            ChartUtilities.saveChartAsPNG(
                    new File("F:\\II stopień\\Semestr IV\\Praca magisterska\\Analiza danych\\vehicle_vintages_survey\\zachodniopomorskie_paliwa.png"),
                    chart2,
                    500,
                    400
            );
        } catch (IOException e) {
            System.out.println("Wystapil problem podczas tworzenia wykresu");
        }

        JFrame frame = new JFrame("Popularność poszczególnych roczników pojazdów względem" +
                " rodzaju paliwa - województwo zachodniopomorskie");
        frame.setLayout(new FlowLayout());
        frame.getContentPane().add(new ChartPanel(chart));
        frame.getContentPane().add(new ChartPanel(chart2));
        frame.pack();
        frame.setVisible(true);
    }

    public void popularity_of_vintages_of_vehicles_and_fuels_poland() {
        // Obliczanie ilosci samochodow osobowych w danych latach produkcji
        Dataset<Row> samochody_zarej_rp = cepikData.getNieokreslone_wojewodztwo()
                .filter(col("rodzaj").equalTo("SAMOCHÓD OSOBOWY")
                        .and(col("data_wyrejestrowania").isNull()))
                .filter(col("rok_produkcji").isNotNull());

        Dataset<Row> pojazdy_leq2003 = samochody_zarej_rp
                .filter(col("rok_produkcji").leq(2003));

        Dataset<Row> pojazdy_gt2003leq2013 = samochody_zarej_rp
                .filter(col("rok_produkcji").gt(2003)
                        .and(col("rok_produkcji").leq(2013)));

        Dataset<Row> pojazdy_gt2013 = samochody_zarej_rp
                .filter(col("rok_produkcji").gt(2013));

        long liczba_leq2003 = pojazdy_leq2003.count();
        long liczba_gt2003leq2013 = pojazdy_gt2003leq2013.count();
        long liczba_gt2013 = pojazdy_gt2013.count();

        if(wszystkieDaneZWojewodztw_roczniki.containsKey("do 2003")) {
            long nowa_wartosc = liczba_leq2003 + wszystkieDaneZWojewodztw_roczniki.get("do 2003");
            wszystkieDaneZWojewodztw_roczniki.remove("do 2003");
            wszystkieDaneZWojewodztw_roczniki.put("do 2003", nowa_wartosc);
        }
        else {
            wszystkieDaneZWojewodztw_roczniki.put("do 2003", liczba_leq2003);
        }

        if(wszystkieDaneZWojewodztw_roczniki.containsKey("2004 - 2013")) {
            long nowa_wartosc = liczba_gt2003leq2013 + wszystkieDaneZWojewodztw_roczniki.get("2004 - 2013");
            wszystkieDaneZWojewodztw_roczniki.remove("2004 - 2013");
            wszystkieDaneZWojewodztw_roczniki.put("2004 - 2013", nowa_wartosc);
        }
        else {
            wszystkieDaneZWojewodztw_roczniki.put("2004 - 2013", liczba_gt2003leq2013);
        }

        if(wszystkieDaneZWojewodztw_roczniki.containsKey("od 2014")) {
            long nowa_wartosc = liczba_gt2013 + wszystkieDaneZWojewodztw_roczniki.get("od 2014");
            wszystkieDaneZWojewodztw_roczniki.remove("od 2014");
            wszystkieDaneZWojewodztw_roczniki.put("od 2014", nowa_wartosc);
        }
        else {
            wszystkieDaneZWojewodztw_roczniki.put("od 2014", liczba_gt2013);
        }

        // Obliczanie ilosci pojazdow na dany rodzaj paliwa w danym przedziale lat produkcji
        List<Row> paliwa_leq2003 = pojazdy_leq2003
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

        List<Row> paliwa_gt2003leq2013 = pojazdy_gt2003leq2013
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

        List<Row> paliwa_gt2013 = pojazdy_gt2013
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


        // Podział samochodów z poszczególnych roczników względem rodzaju paliwa
        // nieokreslone wojewodztwo
        for(Row row : paliwa_leq2003) {
            if(row.get(1) == null) {
                if(row.get(0).equals("BENZYNA")) {
                    if(wszystkieDaneZWojewodztw_paliwa.containsKey("PB (do 2003)")) {
                        long nowa_wartosc = (long)row.get(2)
                                + wszystkieDaneZWojewodztw_paliwa.get("PB (do 2003)");
                        wszystkieDaneZWojewodztw_paliwa.remove("PB (do 2003)");
                        wszystkieDaneZWojewodztw_paliwa.put("PB (do 2003)", nowa_wartosc);
                    }
                    else {
                        wszystkieDaneZWojewodztw_paliwa.put("PB (do 2003)", (long)row.get(2));
                    }
                }
                else if(row.get(0).equals("OLEJ NAPĘDOWY")){
                    if(wszystkieDaneZWojewodztw_paliwa.containsKey("ON (do 2003)")) {
                        long nowa_wartosc = (long)row.get(2)
                                + wszystkieDaneZWojewodztw_paliwa.get("ON (do 2003)");
                        wszystkieDaneZWojewodztw_paliwa.remove("ON (do 2003)");
                        wszystkieDaneZWojewodztw_paliwa.put("ON (do 2003)", nowa_wartosc);
                    }
                    else {
                        wszystkieDaneZWojewodztw_paliwa.put("ON (do 2003)", (long)row.get(2));
                    }
                }
            }
            else {
                if(wszystkieDaneZWojewodztw_paliwa.containsKey("PB+LPG (do 2003)")) {
                    long nowa_wartosc = (long)row.get(2)
                            + wszystkieDaneZWojewodztw_paliwa.get("PB+LPG (do 2003)");
                    wszystkieDaneZWojewodztw_paliwa.remove("PB+LPG (do 2003)");
                    wszystkieDaneZWojewodztw_paliwa.put("PB+LPG (do 2003)", nowa_wartosc);
                }
                else {
                    wszystkieDaneZWojewodztw_paliwa.put("PB+LPG (do 2003)", (long)row.get(2));
                }
            }
        }
        for(Row row : paliwa_gt2003leq2013) {
            if(row.get(1) == null) {
                if(row.get(0).equals("BENZYNA")) {
                    if(wszystkieDaneZWojewodztw_paliwa.containsKey("PB (2004 - 2013)")) {
                        long nowa_wartosc = (long)row.get(2)
                                + wszystkieDaneZWojewodztw_paliwa.get("PB (2004 - 2013)");
                        wszystkieDaneZWojewodztw_paliwa.remove("PB (2004 - 2013)");
                        wszystkieDaneZWojewodztw_paliwa.put("PB (2004 - 2013)", nowa_wartosc);
                    }
                    else {
                        wszystkieDaneZWojewodztw_paliwa.put("PB (2004 - 2013)", (long)row.get(2));
                    }
                }
                else if(row.get(0).equals("OLEJ NAPĘDOWY")){
                    if(wszystkieDaneZWojewodztw_paliwa.containsKey("ON (2004 - 2013)")) {
                        long nowa_wartosc = (long)row.get(2)
                                + wszystkieDaneZWojewodztw_paliwa.get("ON (2004 - 2013)");
                        wszystkieDaneZWojewodztw_paliwa.remove("ON (2004 - 2013)");
                        wszystkieDaneZWojewodztw_paliwa.put("ON (2004 - 2013)", nowa_wartosc);
                    }
                    else {
                        wszystkieDaneZWojewodztw_paliwa.put("ON (2004 - 2013)", (long)row.get(2));
                    }
                }
            }
            else {
                if(wszystkieDaneZWojewodztw_paliwa.containsKey("PB+LPG (2004 - 2013)")) {
                    long nowa_wartosc = (long)row.get(2)
                            + wszystkieDaneZWojewodztw_paliwa.get("PB+LPG (2004 - 2013)");
                    wszystkieDaneZWojewodztw_paliwa.remove("PB+LPG (2004 - 2013)");
                    wszystkieDaneZWojewodztw_paliwa.put("PB+LPG (2004 - 2013)", nowa_wartosc);
                }
                else {
                    wszystkieDaneZWojewodztw_paliwa.put("PB+LPG (2004 - 2013)", (long)row.get(2));
                }
            }
        }
        for(Row row : paliwa_gt2013) {
            if(row.get(1) == null) {
                if(row.get(0).equals("BENZYNA")) {
                    if(wszystkieDaneZWojewodztw_paliwa.containsKey("PB (od 2014)")) {
                        long nowa_wartosc = (long)row.get(2)
                                + wszystkieDaneZWojewodztw_paliwa.get("PB (od 2014)");
                        wszystkieDaneZWojewodztw_paliwa.remove("PB (od 2014)");
                        wszystkieDaneZWojewodztw_paliwa.put("PB (od 2014)", nowa_wartosc);
                    }
                    else {
                        wszystkieDaneZWojewodztw_paliwa.put("PB (od 2014)", (long)row.get(2));
                    }
                }
                else if(row.get(0).equals("OLEJ NAPĘDOWY")){
                    if(wszystkieDaneZWojewodztw_paliwa.containsKey("ON (od 2014)")) {
                        long nowa_wartosc = (long)row.get(2)
                                + wszystkieDaneZWojewodztw_paliwa.get("ON (od 2014)");
                        wszystkieDaneZWojewodztw_paliwa.remove("ON (od 2014)");
                        wszystkieDaneZWojewodztw_paliwa.put("ON (od 2014)", nowa_wartosc);
                    }
                    else {
                        wszystkieDaneZWojewodztw_paliwa.put("ON (od 2014)", (long)row.get(2));
                    }
                }
            }
            else {
                if(wszystkieDaneZWojewodztw_paliwa.containsKey("PB+LPG (od 2014)")) {
                    long nowa_wartosc = (long)row.get(2)
                            + wszystkieDaneZWojewodztw_paliwa.get("PB+LPG (od 2014)");
                    wszystkieDaneZWojewodztw_paliwa.remove("PB+LPG (od 2014)");
                    wszystkieDaneZWojewodztw_paliwa.put("PB+LPG (od 2014)", nowa_wartosc);
                }
                else {
                    wszystkieDaneZWojewodztw_paliwa.put("PB+LPG (od 2014)", (long)row.get(2));
                }
            }
        }

        // tworzenie wykresow dla Polski
        // ROCZNIKI
        DefaultPieDataset dpd = new DefaultPieDataset();
        dpd.setValue("do 2003", wszystkieDaneZWojewodztw_roczniki.get("do 2003"));
        dpd.setValue("2004 - 2013", wszystkieDaneZWojewodztw_roczniki.get("2004 - 2013"));
        dpd.setValue("od 2014", wszystkieDaneZWojewodztw_roczniki.get("od 2014"));

        JFreeChart chart = ChartFactory.createPieChart(
                "Porównanie popularności aut z poszczególnych roczników" +
                        " w całym kraju (Polska) z uwzględnieniem danych bez przydziału województwa",
                dpd,
                true,
                true,
                false
        );

        PiePlot plot = (PiePlot) chart.getPlot();
        plot.setSectionPaint("do 2003", Color.decode("#0096ff"));
        plot.setSectionPaint("2004 - 2013", Color.BLACK);
        plot.setSectionPaint("od 2014", Color.YELLOW);
        plot.setForegroundAlpha(0.5f);
        plot.setExplodePercent("do 2003", 0.20);
        plot.setBackgroundPaint(Color.WHITE);
        plot.setSimpleLabels(true);

        PieSectionLabelGenerator gen = new StandardPieSectionLabelGenerator(
                "{0}: {1} ({2})", new DecimalFormat("0"), new DecimalFormat("0%"));
        plot.setLabelGenerator(gen);

        try {
            ChartUtilities.saveChartAsPNG(
                    new File("F:\\II stopień\\Semestr IV\\Praca magisterska\\Analiza danych\\vehicle_vintages_survey\\polska_ilosc.png"),
                    chart,
                    500,
                    400);
        } catch (Exception e) {
            System.out.println("Wystapil problem podczas tworzenia wykresu");
        }

        wszystkieDaneZWojewodztw_roczniki.forEach((k,v) -> {
            System.out.println(k + " : " + v);
        });

        // PALIWA
        DefaultCategoryDataset dcd = new DefaultCategoryDataset();
        dcd.setValue(wszystkieDaneZWojewodztw_paliwa.get("PB (do 2003)"),
                "BENZYNA", "do 2003");
        dcd.setValue(wszystkieDaneZWojewodztw_paliwa.get("ON (do 2003)"),
                "OLEJ NAPĘDOWY", "do 2003");
        dcd.setValue(wszystkieDaneZWojewodztw_paliwa.get("PB+LPG (do 2003)"),
                "BENZYNA + LPG", "do 2003");
        dcd.setValue(wszystkieDaneZWojewodztw_paliwa.get("PB (2004 - 2013)"),
                "BENZYNA", "2004 - 2013");
        dcd.setValue(wszystkieDaneZWojewodztw_paliwa.get("ON (2004 - 2013)"),
                "OLEJ NAPĘDOWY", "2004 - 2013");
        dcd.setValue(wszystkieDaneZWojewodztw_paliwa.get("PB+LPG (2004 - 2013)"),
                "BENZYNA + LPG", "2004 - 2013");
        dcd.setValue(wszystkieDaneZWojewodztw_paliwa.get("PB (od 2014)"),
                "BENZYNA", "od 2014");
        dcd.setValue(wszystkieDaneZWojewodztw_paliwa.get("ON (od 2014)"),
                "OLEJ NAPĘDOWY", "od 2014");
        dcd.setValue(wszystkieDaneZWojewodztw_paliwa.get("PB+LPG (od 2014)"),
                "BENZYNA + LPG", "od 2014");

        JFreeChart chart2 = ChartFactory.createBarChart(
                "Podział samochodów z poszczególnych roczników względem rodzaju paliwa" +
                        " w całym kraju (Polska) z uwzględnieniem danych bez przydziału województwa",
                "Rok produkcji",
                "Ilość pojazdów",
                dcd,
                PlotOrientation.VERTICAL,
                true,
                true,
                false
        );

        CategoryPlot plot2 = (CategoryPlot) chart2.getPlot();
        plot2.setForegroundAlpha(0.7f);
        plot2.getRenderer().setSeriesPaint(0, Color.decode("#006ff9"));
        plot2.getRenderer().setSeriesPaint(1, Color.ORANGE);
        plot2.getRenderer().setSeriesPaint(2, Color.DARK_GRAY);
        plot2.setRangeGridlinePaint(Color.BLACK);
        plot2.setBackgroundPaint(Color.WHITE);
        BarRenderer renderer = (BarRenderer) plot2.getRenderer();
        renderer.setItemMargin(.1);

        try {
            ChartUtilities.saveChartAsPNG(
                    new File("F:\\II stopień\\Semestr IV\\Praca magisterska\\Analiza danych\\vehicle_vintages_survey\\polska_paliwa.png"),
                    chart2,
                    500,
                    400
            );
        } catch (IOException e) {
            System.out.println("Wystapil problem podczas tworzenia wykresu");
        }

        JFrame frame = new JFrame("Popularność poszczególnych roczników pojazdów względem" +
                " rodzaju paliwa w całym kraju (Polska) z uwzględnieniem danych " +
                "bez przydziału województwa");
        frame.setLayout(new FlowLayout());
        frame.getContentPane().add(new ChartPanel(chart));
        frame.getContentPane().add(new ChartPanel(chart2));
        frame.pack();
        frame.setVisible(true);
    }
}
