package com.company;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.jfree.chart.ChartFactory;
import org.jfree.chart.ChartPanel;
import org.jfree.chart.ChartUtilities;
import org.jfree.chart.JFreeChart;
import org.jfree.chart.axis.AxisSpace;
import org.jfree.chart.labels.*;
import org.jfree.chart.plot.CategoryPlot;
import org.jfree.chart.plot.PiePlot;
import org.jfree.chart.plot.PlotOrientation;
import org.jfree.chart.renderer.category.BarRenderer;
import org.jfree.data.category.DefaultCategoryDataset;
import org.jfree.data.general.DefaultPieDataset;
import org.jfree.ui.TextAnchor;

import javax.swing.*;
import java.awt.*;
import java.io.File;
import java.io.IOException;
import java.text.DecimalFormat;

import static org.apache.spark.sql.functions.col;

public class EngineCapacityAndPowerPopularitySurvey {
    private CepikData cepikData;
    private Dataset<Row> samochody_osobowe;

    public EngineCapacityAndPowerPopularitySurvey(CepikData cepikData) {
        this.cepikData = cepikData;
        samochody_osobowe = cepikData.getWojewodztwo_dolnoslaskie()
                .filter(col("rodzaj").equalTo("SAMOCHÓD OSOBOWY")
                        .and(col("data_wyrejestrowania").isNull()));

        samochody_osobowe = samochody_osobowe.unionByName(cepikData.getWojewodztwo_kujawskopomorskie()
                .filter(col("rodzaj").equalTo("SAMOCHÓD OSOBOWY")
                        .and(col("data_wyrejestrowania").isNull())));

        samochody_osobowe = samochody_osobowe.unionByName(cepikData.getWojewodztwo_lubelskie()
                .filter(col("rodzaj").equalTo("SAMOCHÓD OSOBOWY")
                        .and(col("data_wyrejestrowania").isNull())));

        samochody_osobowe = samochody_osobowe.unionByName(cepikData.getWojewodztwo_lubuskie()
                .filter(col("rodzaj").equalTo("SAMOCHÓD OSOBOWY")
                        .and(col("data_wyrejestrowania").isNull())));

        samochody_osobowe = samochody_osobowe.unionByName(cepikData.getWojewodztwo_lodzkie()
                .filter(col("rodzaj").equalTo("SAMOCHÓD OSOBOWY")
                        .and(col("data_wyrejestrowania").isNull())));

        samochody_osobowe = samochody_osobowe.unionByName(cepikData.getWojewodztwo_malopolskie()
                .filter(col("rodzaj").equalTo("SAMOCHÓD OSOBOWY")
                        .and(col("data_wyrejestrowania").isNull())));

        samochody_osobowe = samochody_osobowe.unionByName(cepikData.getWojewodztwo_mazowieckie()
                .filter(col("rodzaj").equalTo("SAMOCHÓD OSOBOWY")
                        .and(col("data_wyrejestrowania").isNull())));

        samochody_osobowe = samochody_osobowe.unionByName(cepikData.getWojewodztwo_opolskie()
                .filter(col("rodzaj").equalTo("SAMOCHÓD OSOBOWY")
                        .and(col("data_wyrejestrowania").isNull())));

        samochody_osobowe = samochody_osobowe.unionByName(cepikData.getWojewodztwo_podkarpackie()
                .filter(col("rodzaj").equalTo("SAMOCHÓD OSOBOWY")
                        .and(col("data_wyrejestrowania").isNull())));

        samochody_osobowe = samochody_osobowe.unionByName(cepikData.getWojewodztwo_podlaskie()
                .filter(col("rodzaj").equalTo("SAMOCHÓD OSOBOWY")
                        .and(col("data_wyrejestrowania").isNull())));

        samochody_osobowe = samochody_osobowe.unionByName(cepikData.getWojewodztwo_pomorskie()
                .filter(col("rodzaj").equalTo("SAMOCHÓD OSOBOWY")
                        .and(col("data_wyrejestrowania").isNull())));

        samochody_osobowe = samochody_osobowe.unionByName(cepikData.getWojewodztwo_slaskie()
                .filter(col("rodzaj").equalTo("SAMOCHÓD OSOBOWY")
                        .and(col("data_wyrejestrowania").isNull())));

        samochody_osobowe = samochody_osobowe.unionByName(cepikData.getWojewodztwo_swietokrzyskie()
                .filter(col("rodzaj").equalTo("SAMOCHÓD OSOBOWY")
                        .and(col("data_wyrejestrowania").isNull())));

        samochody_osobowe = samochody_osobowe.unionByName(cepikData.getWojewodztwo_warminskomazurskie()
                .filter(col("rodzaj").equalTo("SAMOCHÓD OSOBOWY")
                        .and(col("data_wyrejestrowania").isNull())));

        samochody_osobowe = samochody_osobowe.unionByName(cepikData.getWojewodztwo_wielkopolskie()
                .filter(col("rodzaj").equalTo("SAMOCHÓD OSOBOWY")
                        .and(col("data_wyrejestrowania").isNull())));

        samochody_osobowe = samochody_osobowe.unionByName(cepikData.getWojewodztwo_zachodniopomorskie()
                .filter(col("rodzaj").equalTo("SAMOCHÓD OSOBOWY")
                        .and(col("data_wyrejestrowania").isNull())));

        samochody_osobowe = samochody_osobowe.unionByName(cepikData.getNieokreslone_wojewodztwo()
                .filter(col("rodzaj").equalTo("SAMOCHÓD OSOBOWY")
                        .and(col("data_wyrejestrowania").isNull())));
    }

    // popularnosc samochodow osobowych pod katem pojemnosci silnika w calym kraju (Polska)
    public void popularity_of_engine_capacity_in_poland() {
        Dataset<Row> engine_capacity = samochody_osobowe
                .filter(col("pojemnosc_silnika").isNotNull());

        Dataset<Row> ec_leq1700 = engine_capacity
                .filter(col("pojemnosc_silnika").gt(0.0)
                        .and(col("pojemnosc_silnika").leq(1700.0)));

        Dataset<Row> ec_gt1700leq2000 = samochody_osobowe
                .filter(col("pojemnosc_silnika").gt(1700.0)
                        .and(col("pojemnosc_silnika").leq(2000.0)));

        Dataset<Row> ec_gt2000leq3000 = samochody_osobowe
                .filter(col("pojemnosc_silnika").gt(2000.0)
                        .and(col("pojemnosc_silnika").leq(3000.0)));

        Dataset<Row> ec_gt3000leq4000 = samochody_osobowe
                .filter(col("pojemnosc_silnika").gt(3000.0)
                        .and(col("pojemnosc_silnika").leq(4000.0)));

        Dataset<Row> ec_gt4000 = samochody_osobowe
                .filter(col("pojemnosc_silnika").gt(4000.0));

        long ilosc_leq1700 = ec_leq1700.count();
        long ilosc_gt1700leq2000 = ec_gt1700leq2000.count();
        long ilosc_gt2000leq3000 = ec_gt2000leq3000.count();
        long ilosc_gt3000leq4000 = ec_gt3000leq4000.count();
        long ilosc_gt4000 = ec_gt4000.count();

        /*System.out.println("Ilosc aut  w Polsce z poszczegolna pojemnoscia silnika: ");
        System.out.println("do 110 KM: " + ilosc_leq81);
        System.out.println("111 - 210 KM: " + ilosc_gt81leq155);
        System.out.println("211 - 310 KM: " + ilosc_gt155leq228);
        System.out.println("311 - 410 KM: " + ilosc_gt228leq302);
        System.out.println("od 411 KM: " + ilosc_gt302);*/

        // Tworzenie wykresu - Porównanie popularności aut pod kątem pojemnosci silnika - cała Polska
        DefaultCategoryDataset dcd = new DefaultCategoryDataset();

        dcd.setValue(ilosc_leq1700, "pojemnosc", "do 1700 cm3");
        dcd.setValue(ilosc_gt1700leq2000, "pojemnosc", "1701 - 2000 cm3");
        dcd.setValue(ilosc_gt2000leq3000, "pojemnosc", "2001 - 3000 cm3");
        dcd.setValue(ilosc_gt3000leq4000, "pojemnosc", "3001 - 4000 cm3");
        dcd.setValue(ilosc_gt4000, "pojemnosc", "od 4001 cm3");

        JFreeChart chart = ChartFactory.createBarChart(
                "Popularność samochodów osobowych pod kątem pojemności silnika - cała Polska",
                "Pojemność silnika",
                "Ilość pojazdów",
                dcd,
                PlotOrientation.VERTICAL,
                false,
                true,
                false
        );

        /*CategoryPlot plot = (CategoryPlot) chart.getPlot();
        plot.setForegroundAlpha(0.7f);
        plot.getRenderer().setSeriesPaint(0, Color.decode("#006ff9"));
        plot.setRangeGridlinePaint(Color.BLACK);
        plot.setBackgroundPaint(Color.WHITE);*/

        CategoryPlot plot = (CategoryPlot) chart.getPlot();
        plot.setForegroundAlpha(0.7f);
        BarRenderer renderer = (BarRenderer) plot.getRenderer();
        renderer.setBaseItemLabelGenerator(new StandardCategoryItemLabelGenerator());
        renderer.setBaseItemLabelsVisible(true);
        renderer.setBaseItemLabelFont(new Font("Arial", Font.BOLD, 12));
        plot.getRenderer().setSeriesPaint(0, Color.decode("#006ff9"));
        plot.setRangeGridlinePaint(Color.BLACK);
        plot.setBackgroundPaint(Color.WHITE);

        try {
            ChartUtilities.saveChartAsPNG(
                    new File("F:\\II stopień\\Semestr IV\\Praca magisterska\\Analiza danych\\car_engine_popularity_survey\\engine_capacity_poland.png"),
                    chart,
                    750,
                    400
            );
        } catch (IOException e) {
            System.out.println("Wystapil problem podczas tworzenia wykresu");
        }

        JFrame frame = new JFrame("Popularność samochodów osobowych pod kątem pojemności silnika" +
                " - cała Polska");
        frame.setSize(600, 400);
        frame.setLayout(new FlowLayout());
        frame.getContentPane().add(new ChartPanel(chart));
        frame.pack();
        frame.setVisible(true);
    }

    // popularnosc samochodow osobowych pod katem mocy silnika w calym kraju (Polska)
    public void popularity_of_engine_power_in_poland() {
        Dataset<Row> engine_power = samochody_osobowe
                .filter(col("moc_silnika").isNotNull());

        Dataset<Row> ep_leq81 = engine_power
                .filter(col("moc_silnika").gt(0.0)
                        .and(col("moc_silnika").leq(81.0)));

        Dataset<Row> ep_gt81leq155 = samochody_osobowe
                .filter(col("moc_silnika").gt(81.0)
                        .and(col("moc_silnika").leq(155.0)));

        Dataset<Row> ep_gt155leq228 = samochody_osobowe
                .filter(col("moc_silnika").gt(155.0)
                        .and(col("moc_silnika").leq(228.0)));

        Dataset<Row> ep_gt228leq302 = samochody_osobowe
                .filter(col("moc_silnika").gt(227.0)
                        .and(col("moc_silnika").leq(302.0)));

        Dataset<Row> ep_gt302 = samochody_osobowe
                .filter(col("moc_silnika").gt(302.0));

        long ilosc_leq81 = ep_leq81.count();
        long ilosc_gt81leq155 = ep_gt81leq155.count();
        long ilosc_gt155leq228 = ep_gt155leq228.count();
        long ilosc_gt228leq302 = ep_gt228leq302.count();
        long ilosc_gt302 = ep_gt302.count();

        /*System.out.println("Ilosc aut  w Polsce z poszczegolna pojemnoscia silnika: ");
        System.out.println("do 110 KM: " + ilosc_leq81);
        System.out.println("111 - 210 KM: " + ilosc_gt81leq155);
        System.out.println("211 - 310 KM: " + ilosc_gt155leq228);
        System.out.println("311 - 410 KM: " + ilosc_gt228leq302);
        System.out.println("od 411 KM: " + ilosc_gt302);*/

        // Tworzenie wykresu - Porównanie popularności aut pod kątem mocy silnika - cała Polska
        DefaultCategoryDataset dcd = new DefaultCategoryDataset();

        dcd.setValue(ilosc_leq81, "moc", "do 110 KM");
        dcd.setValue(ilosc_gt81leq155, "moc", "111 - 210 KM");
        dcd.setValue(ilosc_gt155leq228, "moc", "211 - 310 KM");
        dcd.setValue(ilosc_gt228leq302, "moc", "311 - 410 KM");
        dcd.setValue(ilosc_gt302, "moc", "od 411 KM");

        JFreeChart chart = ChartFactory.createBarChart(
                "Popularność samochodów osobowych pod kątem mocy silnika - cała Polska",
                "Moc silnika",
                "Ilość pojazdów",
                dcd,
                PlotOrientation.VERTICAL,
                false,
                true,
                false
        );

        /*CategoryPlot plot = (CategoryPlot) chart.getPlot();
        plot.setForegroundAlpha(0.7f);
        plot.getRenderer().setSeriesPaint(0, Color.decode("#006ff9"));
        plot.setRangeGridlinePaint(Color.BLACK);
        plot.setBackgroundPaint(Color.WHITE); */

        CategoryPlot plot = (CategoryPlot) chart.getPlot();
        plot.setForegroundAlpha(0.7f);
        BarRenderer renderer = (BarRenderer) plot.getRenderer();
        renderer.setBaseItemLabelGenerator(new StandardCategoryItemLabelGenerator());
        renderer.setBaseItemLabelsVisible(true);
        renderer.setBaseItemLabelFont(new Font("Arial", Font.BOLD, 12));
        plot.getRenderer().setSeriesPaint(0, Color.decode("#006ff9"));
        plot.setRangeGridlinePaint(Color.BLACK);
        plot.setBackgroundPaint(Color.WHITE);

        try {
            ChartUtilities.saveChartAsPNG(
                    new File("F:\\II stopień\\Semestr IV\\Praca magisterska\\Analiza danych\\car_engine_popularity_survey\\engine_power_poland.png"),
                    chart,
                    600,
                    400
            );
        } catch (IOException e) {
            System.out.println("Wystapil problem podczas tworzenia wykresu");
        }

        JFrame frame = new JFrame("Popularność samochodów osobowych pod kątem mocy silnika" +
                " - cała Polska");
        frame.setSize(600, 400);
        frame.setLayout(new FlowLayout());
        frame.getContentPane().add(new ChartPanel(chart));
        frame.pack();
        frame.setVisible(true);
    }
}
