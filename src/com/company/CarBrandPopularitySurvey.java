package com.company;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.jfree.chart.ChartFactory;
import org.jfree.chart.ChartPanel;
import org.jfree.chart.ChartUtilities;
import org.jfree.chart.JFreeChart;
import org.jfree.chart.labels.ItemLabelAnchor;
import org.jfree.chart.labels.ItemLabelPosition;
import org.jfree.chart.labels.StandardCategoryItemLabelGenerator;
import org.jfree.chart.plot.CategoryPlot;
import org.jfree.chart.plot.PlotOrientation;
import org.jfree.chart.renderer.category.BarRenderer;
import org.jfree.data.category.DefaultCategoryDataset;
import org.jfree.ui.TextAnchor;

import javax.swing.*;
import java.awt.*;
import java.io.File;
import java.io.IOException;
import java.util.*;
import java.util.List;

import static org.apache.spark.sql.functions.col;

public class CarBrandPopularitySurvey {
    private CepikData cepikData;
    private Dataset<Row> samochody_osobowe;

    public CarBrandPopularitySurvey(CepikData cepikData) {
        this.cepikData = cepikData;
        this.samochody_osobowe = null;
    }

    public void popularity_of_car_brand_woj_lubelskie() {
        // Obliczanie 8 najpopularniejszych marek w wojewodztwie
        Dataset<Row> samochody_osobowe_marka = cepikData.getWojewodztwo_lubelskie()
                .filter(col("rodzaj").equalTo("SAMOCHÓD OSOBOWY")
                        .and(col("data_wyrejestrowania").isNull()))
                .filter(col("marka").isNotNull());

        samochody_osobowe = samochody_osobowe_marka;

        List<Row> marki = samochody_osobowe_marka.groupBy("marka").count()
                .sort(col("count").desc()).limit(8).collectAsList();

        // Tworzenie wykresu
        DefaultCategoryDataset dcd = new DefaultCategoryDataset();

        for(Row row : marki) {
            dcd.setValue((long)row.get(1), "marka", (String)row.get(0));
        }

        JFreeChart chart = ChartFactory.createBarChart(
                "Osiem najpopularniejszych marek samochodów osobowych w województwie lubelskim",
                "Marka pojazdu",
                "Ilość pojazdów",
                dcd,
                PlotOrientation.VERTICAL,
                false,
                true,
                false
        );

        CategoryPlot plot = (CategoryPlot) chart.getPlot();
        plot.setForegroundAlpha(0.7f);
        BarRenderer renderer = (BarRenderer) plot.getRenderer();
        renderer.setBaseItemLabelGenerator(new StandardCategoryItemLabelGenerator());
        renderer.setBaseItemLabelsVisible(true);
        renderer.setBaseItemLabelFont(new Font("Arial", Font.BOLD, 16));
        ItemLabelPosition position = new ItemLabelPosition(ItemLabelAnchor.OUTSIDE12,
                TextAnchor.TOP_CENTER);
        renderer.setBasePositiveItemLabelPosition(position);
        plot.getRenderer().setSeriesPaint(0, Color.decode("#006ff9"));
        plot.setRangeGridlinePaint(Color.BLACK);
        plot.setBackgroundPaint(Color.WHITE);

        try {
            ChartUtilities.saveChartAsPNG(
                    new File("F:\\II stopień\\Semestr IV\\Praca magisterska\\Analiza danych\\car_brand_survey\\lubelskie.png"),
                    chart,
                    1400,
                    600
            );
        } catch (IOException e) {
            System.out.println("Wystapil problem podczas tworzenia wykresu");
        }

        JFrame frame = new JFrame("Popularność marek samochodów osobowych" +
                " - województwo lubelskie");
        frame.setSize(1400, 600);
        frame.setLayout(new FlowLayout());
        frame.getContentPane().add(new ChartPanel(chart));
        frame.pack();
        frame.setVisible(true);
    }

    public void popularity_of_car_brand_woj_dolnoslaskie() {
        // Obliczanie 8 najpopularniejszych marek w wojewodztwie
        Dataset<Row> samochody_osobowe_marka = cepikData.getWojewodztwo_dolnoslaskie()
                .filter(col("rodzaj").equalTo("SAMOCHÓD OSOBOWY")
                        .and(col("data_wyrejestrowania").isNull()))
                .filter(col("marka").isNotNull());

        samochody_osobowe = samochody_osobowe.unionByName(samochody_osobowe_marka);

        List<Row> marki = samochody_osobowe_marka.groupBy("marka").count()
                .sort(col("count").desc()).limit(8).collectAsList();

        // Tworzenie wykresu
        DefaultCategoryDataset dcd = new DefaultCategoryDataset();

        for(Row row : marki) {
            dcd.setValue((long)row.get(1), "marka", (String)row.get(0));
        }

        JFreeChart chart = ChartFactory.createBarChart(
                "Osiem najpopularniejszych marek samochodów osobowych w województwie dolnośląskim",
                "Marka pojazdu",
                "Ilość pojazdów",
                dcd,
                PlotOrientation.VERTICAL,
                false,
                true,
                false
        );

        CategoryPlot plot = (CategoryPlot) chart.getPlot();
        plot.setForegroundAlpha(0.7f);
        BarRenderer renderer = (BarRenderer) plot.getRenderer();
        renderer.setBaseItemLabelGenerator(new StandardCategoryItemLabelGenerator());
        renderer.setBaseItemLabelsVisible(true);
        renderer.setBaseItemLabelFont(new Font("Arial", Font.BOLD, 16));
        ItemLabelPosition position = new ItemLabelPosition(ItemLabelAnchor.OUTSIDE12,
                TextAnchor.TOP_CENTER);
        renderer.setBasePositiveItemLabelPosition(position);
        plot.getRenderer().setSeriesPaint(0, Color.decode("#006ff9"));
        plot.setRangeGridlinePaint(Color.BLACK);
        plot.setBackgroundPaint(Color.WHITE);

        try {
            ChartUtilities.saveChartAsPNG(
                    new File("F:\\II stopień\\Semestr IV\\Praca magisterska\\Analiza danych\\car_brand_survey\\dolnoslaskie.png"),
                    chart,
                    1400,
                    600
            );
        } catch (IOException e) {
            System.out.println("Wystapil problem podczas tworzenia wykresu");
        }

        JFrame frame = new JFrame("Popularność marek samochodów osobowych" +
                " - województwo dolnośląskie");
        frame.setSize(1400, 600);
        frame.setLayout(new FlowLayout());
        frame.getContentPane().add(new ChartPanel(chart));
        frame.pack();
        frame.setVisible(true);
    }

    public void popularity_of_car_brand_woj_kujawskopomorskie() {
        // Obliczanie 8 najpopularniejszych marek w wojewodztwie
        Dataset<Row> samochody_osobowe_marka = cepikData.getWojewodztwo_kujawskopomorskie()
                .filter(col("rodzaj").equalTo("SAMOCHÓD OSOBOWY")
                        .and(col("data_wyrejestrowania").isNull()))
                .filter(col("marka").isNotNull());

        samochody_osobowe = samochody_osobowe.unionByName(samochody_osobowe_marka);

        List<Row> marki = samochody_osobowe_marka.groupBy("marka").count()
                .sort(col("count").desc()).limit(8).collectAsList();

        // Tworzenie wykresu
        DefaultCategoryDataset dcd = new DefaultCategoryDataset();

        for(Row row : marki) {
            dcd.setValue((long)row.get(1), "marka", (String)row.get(0));
        }

        JFreeChart chart = ChartFactory.createBarChart(
                "Osiem najpopularniejszych marek samochodów osobowych w województwie kujawsko-pomorskim",
                "Marka pojazdu",
                "Ilość pojazdów",
                dcd,
                PlotOrientation.VERTICAL,
                false,
                true,
                false
        );

        CategoryPlot plot = (CategoryPlot) chart.getPlot();
        plot.setForegroundAlpha(0.7f);
        BarRenderer renderer = (BarRenderer) plot.getRenderer();
        renderer.setBaseItemLabelGenerator(new StandardCategoryItemLabelGenerator());
        renderer.setBaseItemLabelsVisible(true);
        renderer.setBaseItemLabelFont(new Font("Arial", Font.BOLD, 16));
        ItemLabelPosition position = new ItemLabelPosition(ItemLabelAnchor.OUTSIDE12,
                TextAnchor.TOP_CENTER);
        renderer.setBasePositiveItemLabelPosition(position);
        plot.getRenderer().setSeriesPaint(0, Color.decode("#006ff9"));
        plot.setRangeGridlinePaint(Color.BLACK);
        plot.setBackgroundPaint(Color.WHITE);

        try {
            ChartUtilities.saveChartAsPNG(
                    new File("F:\\II stopień\\Semestr IV\\Praca magisterska\\Analiza danych\\car_brand_survey\\kujawskopomorskie.png"),
                    chart,
                    1400,
                    600
            );
        } catch (IOException e) {
            System.out.println("Wystapil problem podczas tworzenia wykresu");
        }

        JFrame frame = new JFrame("Popularność marek samochodów osobowych" +
                " - województwo kujawsko-pomorskie");
        frame.setSize(1400, 600);
        frame.setLayout(new FlowLayout());
        frame.getContentPane().add(new ChartPanel(chart));
        frame.pack();
        frame.setVisible(true);
    }

    public void popularity_of_car_brand_woj_lubuskie() {
        // Obliczanie 8 najpopularniejszych marek w wojewodztwie
        Dataset<Row> samochody_osobowe_marka = cepikData.getWojewodztwo_lubuskie()
                .filter(col("rodzaj").equalTo("SAMOCHÓD OSOBOWY")
                        .and(col("data_wyrejestrowania").isNull()))
                .filter(col("marka").isNotNull());

        samochody_osobowe = samochody_osobowe.unionByName(samochody_osobowe_marka);

        List<Row> marki = samochody_osobowe_marka.groupBy("marka").count()
                .sort(col("count").desc()).limit(8).collectAsList();

        // Tworzenie wykresu
        DefaultCategoryDataset dcd = new DefaultCategoryDataset();

        for(Row row : marki) {
            dcd.setValue((long)row.get(1), "marka", (String)row.get(0));
        }

        JFreeChart chart = ChartFactory.createBarChart(
                "Osiem najpopularniejszych marek samochodów osobowych w województwie lubuskim",
                "Marka pojazdu",
                "Ilość pojazdów",
                dcd,
                PlotOrientation.VERTICAL,
                false,
                true,
                false
        );

        CategoryPlot plot = (CategoryPlot) chart.getPlot();
        plot.setForegroundAlpha(0.7f);
        BarRenderer renderer = (BarRenderer) plot.getRenderer();
        renderer.setBaseItemLabelGenerator(new StandardCategoryItemLabelGenerator());
        renderer.setBaseItemLabelsVisible(true);
        renderer.setBaseItemLabelFont(new Font("Arial", Font.BOLD, 16));
        ItemLabelPosition position = new ItemLabelPosition(ItemLabelAnchor.OUTSIDE12,
                TextAnchor.TOP_CENTER);
        renderer.setBasePositiveItemLabelPosition(position);
        plot.getRenderer().setSeriesPaint(0, Color.decode("#006ff9"));
        plot.setRangeGridlinePaint(Color.BLACK);
        plot.setBackgroundPaint(Color.WHITE);

        try {
            ChartUtilities.saveChartAsPNG(
                    new File("F:\\II stopień\\Semestr IV\\Praca magisterska\\Analiza danych\\car_brand_survey\\lubuskie.png"),
                    chart,
                    1400,
                    600
            );
        } catch (IOException e) {
            System.out.println("Wystapil problem podczas tworzenia wykresu");
        }

        JFrame frame = new JFrame("Popularność marek samochodów osobowych" +
                " - województwo lubuskie");
        frame.setSize(1400, 600);
        frame.setLayout(new FlowLayout());
        frame.getContentPane().add(new ChartPanel(chart));
        frame.pack();
        frame.setVisible(true);
    }

    public void popularity_of_car_brand_woj_lodzkie() {
        // Obliczanie 8 najpopularniejszych marek w wojewodztwie
        Dataset<Row> samochody_osobowe_marka = cepikData.getWojewodztwo_lodzkie()
                .filter(col("rodzaj").equalTo("SAMOCHÓD OSOBOWY")
                        .and(col("data_wyrejestrowania").isNull()))
                .filter(col("marka").isNotNull());

        samochody_osobowe = samochody_osobowe.unionByName(samochody_osobowe_marka);

        List<Row> marki = samochody_osobowe_marka.groupBy("marka").count()
                .sort(col("count").desc()).limit(8).collectAsList();

        // Tworzenie wykresu
        DefaultCategoryDataset dcd = new DefaultCategoryDataset();

        for(Row row : marki) {
            dcd.setValue((long)row.get(1), "marka", (String)row.get(0));
        }

        JFreeChart chart = ChartFactory.createBarChart(
                "Osiem najpopularniejszych marek samochodów osobowych w województwie łódzkim",
                "Marka pojazdu",
                "Ilość pojazdów",
                dcd,
                PlotOrientation.VERTICAL,
                false,
                true,
                false
        );

        CategoryPlot plot = (CategoryPlot) chart.getPlot();
        plot.setForegroundAlpha(0.7f);
        BarRenderer renderer = (BarRenderer) plot.getRenderer();
        renderer.setBaseItemLabelGenerator(new StandardCategoryItemLabelGenerator());
        renderer.setBaseItemLabelsVisible(true);
        renderer.setBaseItemLabelFont(new Font("Arial", Font.BOLD, 16));
        ItemLabelPosition position = new ItemLabelPosition(ItemLabelAnchor.OUTSIDE12,
                TextAnchor.TOP_CENTER);
        renderer.setBasePositiveItemLabelPosition(position);
        plot.getRenderer().setSeriesPaint(0, Color.decode("#006ff9"));
        plot.setRangeGridlinePaint(Color.BLACK);
        plot.setBackgroundPaint(Color.WHITE);

        try {
            ChartUtilities.saveChartAsPNG(
                    new File("F:\\II stopień\\Semestr IV\\Praca magisterska\\Analiza danych\\car_brand_survey\\lodzkie.png"),
                    chart,
                    1400,
                    600
            );
        } catch (IOException e) {
            System.out.println("Wystapil problem podczas tworzenia wykresu");
        }

        JFrame frame = new JFrame("Popularność marek samochodów osobowych" +
                " - województwo łódzkie");
        frame.setSize(1400, 600);
        frame.setLayout(new FlowLayout());
        frame.getContentPane().add(new ChartPanel(chart));
        frame.pack();
        frame.setVisible(true);
    }

    public void popularity_of_car_brand_woj_malopolskie() {
        // Obliczanie 8 najpopularniejszych marek w wojewodztwie
        Dataset<Row> samochody_osobowe_marka = cepikData.getWojewodztwo_malopolskie()
                .filter(col("rodzaj").equalTo("SAMOCHÓD OSOBOWY")
                        .and(col("data_wyrejestrowania").isNull()))
                .filter(col("marka").isNotNull());

        samochody_osobowe = samochody_osobowe.unionByName(samochody_osobowe_marka);

        List<Row> marki = samochody_osobowe_marka.groupBy("marka").count()
                .sort(col("count").desc()).limit(8).collectAsList();

        // Tworzenie wykresu
        DefaultCategoryDataset dcd = new DefaultCategoryDataset();

        for(Row row : marki) {
            dcd.setValue((long)row.get(1), "marka", (String)row.get(0));
        }

        JFreeChart chart = ChartFactory.createBarChart(
                "Osiem najpopularniejszych marek samochodów osobowych w województwie małopolskim",
                "Marka pojazdu",
                "Ilość pojazdów",
                dcd,
                PlotOrientation.VERTICAL,
                false,
                true,
                false
        );

        CategoryPlot plot = (CategoryPlot) chart.getPlot();
        plot.setForegroundAlpha(0.7f);
        BarRenderer renderer = (BarRenderer) plot.getRenderer();
        renderer.setBaseItemLabelGenerator(new StandardCategoryItemLabelGenerator());
        renderer.setBaseItemLabelsVisible(true);
        renderer.setBaseItemLabelFont(new Font("Arial", Font.BOLD, 16));
        ItemLabelPosition position = new ItemLabelPosition(ItemLabelAnchor.OUTSIDE12,
                TextAnchor.TOP_CENTER);
        renderer.setBasePositiveItemLabelPosition(position);
        plot.getRenderer().setSeriesPaint(0, Color.decode("#006ff9"));
        plot.setRangeGridlinePaint(Color.BLACK);
        plot.setBackgroundPaint(Color.WHITE);

        try {
            ChartUtilities.saveChartAsPNG(
                    new File("F:\\II stopień\\Semestr IV\\Praca magisterska\\Analiza danych\\car_brand_survey\\malopolskie.png"),
                    chart,
                    1400,
                    600
            );
        } catch (IOException e) {
            System.out.println("Wystapil problem podczas tworzenia wykresu");
        }

        JFrame frame = new JFrame("Popularność marek samochodów osobowych" +
                " - województwo małopolskie");
        frame.setSize(1400, 600);
        frame.setLayout(new FlowLayout());
        frame.getContentPane().add(new ChartPanel(chart));
        frame.pack();
        frame.setVisible(true);
    }

    public void popularity_of_car_brand_woj_mazowieckie() {
        // Obliczanie 8 najpopularniejszych marek w wojewodztwie
        Dataset<Row> samochody_osobowe_marka = cepikData.getWojewodztwo_mazowieckie()
                .filter(col("rodzaj").equalTo("SAMOCHÓD OSOBOWY")
                        .and(col("data_wyrejestrowania").isNull()))
                .filter(col("marka").isNotNull());

        samochody_osobowe = samochody_osobowe.unionByName(samochody_osobowe_marka);

        List<Row> marki = samochody_osobowe_marka.groupBy("marka").count()
                .sort(col("count").desc()).limit(8).collectAsList();

        // Tworzenie wykresu
        DefaultCategoryDataset dcd = new DefaultCategoryDataset();

        for(Row row : marki) {
            dcd.setValue((long)row.get(1), "marka", (String)row.get(0));
        }

        JFreeChart chart = ChartFactory.createBarChart(
                "Osiem najpopularniejszych marek samochodów osobowych w województwie mazowieckim",
                "Marka pojazdu",
                "Ilość pojazdów",
                dcd,
                PlotOrientation.VERTICAL,
                false,
                true,
                false
        );

        CategoryPlot plot = (CategoryPlot) chart.getPlot();
        plot.setForegroundAlpha(0.7f);
        BarRenderer renderer = (BarRenderer) plot.getRenderer();
        renderer.setBaseItemLabelGenerator(new StandardCategoryItemLabelGenerator());
        renderer.setBaseItemLabelsVisible(true);
        renderer.setBaseItemLabelFont(new Font("Arial", Font.BOLD, 16));
        ItemLabelPosition position = new ItemLabelPosition(ItemLabelAnchor.OUTSIDE12,
                TextAnchor.TOP_CENTER);
        renderer.setBasePositiveItemLabelPosition(position);
        plot.getRenderer().setSeriesPaint(0, Color.decode("#006ff9"));
        plot.setRangeGridlinePaint(Color.BLACK);
        plot.setBackgroundPaint(Color.WHITE);

        try {
            ChartUtilities.saveChartAsPNG(
                    new File("F:\\II stopień\\Semestr IV\\Praca magisterska\\Analiza danych\\car_brand_survey\\mazowieckie.png"),
                    chart,
                    1400,
                    600
            );
        } catch (IOException e) {
            System.out.println("Wystapil problem podczas tworzenia wykresu");
        }

        JFrame frame = new JFrame("Popularność marek samochodów osobowych" +
                " - województwo mazowieckie");
        frame.setSize(1400, 600);
        frame.setLayout(new FlowLayout());
        frame.getContentPane().add(new ChartPanel(chart));
        frame.pack();
        frame.setVisible(true);
    }

    public void popularity_of_car_brand_woj_opolskie() {
        // Obliczanie 8 najpopularniejszych marek w wojewodztwie
        Dataset<Row> samochody_osobowe_marka = cepikData.getWojewodztwo_opolskie()
                .filter(col("rodzaj").equalTo("SAMOCHÓD OSOBOWY")
                        .and(col("data_wyrejestrowania").isNull()))
                .filter(col("marka").isNotNull());

        samochody_osobowe = samochody_osobowe.unionByName(samochody_osobowe_marka);

        List<Row> marki = samochody_osobowe_marka.groupBy("marka").count()
                .sort(col("count").desc()).limit(8).collectAsList();

        // Tworzenie wykresu
        DefaultCategoryDataset dcd = new DefaultCategoryDataset();

        for(Row row : marki) {
            dcd.setValue((long)row.get(1), "marka", (String)row.get(0));
        }

        JFreeChart chart = ChartFactory.createBarChart(
                "Osiem najpopularniejszych marek samochodów osobowych w województwie opolskim",
                "Marka pojazdu",
                "Ilość pojazdów",
                dcd,
                PlotOrientation.VERTICAL,
                false,
                true,
                false
        );

        CategoryPlot plot = (CategoryPlot) chart.getPlot();
        plot.setForegroundAlpha(0.7f);
        BarRenderer renderer = (BarRenderer) plot.getRenderer();
        renderer.setBaseItemLabelGenerator(new StandardCategoryItemLabelGenerator());
        renderer.setBaseItemLabelsVisible(true);
        renderer.setBaseItemLabelFont(new Font("Arial", Font.BOLD, 16));
        ItemLabelPosition position = new ItemLabelPosition(ItemLabelAnchor.OUTSIDE12,
                TextAnchor.TOP_CENTER);
        renderer.setBasePositiveItemLabelPosition(position);
        plot.getRenderer().setSeriesPaint(0, Color.decode("#006ff9"));
        plot.setRangeGridlinePaint(Color.BLACK);
        plot.setBackgroundPaint(Color.WHITE);

        try {
            ChartUtilities.saveChartAsPNG(
                    new File("F:\\II stopień\\Semestr IV\\Praca magisterska\\Analiza danych\\car_brand_survey\\opolskie.png"),
                    chart,
                    1400,
                    600
            );
        } catch (IOException e) {
            System.out.println("Wystapil problem podczas tworzenia wykresu");
        }

        JFrame frame = new JFrame("Popularność marek samochodów osobowych" +
                " - województwo opolskie");
        frame.setSize(1400, 600);
        frame.setLayout(new FlowLayout());
        frame.getContentPane().add(new ChartPanel(chart));
        frame.pack();
        frame.setVisible(true);
    }

    public void popularity_of_car_brand_woj_podkarpackie() {
        // Obliczanie 8 najpopularniejszych marek w wojewodztwie
        Dataset<Row> samochody_osobowe_marka = cepikData.getWojewodztwo_podkarpackie()
                .filter(col("rodzaj").equalTo("SAMOCHÓD OSOBOWY")
                        .and(col("data_wyrejestrowania").isNull()))
                .filter(col("marka").isNotNull());

        samochody_osobowe = samochody_osobowe.unionByName(samochody_osobowe_marka);

        List<Row> marki = samochody_osobowe_marka.groupBy("marka").count()
                .sort(col("count").desc()).limit(8).collectAsList();

        // Tworzenie wykresu
        DefaultCategoryDataset dcd = new DefaultCategoryDataset();

        for(Row row : marki) {
            dcd.setValue((long)row.get(1), "marka", (String)row.get(0));
        }

        JFreeChart chart = ChartFactory.createBarChart(
                "Osiem najpopularniejszych marek samochodów osobowych w województwie podkarpackim",
                "Marka pojazdu",
                "Ilość pojazdów",
                dcd,
                PlotOrientation.VERTICAL,
                false,
                true,
                false
        );

        CategoryPlot plot = (CategoryPlot) chart.getPlot();
        plot.setForegroundAlpha(0.7f);
        BarRenderer renderer = (BarRenderer) plot.getRenderer();
        renderer.setBaseItemLabelGenerator(new StandardCategoryItemLabelGenerator());
        renderer.setBaseItemLabelsVisible(true);
        renderer.setBaseItemLabelFont(new Font("Arial", Font.BOLD, 16));
        ItemLabelPosition position = new ItemLabelPosition(ItemLabelAnchor.OUTSIDE12,
                TextAnchor.TOP_CENTER);
        renderer.setBasePositiveItemLabelPosition(position);
        plot.getRenderer().setSeriesPaint(0, Color.decode("#006ff9"));
        plot.setRangeGridlinePaint(Color.BLACK);
        plot.setBackgroundPaint(Color.WHITE);

        try {
            ChartUtilities.saveChartAsPNG(
                    new File("F:\\II stopień\\Semestr IV\\Praca magisterska\\Analiza danych\\car_brand_survey\\podkarpackie.png"),
                    chart,
                    1400,
                    600
            );
        } catch (IOException e) {
            System.out.println("Wystapil problem podczas tworzenia wykresu");
        }

        JFrame frame = new JFrame("Popularność marek samochodów osobowych" +
                " - województwo podkarpackie");
        frame.setSize(1400, 600);
        frame.setLayout(new FlowLayout());
        frame.getContentPane().add(new ChartPanel(chart));
        frame.pack();
        frame.setVisible(true);
    }

    public void popularity_of_car_brand_woj_podlaskie() {
        // Obliczanie 8 najpopularniejszych marek w wojewodztwie
        Dataset<Row> samochody_osobowe_marka = cepikData.getWojewodztwo_podlaskie()
                .filter(col("rodzaj").equalTo("SAMOCHÓD OSOBOWY")
                        .and(col("data_wyrejestrowania").isNull()))
                .filter(col("marka").isNotNull());

        samochody_osobowe = samochody_osobowe.unionByName(samochody_osobowe_marka);

        List<Row> marki = samochody_osobowe_marka.groupBy("marka").count()
                .sort(col("count").desc()).limit(8).collectAsList();

        // Tworzenie wykresu
        DefaultCategoryDataset dcd = new DefaultCategoryDataset();

        for(Row row : marki) {
            dcd.setValue((long)row.get(1), "marka", (String)row.get(0));
        }

        JFreeChart chart = ChartFactory.createBarChart(
                "Osiem najpopularniejszych marek samochodów osobowych w województwie podlaskim",
                "Marka pojazdu",
                "Ilość pojazdów",
                dcd,
                PlotOrientation.VERTICAL,
                false,
                true,
                false
        );

        CategoryPlot plot = (CategoryPlot) chart.getPlot();
        plot.setForegroundAlpha(0.7f);
        BarRenderer renderer = (BarRenderer) plot.getRenderer();
        renderer.setBaseItemLabelGenerator(new StandardCategoryItemLabelGenerator());
        renderer.setBaseItemLabelsVisible(true);
        renderer.setBaseItemLabelFont(new Font("Arial", Font.BOLD, 16));
        ItemLabelPosition position = new ItemLabelPosition(ItemLabelAnchor.OUTSIDE12,
                TextAnchor.TOP_CENTER);
        renderer.setBasePositiveItemLabelPosition(position);
        plot.getRenderer().setSeriesPaint(0, Color.decode("#006ff9"));
        plot.setRangeGridlinePaint(Color.BLACK);
        plot.setBackgroundPaint(Color.WHITE);

        try {
            ChartUtilities.saveChartAsPNG(
                    new File("F:\\II stopień\\Semestr IV\\Praca magisterska\\Analiza danych\\car_brand_survey\\podlaskie.png"),
                    chart,
                    1400,
                    600
            );
        } catch (IOException e) {
            System.out.println("Wystapil problem podczas tworzenia wykresu");
        }

        JFrame frame = new JFrame("Popularność marek samochodów osobowych" +
                " - województwo podlaskie");
        frame.setSize(1400, 600);
        frame.setLayout(new FlowLayout());
        frame.getContentPane().add(new ChartPanel(chart));
        frame.pack();
        frame.setVisible(true);
    }

    public void popularity_of_car_brand_woj_pomorskie() {
        // Obliczanie 8 najpopularniejszych marek w wojewodztwie
        Dataset<Row> samochody_osobowe_marka = cepikData.getWojewodztwo_pomorskie()
                .filter(col("rodzaj").equalTo("SAMOCHÓD OSOBOWY")
                        .and(col("data_wyrejestrowania").isNull()))
                .filter(col("marka").isNotNull());

        samochody_osobowe = samochody_osobowe.unionByName(samochody_osobowe_marka);

        List<Row> marki = samochody_osobowe_marka.groupBy("marka").count()
                .sort(col("count").desc()).limit(8).collectAsList();

        // Tworzenie wykresu
        DefaultCategoryDataset dcd = new DefaultCategoryDataset();

        for(Row row : marki) {
            dcd.setValue((long)row.get(1), "marka", (String)row.get(0));
        }

        JFreeChart chart = ChartFactory.createBarChart(
                "Osiem najpopularniejszych marek samochodów osobowych w województwie pomorskim",
                "Marka pojazdu",
                "Ilość pojazdów",
                dcd,
                PlotOrientation.VERTICAL,
                false,
                true,
                false
        );

        CategoryPlot plot = (CategoryPlot) chart.getPlot();
        plot.setForegroundAlpha(0.7f);
        BarRenderer renderer = (BarRenderer) plot.getRenderer();
        renderer.setBaseItemLabelGenerator(new StandardCategoryItemLabelGenerator());
        renderer.setBaseItemLabelsVisible(true);
        renderer.setBaseItemLabelFont(new Font("Arial", Font.BOLD, 16));
        ItemLabelPosition position = new ItemLabelPosition(ItemLabelAnchor.OUTSIDE12,
                TextAnchor.TOP_CENTER);
        renderer.setBasePositiveItemLabelPosition(position);
        plot.getRenderer().setSeriesPaint(0, Color.decode("#006ff9"));
        plot.setRangeGridlinePaint(Color.BLACK);
        plot.setBackgroundPaint(Color.WHITE);

        try {
            ChartUtilities.saveChartAsPNG(
                    new File("F:\\II stopień\\Semestr IV\\Praca magisterska\\Analiza danych\\car_brand_survey\\pomorskie.png"),
                    chart,
                    1400,
                    600
            );
        } catch (IOException e) {
            System.out.println("Wystapil problem podczas tworzenia wykresu");
        }

        JFrame frame = new JFrame("Popularność marek samochodów osobowych" +
                " - województwo pomorskie");
        frame.setSize(1400, 600);
        frame.setLayout(new FlowLayout());
        frame.getContentPane().add(new ChartPanel(chart));
        frame.pack();
        frame.setVisible(true);
    }

    public void popularity_of_car_brand_woj_slaskie() {
        // Obliczanie 8 najpopularniejszych marek w wojewodztwie
        Dataset<Row> samochody_osobowe_marka = cepikData.getWojewodztwo_slaskie()
                .filter(col("rodzaj").equalTo("SAMOCHÓD OSOBOWY")
                        .and(col("data_wyrejestrowania").isNull()))
                .filter(col("marka").isNotNull());

        samochody_osobowe = samochody_osobowe.unionByName(samochody_osobowe_marka);

        List<Row> marki = samochody_osobowe_marka.groupBy("marka").count()
                .sort(col("count").desc()).limit(8).collectAsList();

        // Tworzenie wykresu
        DefaultCategoryDataset dcd = new DefaultCategoryDataset();

        for(Row row : marki) {
            dcd.setValue((long)row.get(1), "marka", (String)row.get(0));
        }

        JFreeChart chart = ChartFactory.createBarChart(
                "Osiem najpopularniejszych marek samochodów osobowych w województwie śląskim",
                "Marka pojazdu",
                "Ilość pojazdów",
                dcd,
                PlotOrientation.VERTICAL,
                false,
                true,
                false
        );

        CategoryPlot plot = (CategoryPlot) chart.getPlot();
        plot.setForegroundAlpha(0.7f);
        BarRenderer renderer = (BarRenderer) plot.getRenderer();
        renderer.setBaseItemLabelGenerator(new StandardCategoryItemLabelGenerator());
        renderer.setBaseItemLabelsVisible(true);
        renderer.setBaseItemLabelFont(new Font("Arial", Font.BOLD, 16));
        ItemLabelPosition position = new ItemLabelPosition(ItemLabelAnchor.OUTSIDE12,
                TextAnchor.TOP_CENTER);
        renderer.setBasePositiveItemLabelPosition(position);
        plot.getRenderer().setSeriesPaint(0, Color.decode("#006ff9"));
        plot.setRangeGridlinePaint(Color.BLACK);
        plot.setBackgroundPaint(Color.WHITE);

        try {
            ChartUtilities.saveChartAsPNG(
                    new File("F:\\II stopień\\Semestr IV\\Praca magisterska\\Analiza danych\\car_brand_survey\\slaskie.png"),
                    chart,
                    1400,
                    600
            );
        } catch (IOException e) {
            System.out.println("Wystapil problem podczas tworzenia wykresu");
        }

        JFrame frame = new JFrame("Popularność marek samochodów osobowych" +
                " - województwo śląskie");
        frame.setSize(1400, 600);
        frame.setLayout(new FlowLayout());
        frame.getContentPane().add(new ChartPanel(chart));
        frame.pack();
        frame.setVisible(true);
    }

    public void popularity_of_car_brand_woj_swietokrzyskie() {
        // Obliczanie 8 najpopularniejszych marek w wojewodztwie
        Dataset<Row> samochody_osobowe_marka = cepikData.getWojewodztwo_swietokrzyskie()
                .filter(col("rodzaj").equalTo("SAMOCHÓD OSOBOWY")
                        .and(col("data_wyrejestrowania").isNull()))
                .filter(col("marka").isNotNull());

        samochody_osobowe = samochody_osobowe.unionByName(samochody_osobowe_marka);

        List<Row> marki = samochody_osobowe_marka.groupBy("marka").count()
                .sort(col("count").desc()).limit(8).collectAsList();

        // Tworzenie wykresu
        DefaultCategoryDataset dcd = new DefaultCategoryDataset();

        for(Row row : marki) {
            dcd.setValue((long)row.get(1), "marka", (String)row.get(0));
        }

        JFreeChart chart = ChartFactory.createBarChart(
                "Osiem najpopularniejszych marek samochodów osobowych w województwie świętokrzyskim",
                "Marka pojazdu",
                "Ilość pojazdów",
                dcd,
                PlotOrientation.VERTICAL,
                false,
                true,
                false
        );

        CategoryPlot plot = (CategoryPlot) chart.getPlot();
        plot.setForegroundAlpha(0.7f);
        BarRenderer renderer = (BarRenderer) plot.getRenderer();
        renderer.setBaseItemLabelGenerator(new StandardCategoryItemLabelGenerator());
        renderer.setBaseItemLabelsVisible(true);
        renderer.setBaseItemLabelFont(new Font("Arial", Font.BOLD, 16));
        ItemLabelPosition position = new ItemLabelPosition(ItemLabelAnchor.OUTSIDE12,
                TextAnchor.TOP_CENTER);
        renderer.setBasePositiveItemLabelPosition(position);
        plot.getRenderer().setSeriesPaint(0, Color.decode("#006ff9"));
        plot.setRangeGridlinePaint(Color.BLACK);
        plot.setBackgroundPaint(Color.WHITE);

        try {
            ChartUtilities.saveChartAsPNG(
                    new File("F:\\II stopień\\Semestr IV\\Praca magisterska\\Analiza danych\\car_brand_survey\\swietokrzyskie.png"),
                    chart,
                    1400,
                    600
            );
        } catch (IOException e) {
            System.out.println("Wystapil problem podczas tworzenia wykresu");
        }

        JFrame frame = new JFrame("Popularność marek samochodów osobowych" +
                " - województwo świętokrzyskie");
        frame.setSize(1400, 600);
        frame.setLayout(new FlowLayout());
        frame.getContentPane().add(new ChartPanel(chart));
        frame.pack();
        frame.setVisible(true);
    }

    public void popularity_of_car_brand_woj_warminskomazurskie() {
        // Obliczanie 8 najpopularniejszych marek w wojewodztwie
        Dataset<Row> samochody_osobowe_marka = cepikData.getWojewodztwo_warminskomazurskie()
                .filter(col("rodzaj").equalTo("SAMOCHÓD OSOBOWY")
                        .and(col("data_wyrejestrowania").isNull()))
                .filter(col("marka").isNotNull());

        samochody_osobowe = samochody_osobowe.unionByName(samochody_osobowe_marka);

        List<Row> marki = samochody_osobowe_marka.groupBy("marka").count()
                .sort(col("count").desc()).limit(8).collectAsList();

        // Tworzenie wykresu
        DefaultCategoryDataset dcd = new DefaultCategoryDataset();

        for(Row row : marki) {
            dcd.setValue((long)row.get(1), "marka", (String)row.get(0));
        }

        JFreeChart chart = ChartFactory.createBarChart(
                "Osiem najpopularniejszych marek samochodów osobowych w województwie warmińsko-mazurskim",
                "Marka pojazdu",
                "Ilość pojazdów",
                dcd,
                PlotOrientation.VERTICAL,
                false,
                true,
                false
        );

        CategoryPlot plot = (CategoryPlot) chart.getPlot();
        plot.setForegroundAlpha(0.7f);
        BarRenderer renderer = (BarRenderer) plot.getRenderer();
        renderer.setBaseItemLabelGenerator(new StandardCategoryItemLabelGenerator());
        renderer.setBaseItemLabelsVisible(true);
        renderer.setBaseItemLabelFont(new Font("Arial", Font.BOLD, 16));
        ItemLabelPosition position = new ItemLabelPosition(ItemLabelAnchor.OUTSIDE12,
                TextAnchor.TOP_CENTER);
        renderer.setBasePositiveItemLabelPosition(position);
        plot.getRenderer().setSeriesPaint(0, Color.decode("#006ff9"));
        plot.setRangeGridlinePaint(Color.BLACK);
        plot.setBackgroundPaint(Color.WHITE);

        try {
            ChartUtilities.saveChartAsPNG(
                    new File("F:\\II stopień\\Semestr IV\\Praca magisterska\\Analiza danych\\car_brand_survey\\warminskomazurskie.png"),
                    chart,
                    1400,
                    600
            );
        } catch (IOException e) {
            System.out.println("Wystapil problem podczas tworzenia wykresu");
        }

        JFrame frame = new JFrame("Popularność marek samochodów osobowych" +
                " - województwo warmińsko-mazurskie");
        frame.setSize(1400, 600);
        frame.setLayout(new FlowLayout());
        frame.getContentPane().add(new ChartPanel(chart));
        frame.pack();
        frame.setVisible(true);
    }

    public void popularity_of_car_brand_woj_wielkopolskie() {
        // Obliczanie 8 najpopularniejszych marek w wojewodztwie
        Dataset<Row> samochody_osobowe_marka = cepikData.getWojewodztwo_wielkopolskie()
                .filter(col("rodzaj").equalTo("SAMOCHÓD OSOBOWY")
                        .and(col("data_wyrejestrowania").isNull()))
                .filter(col("marka").isNotNull());

        samochody_osobowe = samochody_osobowe.unionByName(samochody_osobowe_marka);

        List<Row> marki = samochody_osobowe_marka.groupBy("marka").count()
                .sort(col("count").desc()).limit(8).collectAsList();

        // Tworzenie wykresu
        DefaultCategoryDataset dcd = new DefaultCategoryDataset();

        for(Row row : marki) {
            dcd.setValue((long)row.get(1), "marka", (String)row.get(0));
        }

        JFreeChart chart = ChartFactory.createBarChart(
                "Osiem najpopularniejszych marek samochodów osobowych w województwie wielkopolskim",
                "Marka pojazdu",
                "Ilość pojazdów",
                dcd,
                PlotOrientation.VERTICAL,
                false,
                true,
                false
        );

        CategoryPlot plot = (CategoryPlot) chart.getPlot();
        plot.setForegroundAlpha(0.7f);
        BarRenderer renderer = (BarRenderer) plot.getRenderer();
        renderer.setBaseItemLabelGenerator(new StandardCategoryItemLabelGenerator());
        renderer.setBaseItemLabelsVisible(true);
        renderer.setBaseItemLabelFont(new Font("Arial", Font.BOLD, 16));
        ItemLabelPosition position = new ItemLabelPosition(ItemLabelAnchor.OUTSIDE12,
                TextAnchor.TOP_CENTER);
        renderer.setBasePositiveItemLabelPosition(position);
        plot.getRenderer().setSeriesPaint(0, Color.decode("#006ff9"));
        plot.setRangeGridlinePaint(Color.BLACK);
        plot.setBackgroundPaint(Color.WHITE);

        try {
            ChartUtilities.saveChartAsPNG(
                    new File("F:\\II stopień\\Semestr IV\\Praca magisterska\\Analiza danych\\car_brand_survey\\wielkopolskie.png"),
                    chart,
                    1400,
                    600
            );
        } catch (IOException e) {
            System.out.println("Wystapil problem podczas tworzenia wykresu");
        }

        JFrame frame = new JFrame("Popularność marek samochodów osobowych" +
                " - województwo wielkopolskie");
        frame.setSize(1400, 600);
        frame.setLayout(new FlowLayout());
        frame.getContentPane().add(new ChartPanel(chart));
        frame.pack();
        frame.setVisible(true);
    }

    public void popularity_of_car_brand_woj_zachodniopomorskie() {
        // Obliczanie 8 najpopularniejszych marek w wojewodztwie
        Dataset<Row> samochody_osobowe_marka = cepikData.getWojewodztwo_zachodniopomorskie()
                .filter(col("rodzaj").equalTo("SAMOCHÓD OSOBOWY")
                        .and(col("data_wyrejestrowania").isNull()))
                .filter(col("marka").isNotNull());

        samochody_osobowe = samochody_osobowe.unionByName(samochody_osobowe_marka);

        List<Row> marki = samochody_osobowe_marka.groupBy("marka").count()
                .sort(col("count").desc()).limit(8).collectAsList();

        // Tworzenie wykresu
        DefaultCategoryDataset dcd = new DefaultCategoryDataset();

        for(Row row : marki) {
            dcd.setValue((long)row.get(1), "marka", (String)row.get(0));
        }

        JFreeChart chart = ChartFactory.createBarChart(
                "Osiem najpopularniejszych marek samochodów osobowych w województwie zachodniopomorskim",
                "Marka pojazdu",
                "Ilość pojazdów",
                dcd,
                PlotOrientation.VERTICAL,
                false,
                true,
                false
        );

        CategoryPlot plot = (CategoryPlot) chart.getPlot();
        plot.setForegroundAlpha(0.7f);
        BarRenderer renderer = (BarRenderer) plot.getRenderer();
        renderer.setBaseItemLabelGenerator(new StandardCategoryItemLabelGenerator());
        renderer.setBaseItemLabelsVisible(true);
        renderer.setBaseItemLabelFont(new Font("Arial", Font.BOLD, 16));
        ItemLabelPosition position = new ItemLabelPosition(ItemLabelAnchor.OUTSIDE12,
                TextAnchor.TOP_CENTER);
        renderer.setBasePositiveItemLabelPosition(position);
        plot.getRenderer().setSeriesPaint(0, Color.decode("#006ff9"));
        plot.setRangeGridlinePaint(Color.BLACK);
        plot.setBackgroundPaint(Color.WHITE);

        try {
            ChartUtilities.saveChartAsPNG(
                    new File("F:\\II stopień\\Semestr IV\\Praca magisterska\\Analiza danych\\car_brand_survey\\zachodniopomorskie.png"),
                    chart,
                    1400,
                    600
            );
        } catch (IOException e) {
            System.out.println("Wystapil problem podczas tworzenia wykresu");
        }

        JFrame frame = new JFrame("Popularność marek samochodów osobowych" +
                " - województwo zachodniopomorskie");
        frame.setSize(1400, 600);
        frame.setLayout(new FlowLayout());
        frame.getContentPane().add(new ChartPanel(chart));
        frame.pack();
        frame.setVisible(true);
    }

    public void popularity_of_car_brand_in_poland() {
        // dodanie pojazdow z danych, ktore nie maja okreslonego wojewodztwa
        Dataset<Row> samochody_osobowe_marka = cepikData.getNieokreslone_wojewodztwo()
                .filter(col("rodzaj").equalTo("SAMOCHÓD OSOBOWY")
                        .and(col("data_wyrejestrowania").isNull()))
                .filter(col("marka").isNotNull());

        samochody_osobowe = samochody_osobowe.unionByName(samochody_osobowe_marka);

        List<Row> marki = samochody_osobowe.groupBy("marka").count()
                .sort(col("count").desc()).limit(8).collectAsList();

        // Tworzenie wykresu
        DefaultCategoryDataset dcd = new DefaultCategoryDataset();

        for(Row row : marki) {
            dcd.setValue((long)row.get(1), "marka", (String)row.get(0));
        }

        JFreeChart chart = ChartFactory.createBarChart(
                "Osiem najpopularniejszych marek samochodów osobowych w Polsce (cały kraj)",
                "Marka pojazdu",
                "Ilość pojazdów",
                dcd,
                PlotOrientation.VERTICAL,
                false,
                true,
                false
        );

        CategoryPlot plot = (CategoryPlot) chart.getPlot();
        plot.setForegroundAlpha(0.7f);
        BarRenderer renderer = (BarRenderer) plot.getRenderer();
        renderer.setBaseItemLabelGenerator(new StandardCategoryItemLabelGenerator());
        renderer.setBaseItemLabelsVisible(true);
        renderer.setBaseItemLabelFont(new Font("Arial", Font.BOLD, 16));
        ItemLabelPosition position = new ItemLabelPosition(ItemLabelAnchor.OUTSIDE12,
                TextAnchor.TOP_CENTER);
        renderer.setBasePositiveItemLabelPosition(position);
        plot.getRenderer().setSeriesPaint(0, Color.decode("#006ff9"));
        plot.setRangeGridlinePaint(Color.BLACK);
        plot.setBackgroundPaint(Color.WHITE);

        try {
            ChartUtilities.saveChartAsPNG(
                    new File("F:\\II stopień\\Semestr IV\\Praca magisterska\\Analiza danych\\car_brand_survey\\polska.png"),
                    chart,
                    1400,
                    600
            );
        } catch (IOException e) {
            System.out.println("Wystapil problem podczas tworzenia wykresu");
        }

        JFrame frame = new JFrame("Popularność marek samochodów osobowych" +
                " - Polska (cały kraj)");
        frame.setSize(1400, 600);
        frame.setLayout(new FlowLayout());
        frame.getContentPane().add(new ChartPanel(chart));
        frame.pack();
        frame.setVisible(true);
    }

    // EXTRA: powiat radzynski
    public void popularity_of_car_brand_powiat_radzynski() {
        // Obliczanie 8 najpopularniejszych marek w powiecie radzynskim
        Dataset<Row> samochody_osobowe_marka = cepikData.getWojewodztwo_lubelskie()
                .filter(col("rodzaj").equalTo("SAMOCHÓD OSOBOWY")
                        .and(col("data_wyrejestrowania").isNull()))
                .filter(col("marka").isNotNull());

        Dataset<Row> so_marka_pow_radzynski = samochody_osobowe_marka
                .filter(col("akt_miejsce_rej_powiat").equalTo("RADZYŃSKI"));

        List<Row> marki = so_marka_pow_radzynski.groupBy("marka").count()
                .sort(col("count").desc()).limit(8).collectAsList();

        // Tworzenie wykresu
        DefaultCategoryDataset dcd = new DefaultCategoryDataset();

        for(Row row : marki) {
            dcd.setValue((long)row.get(1), "marka", (String)row.get(0));
        }

        JFreeChart chart = ChartFactory.createBarChart(
                "Osiem najpopularniejszych marek samochodów osobowych w powiecie radzyńskim (woj. lubelskie)",
                "Marka pojazdu",
                "Ilość pojazdów",
                dcd,
                PlotOrientation.VERTICAL,
                false,
                true,
                false
        );

        CategoryPlot plot = (CategoryPlot) chart.getPlot();
        plot.setForegroundAlpha(0.7f);
        BarRenderer renderer = (BarRenderer) plot.getRenderer();
        renderer.setBaseItemLabelGenerator(new StandardCategoryItemLabelGenerator());
        renderer.setBaseItemLabelsVisible(true);
        renderer.setBaseItemLabelFont(new Font("Arial", Font.BOLD, 16));
        ItemLabelPosition position = new ItemLabelPosition(ItemLabelAnchor.OUTSIDE12,
                TextAnchor.TOP_CENTER);
        renderer.setBasePositiveItemLabelPosition(position);
        plot.getRenderer().setSeriesPaint(0, Color.decode("#006ff9"));
        plot.setRangeGridlinePaint(Color.BLACK);
        plot.setBackgroundPaint(Color.WHITE);

        try {
            ChartUtilities.saveChartAsPNG(
                    new File("F:\\II stopień\\Semestr IV\\Praca magisterska\\Analiza danych\\car_brand_survey\\powiat_radzynski.png"),
                    chart,
                    1400,
                    600
            );
        } catch (IOException e) {
            System.out.println("Wystapil problem podczas tworzenia wykresu");
        }

        JFrame frame = new JFrame("Popularność marek samochodów osobowych" +
                " - powiat radzyński (woj. lubelskie)");
        frame.setSize(1400, 600);
        frame.setLayout(new FlowLayout());
        frame.getContentPane().add(new ChartPanel(chart));
        frame.pack();
        frame.setVisible(true);
    }
}
