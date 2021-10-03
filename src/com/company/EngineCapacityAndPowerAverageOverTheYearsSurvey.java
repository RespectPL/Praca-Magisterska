package com.company;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.jfree.chart.ChartFactory;
import org.jfree.chart.ChartPanel;
import org.jfree.chart.ChartUtilities;
import org.jfree.chart.JFreeChart;
import org.jfree.chart.axis.ValueAxis;
import org.jfree.chart.plot.PlotOrientation;
import org.jfree.chart.plot.XYPlot;
import org.jfree.chart.renderer.xy.XYLineAndShapeRenderer;
import org.jfree.data.xy.XYSeries;
import org.jfree.data.xy.XYSeriesCollection;

import javax.swing.*;
import java.awt.*;
import java.io.File;
import java.io.IOException;
import java.util.List;

import static org.apache.spark.sql.functions.*;

public class EngineCapacityAndPowerAverageOverTheYearsSurvey {
    private CepikData cepikData;
    private Dataset<Row> samochody_osobowe;

    public EngineCapacityAndPowerAverageOverTheYearsSurvey(CepikData cepikData) {
        this.cepikData = cepikData;
        samochody_osobowe = cepikData.getWojewodztwo_dolnoslaskie()
                .filter(col("rodzaj").equalTo("SAMOCHÓD OSOBOWY"))
                .filter(col("moc_silnika").isNotNull()
                        .and(col("moc_silnika").gt(0.0)))
                .filter(col("pojemnosc_silnika").isNotNull()
                        .and(col("pojemnosc_silnika").gt(0.0)));

        samochody_osobowe = samochody_osobowe.unionByName(cepikData.getWojewodztwo_kujawskopomorskie()
                .filter(col("rodzaj").equalTo("SAMOCHÓD OSOBOWY"))
                .filter(col("moc_silnika").isNotNull()
                        .and(col("moc_silnika").gt(0.0)))
                .filter(col("pojemnosc_silnika").isNotNull()
                        .and(col("pojemnosc_silnika").gt(0.0))));

        samochody_osobowe = samochody_osobowe.unionByName(cepikData.getWojewodztwo_lubelskie()
                .filter(col("rodzaj").equalTo("SAMOCHÓD OSOBOWY"))
                .filter(col("moc_silnika").isNotNull()
                        .and(col("moc_silnika").gt(0.0)))
                .filter(col("pojemnosc_silnika").isNotNull()
                        .and(col("pojemnosc_silnika").gt(0.0))));

        samochody_osobowe = samochody_osobowe.unionByName(cepikData.getWojewodztwo_lubuskie()
                .filter(col("rodzaj").equalTo("SAMOCHÓD OSOBOWY"))
                .filter(col("moc_silnika").isNotNull()
                        .and(col("moc_silnika").gt(0.0)))
                .filter(col("pojemnosc_silnika").isNotNull()
                        .and(col("pojemnosc_silnika").gt(0.0))));

        samochody_osobowe = samochody_osobowe.unionByName(cepikData.getWojewodztwo_lodzkie()
                .filter(col("rodzaj").equalTo("SAMOCHÓD OSOBOWY"))
                .filter(col("moc_silnika").isNotNull()
                        .and(col("moc_silnika").gt(0.0)))
                .filter(col("pojemnosc_silnika").isNotNull()
                        .and(col("pojemnosc_silnika").gt(0.0))));

        samochody_osobowe = samochody_osobowe.unionByName(cepikData.getWojewodztwo_malopolskie()
                .filter(col("rodzaj").equalTo("SAMOCHÓD OSOBOWY"))
                .filter(col("moc_silnika").isNotNull()
                        .and(col("moc_silnika").gt(0.0)))
                .filter(col("pojemnosc_silnika").isNotNull()
                        .and(col("pojemnosc_silnika").gt(0.0))));

        samochody_osobowe = samochody_osobowe.unionByName(cepikData.getWojewodztwo_mazowieckie()
                .filter(col("rodzaj").equalTo("SAMOCHÓD OSOBOWY"))
                .filter(col("moc_silnika").isNotNull()
                        .and(col("moc_silnika").gt(0.0)))
                .filter(col("pojemnosc_silnika").isNotNull()
                        .and(col("pojemnosc_silnika").gt(0.0))));

        samochody_osobowe = samochody_osobowe.unionByName(cepikData.getWojewodztwo_opolskie()
                .filter(col("rodzaj").equalTo("SAMOCHÓD OSOBOWY"))
                .filter(col("moc_silnika").isNotNull()
                        .and(col("moc_silnika").gt(0.0)))
                .filter(col("pojemnosc_silnika").isNotNull()
                        .and(col("pojemnosc_silnika").gt(0.0))));

        samochody_osobowe = samochody_osobowe.unionByName(cepikData.getWojewodztwo_podkarpackie()
                .filter(col("rodzaj").equalTo("SAMOCHÓD OSOBOWY"))
                .filter(col("moc_silnika").isNotNull()
                        .and(col("moc_silnika").gt(0.0)))
                .filter(col("pojemnosc_silnika").isNotNull()
                        .and(col("pojemnosc_silnika").gt(0.0))));

        samochody_osobowe = samochody_osobowe.unionByName(cepikData.getWojewodztwo_podlaskie()
                .filter(col("rodzaj").equalTo("SAMOCHÓD OSOBOWY"))
                .filter(col("moc_silnika").isNotNull()
                        .and(col("moc_silnika").gt(0.0)))
                .filter(col("pojemnosc_silnika").isNotNull()
                        .and(col("pojemnosc_silnika").gt(0.0))));

        samochody_osobowe = samochody_osobowe.unionByName(cepikData.getWojewodztwo_pomorskie()
                .filter(col("rodzaj").equalTo("SAMOCHÓD OSOBOWY"))
                .filter(col("moc_silnika").isNotNull()
                        .and(col("moc_silnika").gt(0.0)))
                .filter(col("pojemnosc_silnika").isNotNull()
                        .and(col("pojemnosc_silnika").gt(0.0))));

        samochody_osobowe = samochody_osobowe.unionByName(cepikData.getWojewodztwo_slaskie()
                .filter(col("rodzaj").equalTo("SAMOCHÓD OSOBOWY"))
                .filter(col("moc_silnika").isNotNull()
                        .and(col("moc_silnika").gt(0.0)))
                .filter(col("pojemnosc_silnika").isNotNull()
                        .and(col("pojemnosc_silnika").gt(0.0))));

        samochody_osobowe = samochody_osobowe.unionByName(cepikData.getWojewodztwo_swietokrzyskie()
                .filter(col("rodzaj").equalTo("SAMOCHÓD OSOBOWY"))
                .filter(col("moc_silnika").isNotNull()
                        .and(col("moc_silnika").gt(0.0)))
                .filter(col("pojemnosc_silnika").isNotNull()
                        .and(col("pojemnosc_silnika").gt(0.0))));

        samochody_osobowe = samochody_osobowe.unionByName(cepikData.getWojewodztwo_warminskomazurskie()
                .filter(col("rodzaj").equalTo("SAMOCHÓD OSOBOWY"))
                .filter(col("moc_silnika").isNotNull()
                        .and(col("moc_silnika").gt(0.0)))
                .filter(col("pojemnosc_silnika").isNotNull()
                        .and(col("pojemnosc_silnika").gt(0.0))));

        samochody_osobowe = samochody_osobowe.unionByName(cepikData.getWojewodztwo_wielkopolskie()
                .filter(col("rodzaj").equalTo("SAMOCHÓD OSOBOWY"))
                .filter(col("moc_silnika").isNotNull()
                        .and(col("moc_silnika").gt(0.0)))
                .filter(col("pojemnosc_silnika").isNotNull()
                        .and(col("pojemnosc_silnika").gt(0.0))));

        samochody_osobowe = samochody_osobowe.unionByName(cepikData.getWojewodztwo_zachodniopomorskie()
                .filter(col("rodzaj").equalTo("SAMOCHÓD OSOBOWY"))
                .filter(col("moc_silnika").isNotNull()
                        .and(col("moc_silnika").gt(0.0)))
                .filter(col("pojemnosc_silnika").isNotNull()
                        .and(col("pojemnosc_silnika").gt(0.0))));

        samochody_osobowe = samochody_osobowe.unionByName(cepikData.getNieokreslone_wojewodztwo()
                .filter(col("rodzaj").equalTo("SAMOCHÓD OSOBOWY"))
                .filter(col("moc_silnika").isNotNull()
                        .and(col("moc_silnika").gt(0.0)))
                .filter(col("pojemnosc_silnika").isNotNull()
                        .and(col("pojemnosc_silnika").gt(0.0))));
    }

    public void analyse() {
        /*double test = cepikData.getWojewodztwo_lubelskie()
                .filter(col("rodzaj").equalTo("SAMOCHÓD OSOBOWY"))
                .filter(col("pojemnosc_silnika").isNotNull()
                    .and(col("pojemnosc_silnika").gt(0.0)))
                .groupBy("pojemnosc_silnika")*/

        Row rok_2000 = samochody_osobowe
                .filter(col("rok_produkcji").equalTo(2000))
                .select(mean(col("pojemnosc_silnika")), mean(col("moc_silnika")))
                .head();

        Row rok_2002 = samochody_osobowe
                .filter(col("rok_produkcji").equalTo(2002))
                .select(mean(col("pojemnosc_silnika")), mean(col("moc_silnika")))
                .head();

        Row rok_2004 = samochody_osobowe
                .filter(col("rok_produkcji").equalTo(2004))
                .select(mean(col("pojemnosc_silnika")), mean(col("moc_silnika")))
                .head();

        Row rok_2006 = samochody_osobowe
                .filter(col("rok_produkcji").equalTo(2006))
                .select(mean(col("pojemnosc_silnika")), mean(col("moc_silnika")))
                .head();

        Row rok_2008 = samochody_osobowe
                .filter(col("rok_produkcji").equalTo(2008))
                .select(mean(col("pojemnosc_silnika")), mean(col("moc_silnika")))
                .head();

        Row rok_2010 = samochody_osobowe
                .filter(col("rok_produkcji").equalTo(2010))
                .select(mean(col("pojemnosc_silnika")), mean(col("moc_silnika")))
                .head();

        Row rok_2012 = samochody_osobowe
                .filter(col("rok_produkcji").equalTo(2012))
                .select(mean(col("pojemnosc_silnika")), mean(col("moc_silnika")))
                .head();

        Row rok_2014 = samochody_osobowe
                .filter(col("rok_produkcji").equalTo(2014))
                .select(mean(col("pojemnosc_silnika")), mean(col("moc_silnika")))
                .head();

        Row rok_2016 = samochody_osobowe
                .filter(col("rok_produkcji").equalTo(2016))
                .select(mean(col("pojemnosc_silnika")), mean(col("moc_silnika")))
                .head();

        Row rok_2018= samochody_osobowe
                .filter(col("rok_produkcji").equalTo(2018))
                .select(mean(col("pojemnosc_silnika")), mean(col("moc_silnika")))
                .head();

        XYSeries pojemnosc = new XYSeries("Pojemność silnika");
        XYSeries moc = new XYSeries("Moc silnika");

        pojemnosc.add(2000, (double)rok_2000.get(0));
        moc.add(2000, ((double)rok_2000.get(1)/0.73549875));

        pojemnosc.add(2002, (double)rok_2002.get(0));
        moc.add(2002, ((double)rok_2002.get(1)/0.73549875));

        pojemnosc.add(2004, (double)rok_2004.get(0));
        moc.add(2004, ((double)rok_2004.get(1)/0.73549875));

        pojemnosc.add(2006, (double)rok_2006.get(0));
        moc.add(2006, ((double)rok_2006.get(1)/0.73549875));

        pojemnosc.add(2008, (double)rok_2008.get(0));
        moc.add(2008, ((double)rok_2008.get(1)/0.73549875));

        pojemnosc.add(2010, (double)rok_2010.get(0));
        moc.add(2010, ((double)rok_2010.get(1)/0.73549875));

        pojemnosc.add(2012, (double)rok_2012.get(0));
        moc.add(2012, ((double)rok_2012.get(1)/0.73549875));

        pojemnosc.add(2014, (double)rok_2014.get(0));
        moc.add(2014, ((double)rok_2014.get(1)/0.73549875));

        pojemnosc.add(2016, (double)rok_2016.get(0));
        moc.add(2016, ((double)rok_2016.get(1)/0.73549875));

        pojemnosc.add(2018, (double)rok_2018.get(0));
        moc.add(2018, ((double)rok_2018.get(1)/0.73549875));

        XYSeriesCollection xysc_cap = new XYSeriesCollection();
        xysc_cap.addSeries(pojemnosc);

        XYSeriesCollection xysc_pow = new XYSeriesCollection();
        xysc_pow.addSeries(moc);

        // Tworzenie wykresu 1 - srednia pojemnosc silnika
        JFreeChart chart = ChartFactory.createXYLineChart(
                "Średnia pojemność silnika w samochodach osobowych na przestrzeni lat 2000 - 2018 (co dwa lata) w Polsce",
                "Rok",
                "Średnia pojemność (w cm3)",
                xysc_cap,
                PlotOrientation.VERTICAL,
                false,
                true,
                false
        );

        XYPlot plot = (XYPlot) chart.getPlot();
        plot.setBackgroundPaint(Color.white);
        plot.setRangeGridlinesVisible(true);
        plot.setRangeGridlinePaint(Color.BLACK);
        plot.setDomainGridlinesVisible(true);
        plot.setDomainGridlinePaint(Color.BLACK);
        ValueAxis rangeaxis = plot.getRangeAxis();
        rangeaxis.setRange(1500, 1800);
        XYLineAndShapeRenderer renderer = new XYLineAndShapeRenderer();
        renderer.setSeriesStroke(0, new BasicStroke(2.0f));
        renderer.setSeriesPaint(0, Color.decode("#006ff9"));
        plot.setRenderer(renderer);
        //renderer.setSeriesShapesVisible(0, true);

        try {
            ChartUtilities.saveChartAsPNG(
                    new File("F:\\II stopień\\Semestr IV\\Praca magisterska\\Analiza danych\\car_engine_popularity_survey\\trend_mean_engine_capacity_poland.png"),
                    chart,
                    600,//1400,
                    400 //600
            );
        } catch (IOException e) {
            System.out.println("Wystapil problem podczas tworzenia wykresu");
        }

        /*JFrame frame = new JFrame("Średnia pojemność silnika w samochodach osobowych na przestrzeni lat 2000 - 2018 (co dwa lata) w Polsce");
        frame.setSize(1400, 600);
        frame.setLayout(new FlowLayout());
        frame.getContentPane().add(new ChartPanel(chart));
        frame.pack();
        frame.setVisible(true);*/

        // Tworzenie wykresu 2 - srednia moc silnika
        JFreeChart chart2 = ChartFactory.createXYLineChart(
                "Średnia moc silnika w samochodach osobowych na przestrzeni lat 2000 - 2018 (co dwa lata) w Polsce",
                "Rok",
                "Średnia moc (w KM)",
                xysc_pow,
                PlotOrientation.VERTICAL,
                false,
                true,
                false
        );

        XYPlot plot2 = (XYPlot) chart2.getPlot();
        plot2.setBackgroundPaint(Color.white);
        plot2.setRangeGridlinesVisible(true);
        plot2.setRangeGridlinePaint(Color.BLACK);
        plot2.setDomainGridlinesVisible(true);
        plot2.setDomainGridlinePaint(Color.BLACK);
        ValueAxis rangeaxis2 = plot2.getRangeAxis();
        rangeaxis2.setRange((65/0.73549875), (105/0.73549875));
        XYLineAndShapeRenderer renderer2 = new XYLineAndShapeRenderer();
        renderer2.setSeriesStroke(0, new BasicStroke(2.0f));
        renderer2.setSeriesPaint(0, Color.RED);
        plot2.setRenderer(renderer2);

        try {
            ChartUtilities.saveChartAsPNG(
                    new File("F:\\II stopień\\Semestr IV\\Praca magisterska\\Analiza danych\\car_engine_popularity_survey\\trend_mean_engine_power_poland.png"),
                    chart2,
                    600,//1400,
                    400 // 600
            );
        } catch (IOException e) {
            System.out.println("Wystapil problem podczas tworzenia wykresu");
        }

        JFrame frame2 = new JFrame("Średnia pojemność i moc silnika w samochodach osobowych na przestrzeni lat 2000 - 2018 (co dwa lata) w Polsce");
        frame2.setSize(1400, 600);
        frame2.setLayout(new FlowLayout());
        frame2.getContentPane().add(new ChartPanel(chart));
        frame2.getContentPane().add(new ChartPanel(chart2));
        frame2.pack();
        frame2.setVisible(true);
    }
}
