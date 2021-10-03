package com.company;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.jfree.chart.ChartFactory;
import org.jfree.chart.ChartPanel;
import org.jfree.chart.ChartUtilities;
import org.jfree.chart.JFreeChart;
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

import static org.apache.spark.sql.functions.col;

public class CarRegistrationOverTheYearsSurvey {
    private CepikData cepikData;
    private Dataset<Row> samochody_osobowe;

    public CarRegistrationOverTheYearsSurvey(CepikData cepikData) {
        this.cepikData = cepikData;
        samochody_osobowe = cepikData.getWojewodztwo_dolnoslaskie()
                .filter((col("rodzaj").equalTo("SAMOCHÓD OSOBOWY")
                        .and(col("marka").isNotNull()))
                        .and((col("marka").equalTo("VOLKSWAGEN")
                                .or(col("marka").equalTo("OPEL")
                                        .or(col("marka").equalTo("FORD")
                                                .or(col("marka").equalTo("RENAULT")
                                                        .or(col("marka").equalTo("FIAT")
                                                                .or(col("marka").equalTo("SKODA")
                                                                        .or(col("marka").equalTo("AUDI")
                                                                                .or(col("marka").equalTo("TOYOTA")))))))))));

        samochody_osobowe = samochody_osobowe.unionByName(cepikData.getWojewodztwo_kujawskopomorskie()
                .filter((col("rodzaj").equalTo("SAMOCHÓD OSOBOWY")
                        .and(col("marka").isNotNull()))
                        .and((col("marka").equalTo("VOLKSWAGEN")
                                .or(col("marka").equalTo("OPEL")
                                        .or(col("marka").equalTo("FORD")
                                                .or(col("marka").equalTo("RENAULT")
                                                        .or(col("marka").equalTo("FIAT")
                                                                .or(col("marka").equalTo("SKODA")
                                                                        .or(col("marka").equalTo("AUDI")
                                                                                .or(col("marka").equalTo("TOYOTA"))))))))))));

        samochody_osobowe = samochody_osobowe.unionByName(cepikData.getWojewodztwo_lubelskie()
                .filter((col("rodzaj").equalTo("SAMOCHÓD OSOBOWY")
                        .and(col("marka").isNotNull()))
                        .and((col("marka").equalTo("VOLKSWAGEN")
                                .or(col("marka").equalTo("OPEL")
                                        .or(col("marka").equalTo("FORD")
                                                .or(col("marka").equalTo("RENAULT")
                                                        .or(col("marka").equalTo("FIAT")
                                                                .or(col("marka").equalTo("SKODA")
                                                                        .or(col("marka").equalTo("AUDI")
                                                                                .or(col("marka").equalTo("TOYOTA"))))))))))));

        samochody_osobowe = samochody_osobowe.unionByName(cepikData.getWojewodztwo_lubuskie()
                .filter((col("rodzaj").equalTo("SAMOCHÓD OSOBOWY")
                        .and(col("marka").isNotNull()))
                        .and((col("marka").equalTo("VOLKSWAGEN")
                                .or(col("marka").equalTo("OPEL")
                                        .or(col("marka").equalTo("FORD")
                                                .or(col("marka").equalTo("RENAULT")
                                                        .or(col("marka").equalTo("FIAT")
                                                                .or(col("marka").equalTo("SKODA")
                                                                        .or(col("marka").equalTo("AUDI")
                                                                                .or(col("marka").equalTo("TOYOTA"))))))))))));

        samochody_osobowe = samochody_osobowe.unionByName(cepikData.getWojewodztwo_lodzkie()
                .filter((col("rodzaj").equalTo("SAMOCHÓD OSOBOWY")
                        .and(col("marka").isNotNull()))
                        .and((col("marka").equalTo("VOLKSWAGEN")
                                .or(col("marka").equalTo("OPEL")
                                        .or(col("marka").equalTo("FORD")
                                                .or(col("marka").equalTo("RENAULT")
                                                        .or(col("marka").equalTo("FIAT")
                                                                .or(col("marka").equalTo("SKODA")
                                                                        .or(col("marka").equalTo("AUDI")
                                                                                .or(col("marka").equalTo("TOYOTA"))))))))))));

        samochody_osobowe = samochody_osobowe.unionByName(cepikData.getWojewodztwo_malopolskie()
                .filter((col("rodzaj").equalTo("SAMOCHÓD OSOBOWY")
                        .and(col("marka").isNotNull()))
                        .and((col("marka").equalTo("VOLKSWAGEN")
                                .or(col("marka").equalTo("OPEL")
                                        .or(col("marka").equalTo("FORD")
                                                .or(col("marka").equalTo("RENAULT")
                                                        .or(col("marka").equalTo("FIAT")
                                                                .or(col("marka").equalTo("SKODA")
                                                                        .or(col("marka").equalTo("AUDI")
                                                                                .or(col("marka").equalTo("TOYOTA"))))))))))));

        samochody_osobowe = samochody_osobowe.unionByName(cepikData.getWojewodztwo_mazowieckie()
                .filter((col("rodzaj").equalTo("SAMOCHÓD OSOBOWY")
                        .and(col("marka").isNotNull()))
                        .and((col("marka").equalTo("VOLKSWAGEN")
                                .or(col("marka").equalTo("OPEL")
                                        .or(col("marka").equalTo("FORD")
                                                .or(col("marka").equalTo("RENAULT")
                                                        .or(col("marka").equalTo("FIAT")
                                                                .or(col("marka").equalTo("SKODA")
                                                                        .or(col("marka").equalTo("AUDI")
                                                                                .or(col("marka").equalTo("TOYOTA"))))))))))));

        samochody_osobowe = samochody_osobowe.unionByName(cepikData.getWojewodztwo_opolskie()
                .filter((col("rodzaj").equalTo("SAMOCHÓD OSOBOWY")
                        .and(col("marka").isNotNull()))
                        .and((col("marka").equalTo("VOLKSWAGEN")
                                .or(col("marka").equalTo("OPEL")
                                        .or(col("marka").equalTo("FORD")
                                                .or(col("marka").equalTo("RENAULT")
                                                        .or(col("marka").equalTo("FIAT")
                                                                .or(col("marka").equalTo("SKODA")
                                                                        .or(col("marka").equalTo("AUDI")
                                                                                .or(col("marka").equalTo("TOYOTA"))))))))))));

        samochody_osobowe = samochody_osobowe.unionByName(cepikData.getWojewodztwo_podkarpackie()
                .filter((col("rodzaj").equalTo("SAMOCHÓD OSOBOWY")
                        .and(col("marka").isNotNull()))
                        .and((col("marka").equalTo("VOLKSWAGEN")
                                .or(col("marka").equalTo("OPEL")
                                        .or(col("marka").equalTo("FORD")
                                                .or(col("marka").equalTo("RENAULT")
                                                        .or(col("marka").equalTo("FIAT")
                                                                .or(col("marka").equalTo("SKODA")
                                                                        .or(col("marka").equalTo("AUDI")
                                                                                .or(col("marka").equalTo("TOYOTA"))))))))))));

        samochody_osobowe = samochody_osobowe.unionByName(cepikData.getWojewodztwo_podlaskie()
                .filter((col("rodzaj").equalTo("SAMOCHÓD OSOBOWY")
                        .and(col("marka").isNotNull()))
                        .and((col("marka").equalTo("VOLKSWAGEN")
                                .or(col("marka").equalTo("OPEL")
                                        .or(col("marka").equalTo("FORD")
                                                .or(col("marka").equalTo("RENAULT")
                                                        .or(col("marka").equalTo("FIAT")
                                                                .or(col("marka").equalTo("SKODA")
                                                                        .or(col("marka").equalTo("AUDI")
                                                                                .or(col("marka").equalTo("TOYOTA"))))))))))));

        samochody_osobowe = samochody_osobowe.unionByName(cepikData.getWojewodztwo_pomorskie()
                .filter((col("rodzaj").equalTo("SAMOCHÓD OSOBOWY")
                        .and(col("marka").isNotNull()))
                        .and((col("marka").equalTo("VOLKSWAGEN")
                                .or(col("marka").equalTo("OPEL")
                                        .or(col("marka").equalTo("FORD")
                                                .or(col("marka").equalTo("RENAULT")
                                                        .or(col("marka").equalTo("FIAT")
                                                                .or(col("marka").equalTo("SKODA")
                                                                        .or(col("marka").equalTo("AUDI")
                                                                                .or(col("marka").equalTo("TOYOTA"))))))))))));

        samochody_osobowe = samochody_osobowe.unionByName(cepikData.getWojewodztwo_slaskie()
                .filter((col("rodzaj").equalTo("SAMOCHÓD OSOBOWY")
                        .and(col("marka").isNotNull()))
                        .and((col("marka").equalTo("VOLKSWAGEN")
                                .or(col("marka").equalTo("OPEL")
                                        .or(col("marka").equalTo("FORD")
                                                .or(col("marka").equalTo("RENAULT")
                                                        .or(col("marka").equalTo("FIAT")
                                                                .or(col("marka").equalTo("SKODA")
                                                                        .or(col("marka").equalTo("AUDI")
                                                                                .or(col("marka").equalTo("TOYOTA"))))))))))));

        samochody_osobowe = samochody_osobowe.unionByName(cepikData.getWojewodztwo_swietokrzyskie()
                .filter((col("rodzaj").equalTo("SAMOCHÓD OSOBOWY")
                        .and(col("marka").isNotNull()))
                        .and((col("marka").equalTo("VOLKSWAGEN")
                                .or(col("marka").equalTo("OPEL")
                                        .or(col("marka").equalTo("FORD")
                                                .or(col("marka").equalTo("RENAULT")
                                                        .or(col("marka").equalTo("FIAT")
                                                                .or(col("marka").equalTo("SKODA")
                                                                        .or(col("marka").equalTo("AUDI")
                                                                                .or(col("marka").equalTo("TOYOTA"))))))))))));

        samochody_osobowe = samochody_osobowe.unionByName(cepikData.getWojewodztwo_warminskomazurskie()
                .filter((col("rodzaj").equalTo("SAMOCHÓD OSOBOWY")
                        .and(col("marka").isNotNull()))
                        .and((col("marka").equalTo("VOLKSWAGEN")
                                .or(col("marka").equalTo("OPEL")
                                        .or(col("marka").equalTo("FORD")
                                                .or(col("marka").equalTo("RENAULT")
                                                        .or(col("marka").equalTo("FIAT")
                                                                .or(col("marka").equalTo("SKODA")
                                                                        .or(col("marka").equalTo("AUDI")
                                                                                .or(col("marka").equalTo("TOYOTA"))))))))))));

        samochody_osobowe = samochody_osobowe.unionByName(cepikData.getWojewodztwo_wielkopolskie()
                .filter((col("rodzaj").equalTo("SAMOCHÓD OSOBOWY")
                        .and(col("marka").isNotNull()))
                        .and((col("marka").equalTo("VOLKSWAGEN")
                                .or(col("marka").equalTo("OPEL")
                                        .or(col("marka").equalTo("FORD")
                                                .or(col("marka").equalTo("RENAULT")
                                                        .or(col("marka").equalTo("FIAT")
                                                                .or(col("marka").equalTo("SKODA")
                                                                        .or(col("marka").equalTo("AUDI")
                                                                                .or(col("marka").equalTo("TOYOTA"))))))))))));

        samochody_osobowe = samochody_osobowe.unionByName(cepikData.getWojewodztwo_zachodniopomorskie()
                .filter((col("rodzaj").equalTo("SAMOCHÓD OSOBOWY")
                        .and(col("marka").isNotNull()))
                        .and((col("marka").equalTo("VOLKSWAGEN")
                                .or(col("marka").equalTo("OPEL")
                                        .or(col("marka").equalTo("FORD")
                                                .or(col("marka").equalTo("RENAULT")
                                                        .or(col("marka").equalTo("FIAT")
                                                                .or(col("marka").equalTo("SKODA")
                                                                        .or(col("marka").equalTo("AUDI")
                                                                                .or(col("marka").equalTo("TOYOTA"))))))))))));

        samochody_osobowe = samochody_osobowe.unionByName(cepikData.getNieokreslone_wojewodztwo()
                .filter((col("rodzaj").equalTo("SAMOCHÓD OSOBOWY")
                        .and(col("marka").isNotNull()))
                        .and((col("marka").equalTo("VOLKSWAGEN")
                                .or(col("marka").equalTo("OPEL")
                                        .or(col("marka").equalTo("FORD")
                                                .or(col("marka").equalTo("RENAULT")
                                                        .or(col("marka").equalTo("FIAT")
                                                                .or(col("marka").equalTo("SKODA")
                                                                        .or(col("marka").equalTo("AUDI")
                                                                                .or(col("marka").equalTo("TOYOTA"))))))))))));
    }

    public void car_brand_registration_trend_over_the_years() {
        List<Row> reg_2000 = samochody_osobowe
                .filter(col("data_rejestracji_ost").like("2000-%"))
                .groupBy("marka").count().sort(col("count").desc())
                .collectAsList();

        List<Row> reg_2002 = samochody_osobowe
                .filter(col("data_rejestracji_ost").like("2002-%"))
                .groupBy("marka").count().sort(col("count").desc())
                .collectAsList();

        List<Row> reg_2004 = samochody_osobowe
                .filter(col("data_rejestracji_ost").like("2004-%"))
                .groupBy("marka").count().sort(col("count").desc())
                .collectAsList();

        List<Row> reg_2006 = samochody_osobowe
                .filter(col("data_rejestracji_ost").like("2006-%"))
                .groupBy("marka").count().sort(col("count").desc())
                .collectAsList();

        List<Row> reg_2008 = samochody_osobowe
                .filter(col("data_rejestracji_ost").like("2008-%"))
                .groupBy("marka").count().sort(col("count").desc())
                .collectAsList();

        List<Row> reg_2010 = samochody_osobowe
                .filter(col("data_rejestracji_ost").like("2010-%"))
                .groupBy("marka").count().sort(col("count").desc())
                .collectAsList();

        List<Row> reg_2012 = samochody_osobowe
                .filter(col("data_rejestracji_ost").like("2012-%"))
                .groupBy("marka").count().sort(col("count").desc())
                .collectAsList();

        List<Row> reg_2014 = samochody_osobowe
                .filter(col("data_rejestracji_ost").like("2014-%"))
                .groupBy("marka").count().sort(col("count").desc())
                .collectAsList();

        List<Row> reg_2016 = samochody_osobowe
                .filter(col("data_rejestracji_ost").like("2016-%"))
                .groupBy("marka").count().sort(col("count").desc())
                .collectAsList();

        List<Row> reg_2018 = samochody_osobowe
                .filter(col("data_rejestracji_ost").like("2018-%"))
                .groupBy("marka").count().sort(col("count").desc())
                .collectAsList();

        XYSeries volkswagen = new XYSeries("VOLKSWAGEN");
        XYSeries opel = new XYSeries("OPEL");
        XYSeries ford = new XYSeries("FORD");
        XYSeries renault = new XYSeries("RENAULT");
        XYSeries fiat = new XYSeries("FIAT");
        XYSeries skoda = new XYSeries("SKODA");
        XYSeries audi = new XYSeries("AUDI");
        XYSeries toyota = new XYSeries("TOYOTA");

        for (Row row : reg_2000) {
            if (row.get(0).equals("VOLKSWAGEN")) volkswagen.add(2000, (long) row.get(1));
            else if (row.get(0).equals("OPEL")) opel.add(2000, (long) row.get(1));
            else if (row.get(0).equals("FORD")) ford.add(2000, (long) row.get(1));
            else if (row.get(0).equals("RENAULT")) renault.add(2000, (long) row.get(1));
            else if (row.get(0).equals("FIAT")) fiat.add(2000, (long) row.get(1));
            else if (row.get(0).equals("SKODA")) skoda.add(2000, (long) row.get(1));
            else if (row.get(0).equals("AUDI")) audi.add(2000, (long) row.get(1));
            else if (row.get(0).equals("TOYOTA")) toyota.add(2000, (long) row.get(1));
        }

        for (Row row : reg_2002) {
            if (row.get(0).equals("VOLKSWAGEN")) volkswagen.add(2002, (long) row.get(1));
            else if (row.get(0).equals("OPEL")) opel.add(2002, (long) row.get(1));
            else if (row.get(0).equals("FORD")) ford.add(2002, (long) row.get(1));
            else if (row.get(0).equals("RENAULT")) renault.add(2002, (long) row.get(1));
            else if (row.get(0).equals("FIAT")) fiat.add(2002, (long) row.get(1));
            else if (row.get(0).equals("SKODA")) skoda.add(2002, (long) row.get(1));
            else if (row.get(0).equals("AUDI")) audi.add(2002, (long) row.get(1));
            else if (row.get(0).equals("TOYOTA")) toyota.add(2002, (long) row.get(1));
        }

        for (Row row : reg_2004) {
            if (row.get(0).equals("VOLKSWAGEN")) volkswagen.add(2004, (long) row.get(1));
            else if (row.get(0).equals("OPEL")) opel.add(2004, (long) row.get(1));
            else if (row.get(0).equals("FORD")) ford.add(2004, (long) row.get(1));
            else if (row.get(0).equals("RENAULT")) renault.add(2004, (long) row.get(1));
            else if (row.get(0).equals("FIAT")) fiat.add(2004, (long) row.get(1));
            else if (row.get(0).equals("SKODA")) skoda.add(2004, (long) row.get(1));
            else if (row.get(0).equals("AUDI")) audi.add(2004, (long) row.get(1));
            else if (row.get(0).equals("TOYOTA")) toyota.add(2004, (long) row.get(1));
        }

        for (Row row : reg_2006) {
            if (row.get(0).equals("VOLKSWAGEN")) volkswagen.add(2006, (long) row.get(1));
            else if (row.get(0).equals("OPEL")) opel.add(2006, (long) row.get(1));
            else if (row.get(0).equals("FORD")) ford.add(2006, (long) row.get(1));
            else if (row.get(0).equals("RENAULT")) renault.add(2006, (long) row.get(1));
            else if (row.get(0).equals("FIAT")) fiat.add(2006, (long) row.get(1));
            else if (row.get(0).equals("SKODA")) skoda.add(2006, (long) row.get(1));
            else if (row.get(0).equals("AUDI")) audi.add(2006, (long) row.get(1));
            else if (row.get(0).equals("TOYOTA")) toyota.add(2006, (long) row.get(1));
        }

        for (Row row : reg_2008) {
            if (row.get(0).equals("VOLKSWAGEN")) volkswagen.add(2008, (long) row.get(1));
            else if (row.get(0).equals("OPEL")) opel.add(2008, (long) row.get(1));
            else if (row.get(0).equals("FORD")) ford.add(2008, (long) row.get(1));
            else if (row.get(0).equals("RENAULT")) renault.add(2008, (long) row.get(1));
            else if (row.get(0).equals("FIAT")) fiat.add(2008, (long) row.get(1));
            else if (row.get(0).equals("SKODA")) skoda.add(2008, (long) row.get(1));
            else if (row.get(0).equals("AUDI")) audi.add(2008, (long) row.get(1));
            else if (row.get(0).equals("TOYOTA")) toyota.add(2008, (long) row.get(1));
        }

        for (Row row : reg_2010) {
            if (row.get(0).equals("VOLKSWAGEN")) volkswagen.add(2010, (long) row.get(1));
            else if (row.get(0).equals("OPEL")) opel.add(2010, (long) row.get(1));
            else if (row.get(0).equals("FORD")) ford.add(2010, (long) row.get(1));
            else if (row.get(0).equals("RENAULT")) renault.add(2010, (long) row.get(1));
            else if (row.get(0).equals("FIAT")) fiat.add(2010, (long) row.get(1));
            else if (row.get(0).equals("SKODA")) skoda.add(2010, (long) row.get(1));
            else if (row.get(0).equals("AUDI")) audi.add(2010, (long) row.get(1));
            else if (row.get(0).equals("TOYOTA")) toyota.add(2010, (long) row.get(1));
        }

        for (Row row : reg_2012) {
            if (row.get(0).equals("VOLKSWAGEN")) volkswagen.add(2012, (long) row.get(1));
            else if (row.get(0).equals("OPEL")) opel.add(2012, (long) row.get(1));
            else if (row.get(0).equals("FORD")) ford.add(2012, (long) row.get(1));
            else if (row.get(0).equals("RENAULT")) renault.add(2012, (long) row.get(1));
            else if (row.get(0).equals("FIAT")) fiat.add(2012, (long) row.get(1));
            else if (row.get(0).equals("SKODA")) skoda.add(2012, (long) row.get(1));
            else if (row.get(0).equals("AUDI")) audi.add(2012, (long) row.get(1));
            else if (row.get(0).equals("TOYOTA")) toyota.add(2012, (long) row.get(1));
        }

        for (Row row : reg_2014) {
            if (row.get(0).equals("VOLKSWAGEN")) volkswagen.add(2014, (long) row.get(1));
            else if (row.get(0).equals("OPEL")) opel.add(2014, (long) row.get(1));
            else if (row.get(0).equals("FORD")) ford.add(2014, (long) row.get(1));
            else if (row.get(0).equals("RENAULT")) renault.add(2014, (long) row.get(1));
            else if (row.get(0).equals("FIAT")) fiat.add(2014, (long) row.get(1));
            else if (row.get(0).equals("SKODA")) skoda.add(2014, (long) row.get(1));
            else if (row.get(0).equals("AUDI")) audi.add(2014, (long) row.get(1));
            else if (row.get(0).equals("TOYOTA")) toyota.add(2014, (long) row.get(1));
        }

        for (Row row : reg_2016) {
            if (row.get(0).equals("VOLKSWAGEN")) volkswagen.add(2016, (long) row.get(1));
            else if (row.get(0).equals("OPEL")) opel.add(2016, (long) row.get(1));
            else if (row.get(0).equals("FORD")) ford.add(2016, (long) row.get(1));
            else if (row.get(0).equals("RENAULT")) renault.add(2016, (long) row.get(1));
            else if (row.get(0).equals("FIAT")) fiat.add(2016, (long) row.get(1));
            else if (row.get(0).equals("SKODA")) skoda.add(2016, (long) row.get(1));
            else if (row.get(0).equals("AUDI")) audi.add(2016, (long) row.get(1));
            else if (row.get(0).equals("TOYOTA")) toyota.add(2016, (long) row.get(1));
        }

        for (Row row : reg_2018) {
            if (row.get(0).equals("VOLKSWAGEN")) volkswagen.add(2018, (long) row.get(1));
            else if (row.get(0).equals("OPEL")) opel.add(2018, (long) row.get(1));
            else if (row.get(0).equals("FORD")) ford.add(2018, (long) row.get(1));
            else if (row.get(0).equals("RENAULT")) renault.add(2018, (long) row.get(1));
            else if (row.get(0).equals("FIAT")) fiat.add(2018, (long) row.get(1));
            else if (row.get(0).equals("SKODA")) skoda.add(2018, (long) row.get(1));
            else if (row.get(0).equals("AUDI")) audi.add(2018, (long) row.get(1));
            else if (row.get(0).equals("TOYOTA")) toyota.add(2018, (long) row.get(1));
        }

        XYSeriesCollection xysc = new XYSeriesCollection();
        xysc.addSeries(volkswagen);
        xysc.addSeries(opel);
        xysc.addSeries(ford);
        xysc.addSeries(renault);
        xysc.addSeries(fiat);
        xysc.addSeries(skoda);
        xysc.addSeries(audi);
        xysc.addSeries(toyota);

        JFreeChart chart = ChartFactory.createXYLineChart(
                "Zmiana trendu rejestrowanych pojazdów ośmiu najpopularniejszych marek w Polsce " +
                        "na przestrzeni lat 2000 - 2018 (co dwa lata)",
                "Rok rejestracji pojazdu",
                "Ilość zarejestrowanych pojazdów",
                xysc,
                PlotOrientation.VERTICAL,
                true,
                true,
                false
        );

        XYPlot plot = (XYPlot) chart.getPlot();
        plot.setBackgroundPaint(Color.white);
        plot.setRangeGridlinesVisible(true);
        plot.setRangeGridlinePaint(Color.BLACK);
        plot.setDomainGridlinesVisible(true);
        plot.setDomainGridlinePaint(Color.BLACK);
        XYLineAndShapeRenderer renderer = new XYLineAndShapeRenderer();
        renderer.setSeriesStroke(0, new BasicStroke(2.0f));
        renderer.setSeriesStroke(1, new BasicStroke(2.0f));
        renderer.setSeriesStroke(2, new BasicStroke(2.0f));
        renderer.setSeriesStroke(3, new BasicStroke(2.0f));
        renderer.setSeriesStroke(4, new BasicStroke(2.0f));
        renderer.setSeriesStroke(5, new BasicStroke(2.0f));
        renderer.setSeriesStroke(6, new BasicStroke(2.0f));
        renderer.setSeriesStroke(7, new BasicStroke(2.0f));
        renderer.setSeriesPaint(2, Color.decode("#238E23"));
        renderer.setSeriesPaint(3, Color.decode("#8D6E63")); // #795548
        renderer.setSeriesPaint(4, Color.MAGENTA);
        renderer.setSeriesPaint(5, Color.decode("#FF9900"));
        renderer.setSeriesPaint(6, Color.BLACK);
        renderer.setSeriesPaint(7, Color.CYAN);
        plot.setRenderer(renderer);
        //renderer.setSeriesShapesVisible(0, true);

        try {
            ChartUtilities.saveChartAsPNG(
                    new File("F:\\II stopień\\Semestr IV\\Praca magisterska\\Analiza danych\\trend_registration.png"),
                    chart,
                    850,//1400,
                    600
            );
        } catch (IOException e) {
            System.out.println("Wystapil problem podczas tworzenia wykresu");
        }

        JFrame frame = new JFrame("Zmiana trendu rejestrowanych pojazdów ośmiu najpopularniejszych marek w Polsce " +
                "na przestrzeni lat 2000 - 2018 (co dwa lata)");
        //frame.setSize(1400, 600);
        frame.setLayout(new FlowLayout());
        frame.getContentPane().add(new ChartPanel(chart));
        frame.pack();
        frame.setVisible(true);
    }
}
