package com.company;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.jfree.chart.ChartFactory;
import org.jfree.chart.ChartPanel;
import org.jfree.chart.ChartUtilities;
import org.jfree.chart.JFreeChart;
import org.jfree.chart.labels.PieSectionLabelGenerator;
import org.jfree.chart.labels.StandardPieSectionLabelGenerator;
import org.jfree.chart.plot.PiePlot;
import org.jfree.data.general.DefaultPieDataset;

import javax.swing.*;
import javax.swing.table.TableColumnModel;
import java.awt.*;
import java.io.File;
import java.text.DecimalFormat;
import java.util.List;

import static org.apache.spark.sql.functions.col;

public class CarOriginSurvey {
    private CepikData cepikData;
    private Dataset<Row> samochody_osobowe;
    private Dataset<Row> so_marka_model_pochodzenie;

    public CarOriginSurvey(CepikData cepikData) {
        this.cepikData = cepikData;
        samochody_osobowe = cepikData.getWojewodztwo_dolnoslaskie()
                .filter((col("rodzaj").equalTo("SAMOCHÓD OSOBOWY")
                        .and(col("data_wyrejestrowania").isNull()))
                        .and((col("pochodzenie").like("UŻYW%")
                                .or(col("pochodzenie").like("NOWY%")))));

        samochody_osobowe = samochody_osobowe.unionByName(cepikData.getWojewodztwo_kujawskopomorskie()
                .filter((col("rodzaj").equalTo("SAMOCHÓD OSOBOWY")
                        .and(col("data_wyrejestrowania").isNull()))
                        .and((col("pochodzenie").like("UŻYW%")
                                .or(col("pochodzenie").like("NOWY%"))))));

        samochody_osobowe = samochody_osobowe.unionByName(cepikData.getWojewodztwo_lubelskie()
                .filter((col("rodzaj").equalTo("SAMOCHÓD OSOBOWY")
                        .and(col("data_wyrejestrowania").isNull()))
                        .and((col("pochodzenie").like("UŻYW%")
                                .or(col("pochodzenie").like("NOWY%"))))));

        samochody_osobowe = samochody_osobowe.unionByName(cepikData.getWojewodztwo_lubuskie()
                .filter((col("rodzaj").equalTo("SAMOCHÓD OSOBOWY")
                        .and(col("data_wyrejestrowania").isNull()))
                        .and((col("pochodzenie").like("UŻYW%")
                                .or(col("pochodzenie").like("NOWY%"))))));

        samochody_osobowe = samochody_osobowe.unionByName(cepikData.getWojewodztwo_lodzkie()
                .filter((col("rodzaj").equalTo("SAMOCHÓD OSOBOWY")
                        .and(col("data_wyrejestrowania").isNull()))
                        .and((col("pochodzenie").like("UŻYW%")
                                .or(col("pochodzenie").like("NOWY%"))))));

        samochody_osobowe = samochody_osobowe.unionByName(cepikData.getWojewodztwo_malopolskie()
                .filter((col("rodzaj").equalTo("SAMOCHÓD OSOBOWY")
                        .and(col("data_wyrejestrowania").isNull()))
                        .and((col("pochodzenie").like("UŻYW%")
                                .or(col("pochodzenie").like("NOWY%"))))));

        samochody_osobowe = samochody_osobowe.unionByName(cepikData.getWojewodztwo_mazowieckie()
                .filter((col("rodzaj").equalTo("SAMOCHÓD OSOBOWY")
                        .and(col("data_wyrejestrowania").isNull()))
                        .and((col("pochodzenie").like("UŻYW%")
                                .or(col("pochodzenie").like("NOWY%"))))));

        samochody_osobowe = samochody_osobowe.unionByName(cepikData.getWojewodztwo_opolskie()
                .filter((col("rodzaj").equalTo("SAMOCHÓD OSOBOWY")
                        .and(col("data_wyrejestrowania").isNull()))
                        .and((col("pochodzenie").like("UŻYW%")
                                .or(col("pochodzenie").like("NOWY%"))))));

        samochody_osobowe = samochody_osobowe.unionByName(cepikData.getWojewodztwo_podkarpackie()
                .filter((col("rodzaj").equalTo("SAMOCHÓD OSOBOWY")
                        .and(col("data_wyrejestrowania").isNull()))
                        .and((col("pochodzenie").like("UŻYW%")
                                .or(col("pochodzenie").like("NOWY%"))))));

        samochody_osobowe = samochody_osobowe.unionByName(cepikData.getWojewodztwo_podlaskie()
                .filter((col("rodzaj").equalTo("SAMOCHÓD OSOBOWY")
                        .and(col("data_wyrejestrowania").isNull()))
                        .and((col("pochodzenie").like("UŻYW%")
                                .or(col("pochodzenie").like("NOWY%"))))));

        samochody_osobowe = samochody_osobowe.unionByName(cepikData.getWojewodztwo_pomorskie()
                .filter((col("rodzaj").equalTo("SAMOCHÓD OSOBOWY")
                        .and(col("data_wyrejestrowania").isNull()))
                        .and((col("pochodzenie").like("UŻYW%")
                                .or(col("pochodzenie").like("NOWY%"))))));

        samochody_osobowe = samochody_osobowe.unionByName(cepikData.getWojewodztwo_slaskie()
                .filter((col("rodzaj").equalTo("SAMOCHÓD OSOBOWY")
                        .and(col("data_wyrejestrowania").isNull()))
                        .and((col("pochodzenie").like("UŻYW%")
                                .or(col("pochodzenie").like("NOWY%"))))));

        samochody_osobowe = samochody_osobowe.unionByName(cepikData.getWojewodztwo_swietokrzyskie()
                .filter((col("rodzaj").equalTo("SAMOCHÓD OSOBOWY")
                        .and(col("data_wyrejestrowania").isNull()))
                        .and((col("pochodzenie").like("UŻYW%")
                                .or(col("pochodzenie").like("NOWY%"))))));

        samochody_osobowe = samochody_osobowe.unionByName(cepikData.getWojewodztwo_warminskomazurskie()
                .filter((col("rodzaj").equalTo("SAMOCHÓD OSOBOWY")
                        .and(col("data_wyrejestrowania").isNull()))
                        .and((col("pochodzenie").like("UŻYW%")
                                .or(col("pochodzenie").like("NOWY%"))))));

        samochody_osobowe = samochody_osobowe.unionByName(cepikData.getWojewodztwo_wielkopolskie()
                .filter((col("rodzaj").equalTo("SAMOCHÓD OSOBOWY")
                        .and(col("data_wyrejestrowania").isNull()))
                        .and((col("pochodzenie").like("UŻYW%")
                                .or(col("pochodzenie").like("NOWY%"))))));

        samochody_osobowe = samochody_osobowe.unionByName(cepikData.getWojewodztwo_zachodniopomorskie()
                .filter((col("rodzaj").equalTo("SAMOCHÓD OSOBOWY")
                        .and(col("data_wyrejestrowania").isNull()))
                        .and((col("pochodzenie").like("UŻYW%")
                                .or(col("pochodzenie").like("NOWY%"))))));

        samochody_osobowe = samochody_osobowe.unionByName(cepikData.getNieokreslone_wojewodztwo()
                .filter((col("rodzaj").equalTo("SAMOCHÓD OSOBOWY")
                        .and(col("data_wyrejestrowania").isNull()))
                        .and((col("pochodzenie").like("UŻYW%")
                                .or(col("pochodzenie").like("NOWY%"))))));

        so_marka_model_pochodzenie = samochody_osobowe
                .filter(col("marka").isNotNull()
                        .and(col("model").isNotNull()));
    }

    public void car_origin_in_poland() {
        List<Row> so_pochodzenie = samochody_osobowe
                .groupBy("pochodzenie").count().sort(col("count").desc())
                .collectAsList();

        DefaultPieDataset dpd = new DefaultPieDataset();

        for(Row row : so_pochodzenie) {
            if(row.get(0).equals("NOWY IMPORT INDYW"))
                dpd.setValue("NOWY IMPORTOWANY", (long)row.get(1));
            else if(row.get(0).equals("UŻYW. IMPORT INDYW"))
                dpd.setValue("UŻYW. IMPORTOWANY", (long)row.get(1));
            else {
                dpd.setValue((String)row.get(0), (long)row.get(1));
            }
        }

        JFreeChart chart = ChartFactory.createPieChart(
                "Pochodzenie i stan rejestrowanych samochodów osobowych w Polsce",
                dpd,
                true,
                true,
                false
        );

        PiePlot plot = (PiePlot) chart.getPlot();
        plot.setSectionPaint("UŻYW. IMPORTOWANY", Color.BLACK);
        plot.setSectionPaint("UŻYW. ZAKUPIONY W KRAJU", Color.decode("#0096ff"));
        plot.setSectionPaint("NOWY ZAKUPIONY W KRAJU", Color.YELLOW);
        plot.setSectionPaint("NOWY IMPORTOWANY", Color.RED);
        plot.setForegroundAlpha(0.5f);
        plot.setExplodePercent("UŻYW. ZAKUPIONY W KRAJU", 0.20);
        plot.setBackgroundPaint(Color.WHITE);
        plot.setSimpleLabels(true);

        PieSectionLabelGenerator gen = new StandardPieSectionLabelGenerator(
                "{0}: {1} ({2})", new DecimalFormat("0"), new DecimalFormat("0.00%"));
        plot.setLabelGenerator(gen);

        try {
            ChartUtilities.saveChartAsPNG(new File("F:\\II stopień\\Semestr IV\\Praca magisterska\\Analiza danych\\pochodzenie_stan_so.png"),
                    chart, 500, 400);
        } catch (Exception e) {
            System.out.println("Wystapil problem podczas tworzenia wykresu");
        }

        JFrame frame = new JFrame("Pochodzenie i stan rejestrowanych samochodów osobowych w Polsce");
        frame.setLayout(new FlowLayout());
        frame.getContentPane().add(new ChartPanel(chart));
        frame.pack();
        frame.setVisible(true);
    }

    public void most_popular_car_model_new_bought_in_Poland() {
        List<Row> nowe_kraj = so_marka_model_pochodzenie.filter(col("pochodzenie").equalTo("NOWY ZAKUPIONY W KRAJU"))
                .groupBy("marka", "model").count().sort(col("count").desc())
                .limit(10).collectAsList();

        String[][] data_tabela = new String[10][3];

        String[] data_kolumny = { "LP.", "MARKA I MODEL", "ILOŚĆ" };

        int i = 0;
        for(Row row : nowe_kraj) {
            data_tabela[i][0] = (i+1) + "";
            data_tabela[i][1] = row.get(0) + " " + row.get(1);
            data_tabela[i][2] = row.get(2) + "";
            i++;
        }

        JFrame frame = new JFrame("Dziesięć najpopularniejszych modeli nowych samochodów osobowych kupowanych w kraju");
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
        frame.setSize(600, 400);
    }

    public void most_popular_car_model_used_imported_to_Poland() {
        List<Row> uzywane_import = so_marka_model_pochodzenie.filter(col("pochodzenie").equalTo("UŻYW. IMPORT INDYW"))
                .groupBy("marka", "model").count().sort(col("count").desc())
                .limit(10).collectAsList();

        String[][] data_tabela = new String[10][3];

        String[] data_kolumny = { "LP.", "MARKA I MODEL", "ILOŚĆ" };

        int i = 0;
        for(Row row : uzywane_import) {
            data_tabela[i][0] = (i+1) + "";
            data_tabela[i][1] = row.get(0) + " " + row.get(1);
            data_tabela[i][2] = row.get(2) + "";
            i++;
        }

        JFrame frame = new JFrame("Dziesięć najpopularniejszych modeli używanych samochodów osobowych sprowadzanych z zagranicy");
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
        frame.setSize(600, 400);
    }

}
