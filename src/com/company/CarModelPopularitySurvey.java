package com.company;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import javax.swing.*;
import javax.swing.table.TableColumnModel;
import java.awt.*;
import java.util.List;

import static org.apache.spark.sql.functions.col;

public class CarModelPopularitySurvey {
    private CepikData cepikData;
    private Dataset<Row> samochody_osobowe;

    public CarModelPopularitySurvey(CepikData cepikData) {
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

    // Polska
    public void popularity_of_car_models_in_poland() {
       Dataset<Row> modele_dataset = samochody_osobowe
                .filter(col("marka").isNotNull()
                    .and(col("model").isNotNull()))
                .groupBy("marka", "model").count()
                .sort(col("count").desc());

       Dataset<Row> grand_five_dataset = modele_dataset
               .filter((col("marka").equalTo("AUDI")
                        .and(col("model").equalTo("A3")))
               .or((col("marka").equalTo("AUDI")
                       .and(col("model").equalTo("80")))
               .or((col("marka").equalTo("KIA")
                       .and(col("model").equalTo("CARENS")))
               .or((col("marka").equalTo("OPEL")
                       .and(col("model").equalTo("MERIVA")))
               .or((col("marka").equalTo("DAEWOO")
                       .and(col("model").equalTo("TICO"))))))));

        List<Row> modele = modele_dataset.limit(20).collectAsList();

        List<Row> grand_five = grand_five_dataset.collectAsList();

        String[][] data_tabela = new String[26][3];

        String[] data_kolumny = { "LP.", "MARKA I MODEL", "ILOŚĆ" };

        int i = 0;
        for(Row row : modele) {
            data_tabela[i][0] = (i+1) + "";
            data_tabela[i][1] = row.get(0) + " " + row.get(1);
            data_tabela[i][2] = row.get(2) + "";
            i++;
        }

        data_tabela[i][0] = "";
        data_tabela[i][1] = "";
        data_tabela[i][2] = "";
        i++;

        for(Row row : grand_five) {
            if(!modele.contains(row)) {
                data_tabela[i][0] = "";
                data_tabela[i][1] = row.get(0) + " " + row.get(1);
                data_tabela[i][2] = row.get(2) + "";
                i++;
            }
        }

        JFrame frame = new JFrame("Badanie popularności modeli samochodów osobowych w Polsce");
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
        frame.setSize(600, 750);
    }

    // Powiat radzynski
    public void popularity_of_car_models_powiat_radzynski() {
        Dataset<Row> so_pow_radzynski = samochody_osobowe
                .filter(col("akt_miejsce_rej_powiat").equalTo("RADZYŃSKI"));

        Dataset<Row> modele_dataset_radz = so_pow_radzynski
                .filter(col("marka").isNotNull()
                        .and(col("model").isNotNull()))
                .groupBy("marka", "model").count()
                .sort(col("count").desc());

        Dataset<Row> grand_five_dataset_radz = modele_dataset_radz
                .filter((col("marka").equalTo("AUDI")
                        .and(col("model").equalTo("A3")))
                        .or((col("marka").equalTo("AUDI")
                                .and(col("model").equalTo("80")))
                                .or((col("marka").equalTo("KIA")
                                        .and(col("model").equalTo("CARENS")))
                                        .or((col("marka").equalTo("OPEL")
                                                .and(col("model").equalTo("MERIVA")))
                                                .or((col("marka").equalTo("DAEWOO")
                                                        .and(col("model").equalTo("TICO"))))))));

        List<Row> modele = modele_dataset_radz.limit(20).collectAsList();

        List<Row> grand_five = grand_five_dataset_radz.collectAsList();

        String[][] data_tabela = new String[26][3];

        String[] data_kolumny = { "LP.", "MARKA I MODEL", "ILOŚĆ" };

        int i = 0;
        for(Row row : modele) {
            data_tabela[i][0] = (i+1) + "";
            data_tabela[i][1] = row.get(0) + " " + row.get(1);
            data_tabela[i][2] = row.get(2) + "";
            i++;
        }

        data_tabela[i][0] = "";
        data_tabela[i][1] = "";
        data_tabela[i][2] = "";
        i++;

        for(Row row : grand_five) {
            if(!modele.contains(row)) {
                data_tabela[i][0] = "";
                data_tabela[i][1] = row.get(0) + " " + row.get(1);
                data_tabela[i][2] = row.get(2) + "";
                i++;
            }
        }

        JFrame frame = new JFrame("Badanie popularności modeli samochodów osobowych w powiecie radzyńskim");
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
        frame.setSize(600, 750);
    }
}
