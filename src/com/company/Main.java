package com.company;

import org.apache.spark.sql.SparkSession;

import java.util.Scanner;

import static org.apache.spark.sql.functions.col;

public class Main {
    private static final SparkSession spark = SparkSession
            .builder()
            .appName("CEPiK Big Data Analysis with Apache Spark")
            .config("spark.master", "local")
            .config("spark.driver.memory", "10g")
            .getOrCreate();
    private static final CepikData cepikData = new CepikData(spark);

    public static void main(String[] args) {
        System.out.println("*******************************************************");
        System.out.println("*         CEPiK BIG DATA ANALYSIS APPLICATION         *");
        System.out.println("*******************************************************");

        int choice = menu();

        while(choice != 0) {
            switch (choice) {
                // Info o ilosci wszystkich pojazdow w bazie, pojazdow aktualnie zarejestrowanych
                // oraz pojazdow wyrejestrowanych
                case 1: {
                    System.out.println("PROSZĘ CZEKAĆ, TO MOŻE POTRWAĆ KILKADZIESIĄT MINUT!");
                    //NumberOfVehicles number_of_vehicles = new NumberOfVehicles(spark);
                    NumberOfVehicles number_of_vehicles = new NumberOfVehicles(cepikData);
                    // ilosc pojazdow w bazie
                    number_of_vehicles.info_about_all_registered_vehicles_in_poland();
                    // typy pojazdow
                    number_of_vehicles.info_about_types_of_vehicles_in_poland();
                    break;
                }
                // badanie popularnosci danego rodzaju paliwa w Polsce z podziałem na województwa
                case 2: {
                    System.out.println("PROSZĘ CZEKAĆ, TO MOŻE POTRWAĆ KILKADZIESIĄT MINUT!");
                    //FuelPopularitySurvey fuelPopularitySurvey = new FuelPopularitySurvey(spark);
                    FuelPopularitySurvey fuelPopularitySurvey = new FuelPopularitySurvey(cepikData);
                    // 1. wojewodztwo lubelskie
                    fuelPopularitySurvey.popularity_of_fuels_woj_lubelskie();
                    // 2. wojewodztwo dolnoslaskie
                    fuelPopularitySurvey.popularity_of_fuels_woj_dolnoslaskie();
                    // 3. wojewodztwo kujawsko-pomorskie
                    fuelPopularitySurvey.popularity_of_fuels_woj_kujawskopomorskie();
                    // 4. wojewodztwo podlaskie
                    fuelPopularitySurvey.popularity_of_fuels_woj_podlaskie();
                    // 5. wojewodztwo lubuskie
                    fuelPopularitySurvey.popularity_of_fuels_woj_lubuskie();
                    // 6. wojewodztwo mazowieckie
                    fuelPopularitySurvey.popularity_of_fuels_woj_mazowieckie();
                    // 7. wojewodztwo lodzkie
                    fuelPopularitySurvey.popularity_of_fuels_woj_lodzkie();
                    // 8. wojewodztwo malopolskie
                    fuelPopularitySurvey.popularity_of_fuels_woj_maloplskie();
                    // 9. wojewodztwo opolskie
                    fuelPopularitySurvey.popularity_of_fuels_woj_opolskie();
                    // 10. wojewodztwo podkarpackie
                    fuelPopularitySurvey.popularity_of_fuels_woj_podkarpackie();
                    // 11. wojewodztwo pomorskie
                    fuelPopularitySurvey.popularity_of_fuels_woj_pomorskie();
                    // 12. wojewodztwo slaskie
                    fuelPopularitySurvey.popularity_of_fuels_woj_slaskie();
                    // 13. wojewodztwo swietokrzyskie
                    fuelPopularitySurvey.popularity_of_fuels_woj_swietokrzyskie();
                    // 14. wojewodztwo warminsko-mazurskie
                    fuelPopularitySurvey.popularity_of_fuels_woj_warminskomazurskie();
                    // 15. wojewodztwo wielkopolskie
                    fuelPopularitySurvey.popularity_of_fuels_woj_wielkopolskie();
                    // 16. wojewodztwo zachodniopomorskie
                    fuelPopularitySurvey.popularity_of_fuels_woj_zachodniopomorskie();
                    // 17. Polska ogółem
                    fuelPopularitySurvey.popularity_of_fuels_poland();
                    break;
                }
                // badanie popularnosci samochodow z rocznika do 2005, 2005 - 2015 oraz 2015 - 2019
                // dołączajac popularnosc danego rodzaju paliwa w tym rocznika
                // pod katem zamoznosci Polakow z podziałem na województwa
                case 3: {
                    System.out.println("PROSZĘ CZEKAĆ, TO MOŻE POTRWAĆ KILKADZIESIĄT MINUT!");
                    //VehicleVintagesSurvey vehicleVintagesSurvey = new VehicleVintagesSurvey(spark);
                    VehicleVintagesSurvey vehicleVintagesSurvey = new VehicleVintagesSurvey(cepikData);
                    // 1. wojewodztwo lubelskie
                    vehicleVintagesSurvey.popularity_of_vintages_of_vehicles_and_fuels_woj_lubelskie();
                    // 2. wojewodztwo dolnoslaskie
                    vehicleVintagesSurvey.popularity_of_vintages_of_vehicles_and_fuels_woj_dolnoslaskie();
                    // 3. wojewodztwo kujawsko-pomorskie
                    vehicleVintagesSurvey.popularity_of_vintages_of_vehicles_and_fuels_woj_kujawskopomorskie();
                    // 4. wojewodztwo lubuskie
                    vehicleVintagesSurvey.popularity_of_vintages_of_vehicles_and_fuels_woj_lubuskie();
                    // 5. wojewodztwo lodzkie
                    vehicleVintagesSurvey.popularity_of_vintages_of_vehicles_and_fuels_woj_lodzkie();
                    // 6. wojewodztwo malopolskie
                    vehicleVintagesSurvey.popularity_of_vintages_of_vehicles_and_fuels_woj_malopolskie();
                    // 7. wojewodztwo mazowieckie
                    vehicleVintagesSurvey.popularity_of_vintages_of_vehicles_and_fuels_woj_mazowieckie();
                    // 8. wojewodztwo opolskie
                    vehicleVintagesSurvey.popularity_of_vintages_of_vehicles_and_fuels_woj_opolskie();
                    // 9. wojewodztwo podkarpackie
                    vehicleVintagesSurvey.popularity_of_vintages_of_vehicles_and_fuels_woj_podkarpackie();
                    // 10. wojewodztwo podlaskie
                    vehicleVintagesSurvey.popularity_of_vintages_of_vehicles_and_fuels_woj_podlaskie();
                    // 11. wojewodztwo pomorskie
                    vehicleVintagesSurvey.popularity_of_vintages_of_vehicles_and_fuels_woj_pomorskie();
                    // 12. wojewodztwo slaskie
                    vehicleVintagesSurvey.popularity_of_vintages_of_vehicles_and_fuels_woj_slaskie();
                    // 13. wojewodztwo swietokrzyskie
                    vehicleVintagesSurvey.popularity_of_vintages_of_vehicles_and_fuels_woj_swietokrzyskie();
                    // 14. wojewodztwo warminsko-mazurskie
                    vehicleVintagesSurvey.popularity_of_vintages_of_vehicles_and_fuels_woj_warminskomazurskie();
                    // 15. wojewodztwo wielkopolskie
                    vehicleVintagesSurvey.popularity_of_vintages_of_vehicles_and_fuels_woj_wielkopolskie();
                    // 16. wojewodztwo zachodniopomorskie
                    vehicleVintagesSurvey.popularity_of_vintages_of_vehicles_and_fuels_woj_zachodniopomorskie();
                    // 17. Polska ogółem (na razie TYLKO zliczanie)
                    vehicleVintagesSurvey.popularity_of_vintages_of_vehicles_and_fuels_poland();
                    break;
                }
                // badanie popularnosci samochodow pod katem pojemnosci i mocy silnika w Polsce
                case 4: {
                    System.out.println("PROSZĘ CZEKAĆ, TO MOŻE POTRWAĆ KILKADZIESIĄT MINUT!");
                    EngineCapacityAndPowerPopularitySurvey engineCapacityAndPowerPopularitySurvey = new EngineCapacityAndPowerPopularitySurvey(cepikData);
                    // pojemnosc silnika
                    engineCapacityAndPowerPopularitySurvey.popularity_of_engine_capacity_in_poland();
                    // moc silnika
                    engineCapacityAndPowerPopularitySurvey.popularity_of_engine_power_in_poland();
                    break;
                }
                // Badanie średniej pojemności i mocy silnika w samochodach osobowych
                // na przestrzeni lat 2000 - 2018 (co dwa lata) w Polsce
                case 5: {
                    System.out.println("PROSZĘ CZEKAĆ, TO MOŻE POTRWAĆ KILKADZIESIĄT MINUT!");
                    EngineCapacityAndPowerAverageOverTheYearsSurvey engineCapacityAndPowerAverageOverTheYearsSurvey
                            = new EngineCapacityAndPowerAverageOverTheYearsSurvey(cepikData);
                    engineCapacityAndPowerAverageOverTheYearsSurvey.analyse();
                    break;
                }
                // badanie popularnosci marek samochodow osobowych w Polsce z podziałem na województwa
                // + powiat radzynski
                case 6 : {
                    System.out.println("PROSZĘ CZEKAĆ, TO MOŻE POTRWAĆ KILKADZIESIĄT MINUT!");
                    CarBrandPopularitySurvey carBrandPopularitySurvey = new CarBrandPopularitySurvey(cepikData);
                    // 1. wojewodztwo lubelskie
                    carBrandPopularitySurvey.popularity_of_car_brand_woj_lubelskie();
                    // 2. wojewodztwo dolnoslaskie
                    carBrandPopularitySurvey.popularity_of_car_brand_woj_dolnoslaskie();
                    // 3. wojewodztwo kujawsko-pomorskie
                    carBrandPopularitySurvey.popularity_of_car_brand_woj_kujawskopomorskie();
                    // 4. wojewodztwo lubuskie
                    carBrandPopularitySurvey.popularity_of_car_brand_woj_lubuskie();
                    // 5. wojewodztwo lodzkie
                    carBrandPopularitySurvey.popularity_of_car_brand_woj_lodzkie();
                    // 6. wojewodztwo malopolskie
                    carBrandPopularitySurvey.popularity_of_car_brand_woj_malopolskie();
                    // 7. wojewodztwo mazowieckie
                    carBrandPopularitySurvey.popularity_of_car_brand_woj_mazowieckie();
                    // 8. wojewodztwo opolskie
                    carBrandPopularitySurvey.popularity_of_car_brand_woj_opolskie();
                    // 9. wojewodztwo podkarpackie
                    carBrandPopularitySurvey.popularity_of_car_brand_woj_podkarpackie();
                    // 10. wojewodztwo podlaskie
                    carBrandPopularitySurvey.popularity_of_car_brand_woj_podlaskie();
                    // 11. wojewodztwo pomorskie
                    carBrandPopularitySurvey.popularity_of_car_brand_woj_pomorskie();
                    // 12. wojewodztwo slaskie
                    carBrandPopularitySurvey.popularity_of_car_brand_woj_slaskie();
                    // 13. wojewodztwo swietokrzyskie
                    carBrandPopularitySurvey.popularity_of_car_brand_woj_swietokrzyskie();
                    // 14. wojewodztwo warminsko-mazurskie
                    carBrandPopularitySurvey.popularity_of_car_brand_woj_warminskomazurskie();
                    // 15. wojewodztwo wielkopolskie
                    carBrandPopularitySurvey.popularity_of_car_brand_woj_wielkopolskie();
                    // 16. wojewodztwo zachodniopomorskie
                    carBrandPopularitySurvey.popularity_of_car_brand_woj_zachodniopomorskie();
                    // 17. Polska ogółem (na razie zliczanie)
                    carBrandPopularitySurvey.popularity_of_car_brand_in_poland();
                    // 18. EXTRA: powiat radzyński
                    carBrandPopularitySurvey.popularity_of_car_brand_powiat_radzynski();
                    break;
                }
                // badanie popularnosci modeli samochodow osobowych w Polsce + powiat radzynski
                case 7 : {
                    System.out.println("PROSZĘ CZEKAĆ, TO MOŻE POTRWAĆ KILKADZIESIĄT MINUT!");
                    CarModelPopularitySurvey carModelPopularitySurvey = new CarModelPopularitySurvey(cepikData);
                    // Polska
                    carModelPopularitySurvey.popularity_of_car_models_in_poland();
                    // EXTRA: powiat radzyński
                    carModelPopularitySurvey.popularity_of_car_models_powiat_radzynski();
                    break;
                }
                // badanie zmiany trendu rejestrowanych pojazdów z 8 najpopularniejszych marek w Polsce
                // na przestrzeni lat 2000 - 2018 (testowanie co dwa lata)
                case 8 : {
                    System.out.println("PROSZĘ CZEKAĆ, TO MOŻE POTRWAĆ KILKADZIESIĄT MINUT!");
                    CarRegistrationOverTheYearsSurvey carRegistrationOverTheYearsSurvey = new CarRegistrationOverTheYearsSurvey(cepikData);
                    carRegistrationOverTheYearsSurvey.car_brand_registration_trend_over_the_years();
                    break;
                }
                // Badanie pochodzenia pojazdow rejestrowanych w Polsce pod kątem tego,
                // czy został zakupiony w kraju, czy za granicą, czy był nowy, czy używany, itp.
                case 9 : {
                    System.out.println("PROSZĘ CZEKAĆ, TO MOŻE POTRWAĆ KILKADZIESIĄT MINUT!");
                    CarOriginSurvey carOriginSurvey = new CarOriginSurvey(cepikData);
                    // pochodzenie i stan samochodow osobowych w Polsce
                    carOriginSurvey.car_origin_in_poland();
                    // TOP 10 najpopularniejszych modeli nowych samochodow osobowych zakupionych w Polsce
                    // oraz używanych samochodow osobowych importowanych z zagranicy
                    carOriginSurvey.most_popular_car_model_new_bought_in_Poland();
                    carOriginSurvey.most_popular_car_model_used_imported_to_Poland();
                    break;
                }
                // Klasyfikacja samochodow osobowych na podstawie 'drzewa decyzyjnego'
                // do grup: samochody małe/miejskie, samochody kompaktowe, samochody rodzinne
                // oraz samochody terenowe/sportowe/limuzyny
                case 10: {
                    System.out.println("PROSZĘ CZEKAĆ, TO MOŻE POTRWAĆ KILKADZIESIĄT MINUT!");
                    ClassificationCars classificationCars = new ClassificationCars(cepikData);
                    //classificationCars.classifier();
                    // TOP 10 modeli samochodów osobowych w każdej grupie
                    classificationCars.most_popular_model_in_small_cars();
                    classificationCars.most_popular_model_in_compact_car();
                    classificationCars.most_popular_model_in_family_car();
                    classificationCars.most_popular_model_in_offroad_and_limousine();
                    classificationCars.most_popular_model_in_sport_car();
                    break;
                }
                default: {
                    System.out.println("Zły wybór!");
                    break;
                }
            }
            choice = menu();
        }

        System.out.println("*******************************************************");
        System.out.println("APLIKACJA ZOSTAŁA ZAMKNIĘTA");
        System.exit(0);
    }

    public static int menu() {
        System.out.println("*******************************************************");
        System.out.println("*                        MENU                         *");
        System.out.println("*******************************************************");
        System.out.println("1. Badanie ilości wszystkich rejestrowanych samochodów do 2019 roku " +
                "(podział na pojazdy aktualnie zarejestrowane i wyrejestrowane) " +
                "oraz badanie popularności pojazdów pod kątem rodzaju");
        System.out.println("2. Badanie popularności rodzajów paliw w samochodach osobowych w Polsce z podziałem na województwa");
        System.out.println("3. Badanie popularności samochodów osobowych ze starszych roczników (do 2003), aut 'średniego wieku' (2004 - 2013) oraz samochodów nowych (od 2014) w Polsce z podziałem na województwa");
        System.out.println("4. Badanie popularności samochodów osobowych pod kątem pojemności i mocy silnika - w Polsce");
        System.out.println("5. Badanie średniej pojemności i mocy silnika w samochodach osobowych na przestrzeni lat 2000 - 2018 (co dwa lata) w Polsce");
        System.out.println("6. Badanie popularności marek samochodów osobowych w Polsce z podziałem na województwa oraz z wyszczególnieniem powiatu radzyńskiego");
        System.out.println("7. Badanie popularności modeli samochodów osobowych w Polsce z wyszczególnieniem powiatu radzyńskiego");
        // TODO TEST \/
        System.out.println("8. Badanie zmiany trendu rejestrowanych samochodów osobowych najpopularniejszych marek w Polsce na przestrzeni lat 2000 - 2018 (ilość rejestrowanych pojazdów badana co dwa lata)");
        System.out.println("9. Badanie pochodzenia i stanu samochodów osobowych zarejestrowanych w Polsce oraz badanie popularności modeli samochodów osobowych, ktore kupowane są w kraju jako nowe oraz używanych, które są sprowadzane z zagranicy");
        System.out.println("10. Klasyfikacja samochodów osobowych na samochody małe/miejskie, kompaktowe, rodzinne oraz terenowe/sportowe/limuzyny");
        System.out.println("0. Zakończ");

        Scanner sc = new Scanner(System.in);

        System.out.print("Twój wybór: ");
        return sc.nextInt();
    }
}
