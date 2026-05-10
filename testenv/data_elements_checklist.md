# Data Elements Checklist

## precip(h/j) [must]
- [ ] inm
- [x] openmeteo
- [ ] noaa

## temp min max(h) [must]
- [ ] inm
- [x] openweather

## humid rel(h) [should]
- [ ] inm
- [x] openweather

## windspeed(h) [must] (can get from openweather or openmeteo)
- [ ] inm
- [ ] noaa

## press atm(h) [could] (can get from openweather or openmeteo)
- [ ] inm

## prev meteo 7j(j) [must]
- [x] openmeteo
- [x] ecmwf

## ray solair(h) [must]
- [x] nasa power
- [x] ecmwf

## prev saison(m) [should]
- [ ] copernicus

## Prix mondiaux cultures(j) [should]
- [ ] fao (found yearly, apparently daily data is only available in tridge)
- [ ] worldbank
- [ ] ~~tridge~~ (require work email not uni but it does have the needed data with the right frequency)

## hist prix(a) [should]
- [ ] fao (found yearly)
- [ ] worldbank

## list cultures(a) [must]  
- [x] fao
- [ ] crda

## risques ravageurs(7j) [should]
- [ ] fao (not found, might need more research)
- [ ] iresa

## Kc(s) [must]
- [x] fao

## prix tun cultur(j) [must]
- [ ] onagri
- [ ] apia

## rendemant moy per crop(a) [must]
- [ ] onagri
- [ ] crda

## cout eau(m) [should]
- [ ] gda
- [x] sonede

## water availability(m) [should]
- [ ] gda
- [ ] crda

## crop calendar(a) [should]
- [ ] crda
- [ ] farmer

## dirt type(a) [must]
- [ ] crda

## water needs per crop(s) [must]
- [x] fao56

## et0(j)(calcul) [must]
- [x] fao56

## ndvi(5j) [could]
- [x] sentinel-2 (google earth engin)

## ndwi(5j) [could]
- [x] esa (google earth engin)

## surface temp(j) [should]
- [ ] modis (although it offers 1km² resolution, it introduces high maintenance cost and cannot be used by airflow, it's best to use nasapower instead with a res of 50km²)

