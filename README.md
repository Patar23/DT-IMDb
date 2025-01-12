# ETL proces pre IMDb dataset.

Účelom tohto repozitáru je implementácia ETL procesu v Snowflake na analýzu dát z IMDb datasetu.

---
## 1. Úvod a popis zdrojových dát
Cieľom semestrálneho projektu je analyzovať dáta týkajúce sa filmov, hercov, režisérov a ich hodnotení. Táto analýza umožňuje identifikovať trendy v obľúbenosti filmov, najúspešnejšie snímky a názor divákov.
Zdrojové dáta sú čerpané z nasledujúceho [datasetu](https://github.com/AntaraChat/SQL---IMDb-Movie-Analysis/tree/main). Dataset obsahuje 6 hlavných tabuliek:
- `movie`
- `ratings`
- `genre`
- `director_mapping`
- `role_mapping`
- `names`

Účelom ETL procesu bolo tieto dáta pripraviť, transformovať a sprístupniť pre viacdimenzionálnu analýzu.

---

### 1.1 Dátová architektúra

### ERD diagram
Surové dáta sú organizované v rámci relačného modelu, ktorý je znázornený na diagrame entít a relácií (ERD):

<p align="center">
  <img src="https://github.com/Patar23/DT-IMDb/blob/main/erd_schema.png" alt="ERD Schema">
  <br>
  <em>Obrázok 1 Entitno-relačná schéma IMDb datasetu</em>
</p>

---
## **2 Dimenzionálny model**

Navrhnutý bol hviezdicový model, pre efektívnu analýzu kde centrálny bod predstavuje faktová tabuľka **`fact_movies`**, ktorá je prepojená s dimenziami:
- **`dim_movies`**:
- **`dim_names`**: 
- **`dim_directors`**:
- **`dim_genres`**: 
- **`dim_roles`**:

Štruktúra hviezdicového modelu je znázornená na diagrame nižšie. Diagram ukazuje prepojenia medzi faktovou tabuľkou a dimenziami, čo zjednodušuje pochopenie a implementáciu modelu.

<p align="center">
  <img src="https://github.com/Patar23/DT-IMDb/blob/main/star_schema.PNG" alt="Star Schema">
  <br>
  <em>Obrázok 2 Hviezdicová schéma IMDb datasetu</em>
</p>

---
## **3. ETL proces v Snowflake**
ETL proces pozostával z troch hlavných fáz: `extrahovanie` (Extract), `transformácia` (Transform) a `načítanie` (Load). Tento proces bol implementovaný v Snowflake s cieľom pripraviť zdrojové dáta zo staging vrstvy do viacdimenzionálneho modelu vhodného na analýzu a vizualizáciu.

---
### **3.1 Extract (Extrahovanie dát)**
Dáta zo zdrojového datasetu (formát `.csv`) boli najprv nahraté do Snowflake prostredníctvom interného stage úložiska s názvom `FLAMINGO_STAGE`. Stage v Snowflake slúži ako dočasné úložisko na import alebo export dát. Vytvorenie stage bolo zabezpečené príkazom:

#### Príklad kódu:
```sql
CREATE OR REPLACE STAGE FLAMINGO_STAGE;
```
Do stage boli následne nahraté súbory obsahujúce údaje o filmoch. Dáta boli importované do staging tabuliek pomocou príkazu `COPY INTO`. Pre každú tabuľku sa použil podobný príkaz:

```sql
COPY INTO movie_staging
FROM @FLAMINGO_STAGE/movie.csv
FILE_FORMAT = (TYPE = 'CSV' FIELD_OPTIONALLY_ENCLOSED_BY = '"' SKIP_HEADER = 1)
ON_ERROR = 'CONTINUE';
```

V prípade nekonzistentných záznamov bol použitý parameter `ON_ERROR = 'CONTINUE'`, ktorý zabezpečil pokračovanie procesu bez prerušenia pri chybách.

---
### **3.1 Transfor (Transformácia dát)**

V tejto fáze prebehlo čistenie, transformácia a obohatenie dát zo staging tabuliek. Cieľom bolo pripraviť dimenzionálne tabuľky a faktovú tabuľku, ktoré zabezpečia efektívnu a jednoduchú analýzu.

Dimenzia `dim_movies` obsahuje základné informácie o filmoch, ako sú názov, rok natočenia, dátum vydania, dĺžka filmu, krajina pôvodu, jazyky a produkčná spoločnosť. Tieto údaje sú nevyhnutné pre analýzu filmov z rôznych aspektov.
```sql
CREATE TABLE dim_movies AS
SELECT DISTINCT
    id AS dim_movieId,         
    title,                           
    year,                           
    TO_DATE(date_published) AS date_published,
    duration,                      
    country,                       
    languages,                    
    production_company         
FROM movie_staging;
```
Dimenzia `dim_genres` obsahuje informácie o žánroch filmov, pričom každý film je spojený s jedným alebo viacerými žánrami. Tento krok transformácie zahŕňa prepojenie medzi tabuľkami filmov a žánrov, aby bolo možné analyzovať, ako rôzne žánre ovplyvňujú hodnotenia a celkový úspech filmov.
```sql
CREATE TABLE dim_genres AS
SELECT DISTINCT
    movie_id AS dim_genreId,        
    genre AS genre_name              
FROM genre_staging;
```

Dimenzia `dim_names` obsahuje údaje o osobách, ktoré sa podieľali na tvorbe filmov, ako sú herci a ďalší kreatívni pracovníci. Zahŕňa informácie o menách, výške, dátume narodenia a filmoch, na ktorých sa podieľali. Tieto dáta poskytujú cenný kontext pre analýzu výkonu jednotlivých mien v rámci filmového priemyslu.
```sql
CREATE TABLE dim_names AS
SELECT DISTINCT
    id AS dim_nameId,            
    name,                           
    height,                          
    TO_DATE(date_of_birth) AS date_of_birth,
    known_for_movies          
FROM names_staging;

```

Dimenzia `dim_directors` obsahuje informácie o režiséroch, ktorí stoja za filmami. Tento krok zahŕňa prepojenie režisérov na ich filmy a poskytuje podrobnosti o ich tvorbe, čo je dôležité pre analýzu úspešnosti filmov podľa konkrétnych režisérov.
```sql
CREATE TABLE dim_directors AS
SELECT DISTINCT
    dm.name_id AS director_id,     
    n.name AS name               
FROM director_mapping_staging dm
JOIN names_staging n
  ON dm.name_id = n.id; 
```

Dimenzia `dim_roles` uchováva informácie o postavách v jednotlivých filmoch, ich názvoch a kategóriách. Tento krok umožňuje analyzovať, aké role sa vyskytujú vo filmoch a ako ich obsadenie ovplyvňuje hodnotenie filmov.
```sql
CREATE TABLE dim_roles AS
SELECT DISTINCT
    rm.name_id AS role_id,        
    n.name AS role_name,          
    category
FROM role_mapping_staging rm
JOIN names_staging n
  ON rm.name_id = n.id;   
```

Faktová tabuľka `fact_movies` obsahuje kľúčové metriky týkajúce sa filmov, ako sú priemerné hodnotenie, počet hlasov a celkový príjem filmu. Táto tabuľka je prepojená s dimenziami, čím poskytuje komplexný pohľad na výkonnosť filmov v rôznych kategóriách (žánre, režiséri, herci a pod.).
```sql
CREATE TABLE fact_movies AS
SELECT 
    m.id AS fact_movie_id,               
    r.avg_rating,                      
    r.total_votes,                        
    m.worlwide_gross_income,               
    g.movie_id AS genre_id,                
    d.name_id AS director_id,              
    m.id AS movie_id,                     
    n.id AS name_id,                       
    ro.name_id AS role_id                  
FROM ratings_staging r
JOIN movie_staging m ON r.movie_id = m.id                      
LEFT JOIN genre_staging g ON m.id = g.movie_id                
LEFT JOIN director_mapping_staging d ON m.id = d.movie_id     
LEFT JOIN role_mapping_staging ro ON m.id = ro.movie_id       
LEFT JOIN names_staging n ON ro.name_id = n.id;             
```
---
### **3.3 Load (Načítanie dát)**

Po úspešnom vytvorení dimenzií a faktovej tabuľky boli dáta nahraté do finálnej štruktúry. Na záver boli staging tabuľky odstránené, aby sa optimalizovalo využitie úložiska:
```sql
DROP TABLE IF EXISTS movie_staging;
DROP TABLE IF EXISTS names_staging;
DROP TABLE IF EXISTS ratings_staging;
DROP TABLE IF EXISTS genre_staging;
DROP TABLE IF EXISTS director_mapping_staging;
DROP TABLE IF EXISTS role_mapping_staging;
```
ETL proces v Snowflake umožnil spracovanie pôvodných dát z `.csv` formátu do viacdimenzionálneho modelu typu hviezda. Tento proces zahŕňal čistenie, obohacovanie a reorganizáciu údajov. Výsledný model umožňuje analýzu čitateľských preferencií a správania používateľov, pričom poskytuje základ pre vizualizácie a reporty.

---
## **4 Vizualizácia dát**

Dashboard obsahuje `6 vizualizácií`, ktoré poskytujú prehľad o kľúčových metrikách a trendoch týkajúcich sa filmov, režisérov, žánrov a hodnotení. Tieto vizualizácie zodpovedajú dôležité otázky a umožňujú lepšie pochopiť preferencie divákov a vzorce ich správania pri hodnotení filmov.

<p align="center">
  <img src="https://github.com/Patar23/DT-IMDb/blob/main/IMDb_dashboard.png" alt="dashboard">
  <br>
  <em>Obrázok 3 Dashboard IMDb datasetu</em>
</p>

---
### **Graf 1: Priemerné hodnotenie filmov podľa žánru**
Popis grafu: Tento bar chart zobrazuje priemerné hodnotenie filmov rozdelených podľa žánrov. Umožňuje analyzovať, ktoré žánre majú najlepšie priemerné hodnotenie, čo môže byť indikátorom kvality filmov v danom žánri a preferencií divákov v súvislosti s hodnotením filmov.

```sql
SELECT g.GENRE_NAME, AVG(f.AVG_RATING) AS AVERAGE_RATING
FROM FACT_MOVIES f
JOIN DIM_GENRES g ON f.GENRE_ID = g.DIM_GENREID
GROUP BY g.GENRE_NAME
ORDER BY AVERAGE_RATING DESC;
```
---
### **Graf 2: Počet filmov v databáze**
Popis grafu: Tento scorecard ukazuje celkový počet filmov v databáze IMDb. Tento údaj poskytuje základný prehľad o množstve filmov zahrnutých v databáze a je užitočný pri hodnotení rozsahu dát v rámci projektu alebo analýzy.

```sql
SELECT COUNT(f.FACT_MOVIE_ID) AS MOVIE_COUNT
FROM FACT_MOVIES f;

```
---
### **Graf 3: Počet filmov podľa žánru v priebehu rokov**
Popis grafu: Tento line chart zobrazuje vývoj počtu filmov v jednotlivých žánroch v priebehu rokov. Pomáha pochopiť, ktoré žánre získali na popularite v rôznych obdobiach a umožňuje vidieť trend v produkcii filmov podľa žánrov v čase.

```sql
SELECT m.YEAR, g.GENRE_NAME, COUNT(f.FACT_MOVIE_ID) AS MOVIE_COUNT
FROM FACT_MOVIES f
JOIN DIM_MOVIES m ON f.MOVIE_ID = m.DIM_MOVIEID
JOIN DIM_GENRES g ON f.GENRE_ID = g.DIM_GENREID
GROUP BY m.YEAR, g.GENRE_NAME
ORDER BY m.YEAR, MOVIE_COUNT DESC;
```
---
### **Počet filmov podľa režiséra a žánru**
Popis grafu: Tento heatgrid chart zobrazuje počet filmov podľa jednotlivých režisérov a ich žánrov. Pomáha identifikovať, ktorí režiséri sa najviac zameriavajú na konkrétne žánre, a poskytuje prehľad o výbere žánrov v rámci režisérskej tvorby.

```sql
SELECT d.NAME AS DIRECTOR_NAME, g.GENRE_NAME, COUNT(f.FACT_MOVIE_ID) AS MOVIE_COUNT
FROM FACT_MOVIES f
JOIN DIM_DIRECTORS d ON f.DIRECTOR_ID = d.DIRECTOR_ID
JOIN DIM_GENRES g ON f.GENRE_ID = g.DIM_GENREID
GROUP BY d.NAME, g.GENRE_NAME
ORDER BY MOVIE_COUNT DESC;
```
---
### **Graf 5: Top 5 filmov s najvyšším počtom hlasov**
Popis grafu: Tento bar chart zobrazuje top 5 filmov s najvyšším počtom hlasov v databáze IMDb. Umožňuje rýchly prehľad o tom, ktoré filmy získali najväčšiu popularitu medzi divákmi a ktoré filmy majú najväčšiu základňu hlasujúcich divákov.

```sql
SELECT m.TITLE, SUM(f.TOTAL_VOTES) AS TOTAL_VOTES
FROM FACT_MOVIES f
JOIN DIM_MOVIES m ON f.MOVIE_ID = m.DIM_MOVIEID
GROUP BY m.TITLE
ORDER BY TOTAL_VOTES DESC
LIMIT 5;
```
---
### **Graf 6: Výška hercov vs. Počet filmov, v ktorých sa objavili**
Popis grafu: Tento scatter chart zobrazuje vzťah medzi výškou hercov a počtom filmov, v ktorých sa objavili. Pomáha analyzovať, či existuje nejaký vzor alebo väzba medzi fyzickými vlastnosťami hercov (ako je výška) a ich početnosťou v databáze filmov.

```sql
SELECT n.HEIGHT, COUNT(f.FACT_MOVIE_ID) AS MOVIE_COUNT
FROM FACT_MOVIES f
JOIN DIM_NAMES n ON f.NAME_ID = n.DIM_NAMEID
GROUP BY n.HEIGHT
ORDER BY n.HEIGHT;
```

Dashboard poskytuje podrobný prehľad o dátach v databáze filmov, pričom odpovedá na kľúčové otázky týkajúce sa preferencií divákov a trendov v oblasti filmového priemyslu. Vizualizácie umožňujú efektívnu analýzu a interpretáciu týchto dát, čo môže pomôcť pri vylepšovaní odporúčacích systémov, marketingových kampaní a správy filmových knižníc.

---

**Autor:** Patrik Vereš
