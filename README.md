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
