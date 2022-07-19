# Databricks notebook source
# MAGIC %md
# MAGIC # Travail de Bachelor en informatique de gestion
# MAGIC ## Plateforme analytique pour l’analyse de l’impact du COVID-19 sur la mobilité
# MAGIC ##### **Sabrina Zine-Elkelma**

# COMMAND ----------

# MAGIC %md
# MAGIC **Étapes du processus :**
# MAGIC 1. [Compréhension métier](attachment:./#Businessunderstanding)
# MAGIC 2. [Compréhension des données](attachment:./#Dataunderstanding)
# MAGIC 3. [Préparation des données](attachment:./#Datapreparation)
# MAGIC 4. [Analyse exploratoire des données](attachment:./#EDA)
# MAGIC 5. [Modélisation](attachment:./#Modelling)
# MAGIC 6. [Evaluation](attachment:./#Evaluation)
# MAGIC 7. [Déploiement](attachment:./#Deployment)
# MAGIC 8. [Bibliographie](attachment:./#Bibliographie)

# COMMAND ----------

# MAGIC %run ../00_initialization/01_lib

# COMMAND ----------

import pandas as pd
import matplotlib.pyplot as plt
import numpy as np
import seaborn as sns
from scipy.stats import chi2
from scipy import stats
import statsmodels.api as sm
import math
from statsmodels.graphics.gofplots import ProbPlot

sns.set_theme(style="whitegrid")

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC -- Configuration de la base de données par défaut
# MAGIC 
# MAGIC use gold_db

# COMMAND ----------

# MAGIC %md
# MAGIC # 1. Compréhension métier  <a class="anchor" id="Businessunderstanding"></a>

# COMMAND ----------

# MAGIC %md
# MAGIC #### Contexte
# MAGIC 
# MAGIC Dans le cadre du travail de Bachelor en informatique de gestion à la HEG-Arc, il a été demandé de créer un PoC d'une plateformae analytique pour anaylser l'impact du COVID-19 sur la mobilité en Suisse. Le but étant d'utiliser des données IoT (Internet of Things : postables, GPS, ...) fournies par Google, Apple ou TomTom afin de les lier avec des fournisseurs de données de la pandémie comme Our Wolrd in Data (OWID).
# MAGIC 
# MAGIC L'Office Fédéral de la Statistique a sorti des statistiques expérimentales sur la mobilité la population durant la pandémie (https://www.experimental.bfs.admin.ch/expstat/fr/home/methodes-innovation/mrmt.assetdetail.19324523.html). Les résultats de cette analyse ont été trouvés grâce à des sondages envoyés à une partie de la population durant l'été 2021. 
# MAGIC 
# MAGIC **Objectifs métiers**
# MAGIC 
# MAGIC Le but principal des résultats de ce projet est de comparer les conclusions obtenues grâce aux sondages de l'OFS et ceux provenant de données IoT. Egalement, une conversation et des visuels pour accompagner l'analyse sont demandés, afin d'éventuellement proposer une autre approches et des suppositions supplémentaires sur ce qui a pu se passer pendant la pandémie en matière de mobilité.
# MAGIC 
# MAGIC #### Contraintes, coûts et bénéfices
# MAGIC Le travail se réalise sous la direction du directeur de travail (Lev Kiwi) et avec l'aide d'un assistant pour les questions techniques ou administratives (Melvyn Vogelsang). Il se réalise en 14 semaines par l'étudiante seule du 4 avril au 18 juillet 2022.
# MAGIC 
# MAGIC Il n'y a pas de réel coût financier pour la réalisation de se travail étant donné le cadre académique, mais il pourrait présenter quelques bénéfices s'il était terminé et mis en production par la Confédération, pour éventuellement prédire les habitudes de la population suisse dans un contexte de pandémie. En effet, pour les transports publics par exemple, les offres pourraient être adaptées.
# MAGIC 
# MAGIC #### Résultats souhaités
# MAGIC Il serait intéressant de voir à quel point l'analyse de l'OFS faite à l'aide de sondages est similaire ou non à une analyse faite avec des sources de données différentes. Le but serait également de tenter de ressortir quelque chose de plus, de découvrir d'autres éléments pas mentionner dans ces résultats comme par exemple des aspects psycologiques sur le comportement en matière de mobilité de la population vis à vis de chiffres du COVID-19. En résumé, c'est la découverte de nouveaux éléments grâce à des types de données pas encore pleinement exploitées.

# COMMAND ----------

# MAGIC %md
# MAGIC # 2. Compréhension des données <a class="anchor" id="Dataunderstanding"></a>

# COMMAND ----------

# Récupération de toutes les données en dataframe pour les analyses

full_df = sqlContext.sql("select fk_geography as region, fk_index as index, value, id as date_int, date, vague, homeworking, school_closed, resto_closed, groceries_closed, covid_pass_active from factindex i join dimCalendar c on fk_calendar = id order by region, index, date_int").toPandas()


#  Séléction des données nationnales uniquement

df = full_df[full_df['region'] == 'CH']
df = df.drop('region', axis=1)
 
    
# Création des datasets pour chaque index afin de faciliter les manipulations

df_work = df.loc[df['index'] == 'workplaces']
df_retail = df.loc[df['index'] == 'reatail_and_recreation']
df_grocery = df.loc[df['index'] == 'grocery_and_pharmacy']
df_park = df.loc[df['index'] == 'parks']
df_resid = df.loc[df['index'] == 'residential']
df_driving = df.loc[df['index'] == 'driving']
df_walking = df.loc[df['index'] == 'walking']
df_transit = df.loc[df['index'] == 'transit']
df_cong = df.loc[df['index'] == 'congestion']
df_stringency = df.loc[df['index'] == 'stringency_index']
df_repr_rate = df.loc[df['index'] == 'reproduction_rate']
df_new_cases = df.loc[df['index'] == 'new_cases']
df_tot_cases = df.loc[df['index'] == 'total_cases']
df_new_deaths = df.loc[df['index'] == 'new_deaths']
df_tot_deaths = df.loc[df['index'] == 'total_deaths']
df_hosp = df.loc[df['index'] == 'hosp_patients']
df_icu = df.loc[df['index'] == 'icu_patients']
df_tot_vac = df.loc[df['index'] == 'total_vaccinations']
df_vac = df.loc[df['index'] == 'people_vaccinated']
df_full_vac = df.loc[df['index'] == 'people_fully_vaccinated']

# COMMAND ----------

# MAGIC %md
# MAGIC #### Description des données
# MAGIC 
# MAGIC Comme les données provienne du modèle final, aucune analyse sur la qualité n'est effectuée après coup. En effet, cela s'est fait au préalable, lors de la récupération des données brutes et de leur transformation.
# MAGIC 
# MAGIC Ce tableau résume toutes les variables gardées dans le modèle gold provenant des différents datasets :
# MAGIC 
# MAGIC |N° | Table | Variable | Description | Type |
# MAGIC |:-----------|:-----------|:-------------------|:---------------------|:----------------|
# MAGIC |0  |dimCalendar | id             | clé primaire de la table (date en Integer)  | qualitative ordinale |
# MAGIC |1  |dimCalendar | date | date du jour  | qualitative ordinale |
# MAGIC |2  |dimCalendar | day_of_week           | jour de semaine   | qualitative ordinale |
# MAGIC |2  |dimCalendar | day           | numéro du jour   | qualitative ordinale |
# MAGIC |2  |dimCalendar | month            | numéro du mois   | qualitative ordinale |
# MAGIC |3  |dimCalendar | year             | année   | qualitative ordinale |
# MAGIC |4  |dimCalendar | vague           | numéro de la vague de la pandémie   | qualitative ordinale |
# MAGIC |5  |dimCalendar | homeworking             | télétravail   | qualitative dichotomique |
# MAGIC |6  |dimCalendar | school_closed             | fermeture des écoles   | qualitative dichotomique |
# MAGIC |7  |dimCalendar | resto_closed             | fermeture des restaurants   | qualitative dichotomique|
# MAGIC |8  |dimCalendar | groceries_closed             | fermture des magasins   | qualitative dichotomique |
# MAGIC |9  |dimCalendar | covid_pass_active             | pass covid   | qualitative dichotomique |
# MAGIC |10 |dimIndex | id             | clé primaire de la table (nom de l'index)  | qualitative nominale |
# MAGIC |11 |dimGeography | id             | clé primaire de la table (abréviation du canton ou CH pour la Suisse)  | qualitative nominale |
# MAGIC |12 |dimGeography | région           | nom du cangon ou Suisse   | qualitative nominale |
# MAGIC |13 |dimGeography | most_populate_city             | ville la plus peuplée du canton ou NA si non référencée   | qualitative nominale |
# MAGIC |14 |factIndex | fk_calendar            | clé étrangère sur dimCalendar   | qualitative ordinale |
# MAGIC |15 |dimCalendar | fk_index             | clé étrangère sur dimIndex | qualitative nominale |
# MAGIC |16 |dimCalendar | fk_geography         | clé étrangère sur dimGeography   | qualitative nominale |
# MAGIC |17 |dimCalendar | value             | valeur de l'index correspondant au fait   | - |
# MAGIC 
# MAGIC 
# MAGIC Ce tableau résume la liste des index :
# MAGIC 
# MAGIC |N° | Index | Description | Dataset de provenance | 
# MAGIC |:-----------|:-----------|:-------------------|:---------------------|
# MAGIC |0  |driving | taux de déplacement en voiture    | Apple   | 
# MAGIC |1  |walking | taux de déplacement à pied   | Apple  | 
# MAGIC |2  |transit | taux de déplacement en transports publics    | Apple + Google  | 
# MAGIC |3  |workplaces | taux de fréquentation des lieux de travail    | Google  | 
# MAGIC |4  |residential | taux de fréquentation des lieux de résidence    | Google  | 
# MAGIC |5  |parks | taux de fréquentation des parcs  | Google  | 
# MAGIC |6  |grocery_and_pharmacy | taux de fréquentation des magasin de première nécessité et des pharmacies  | Google  | 
# MAGIC |7  |retail_and_recreation | taux de fréquentation des autres magasins et des lieux de loisirs   | Google | 
# MAGIC |8  |congestion | indice de congestion du trafic   | TomTom  | 
# MAGIC |9  |stringency_index | indice de dureté des mesures prises par la Confédération  | OWID  | 
# MAGIC |10 |reproduction_rate | taux de reproduction du virus   | OWID  | 
# MAGIC |11 |total_cases | nombre total de cas    | OWID  | 
# MAGIC |12 |new_cases | nombre de nouveaux cas    | OWID  | 
# MAGIC |13 |total_deaths | nombre total de décès   | OWID  | 
# MAGIC |14  |new_deaths | nombre de  nouveaux décès | OWID  | 
# MAGIC |15  |hosp_patients | nombre de patients hospitalisés à cause du COVID-19   | OWID  | 
# MAGIC |16  |icu_patients | nombre de patients aux soins intensifs à cause du COVID-19   | OWID  | 
# MAGIC |17  |total_vaccinations | nombre total de vaccinations| OWID  | 
# MAGIC |18  |people_vaccinated | nombre de personnes ayant reçu au moins une dose    | OWID  | 
# MAGIC |19  |people_fully_vaccinated | nombre de personne totalement vaccinées     | OWID  | 
# MAGIC 
# MAGIC Le format des index de mobilité, le taux de reproduction du virus et le stringency sont donnés en pourcentage et celui des données du COVID-19 en nombre entier. Concernant les données de mobilité, 
# MAGIC 
# MAGIC Que ce soit pour les variables ou les index, pas tous ne sont forcément utilisés. Cependant, certains ont été gardés, car jugés pertinents avant les analyses et pourraient être utilisés dans une itération future.
# MAGIC 
# MAGIC Les données de mobilité récupérées ne représentent évidemment pas toute la population. À savoir que les parts de marchés d'Apple en Suisse sont de 52% et Google 46% selon Statcounter (https://gs.statcounter.com/os-market-share/mobile/switzerland/#yearly-2019-2022-bar). Il est également important de noter que ce sont des chiffres basés sur une habitude enregitrée par les utilisateurs de ces appreils. Par exemple, si Apple a enregitré un certain taux de déplacement en transports publics à un certain jour de l'année pendant quelques temps, une moyenne est effectées et celle-ci est utilisée comme base de comparaison (appelée baseline) pour les enregistrements lors de la pandémie.

# COMMAND ----------

# MAGIC %md
# MAGIC #### Premières analyses
# MAGIC 
# MAGIC **Taux de fréquentation**
# MAGIC 
# MAGIC Concernant la première vague en mars 2020, on peut observer une grande diminution des fréquentation des lieux de loisirs et une baisse un peu moins conséquente sur les lieux de travail. Ce qui peut sembler normal étant donné la fermture de ces lieux, mais l'obligation de travailler en présentiel pour certains secteurs. Par logique, une augmentation de fréquentation des lieux de résidence est remarquée vu le semi-confinement. Concernant les magasin de première nécessité, la fréquentation est plutôt stable, certains pics sont enregistrés mais pas forcément explicable à première vue. La fréquentation des parcs quant à elle est très variée, beaucoup de pics sont enregitrés, peut-être en rapport avec la météo, mais de manière générale durant toute la pandémie, elle a largement augmenté durant les saisons chaudes.
# MAGIC 
# MAGIC Pour les autres vagues, les mêmes observations sont remarquées mais avec un impact moins élevé et en diminuant plus on avance dans le temps. Cela peut être expliqué par le fait que la population s'est habituée ou que les mesures sont devenues moins strictes dans le temps.

# COMMAND ----------

f = plt.figure()
f.set_figwidth(30)
f.set_figheight(12)
plt.plot(df_work['date'], df_work['value'].astype(float), label = "Workplaces")
plt.plot(df_retail['date'], df_retail['value'].astype(float), label = "Retail and recreation")
plt.plot(df_grocery['date'], df_grocery['value'].astype(float), label = "Grocery and pharmacy")
plt.plot(df_park['date'], df_park['value'].astype(float), label = "Parks")
plt.plot(df_resid['date'], df_resid['value'].astype(float), label = "Residential")
plt.title('Mobility data')
plt.xlabel('Date')
plt.ylabel('Fréquentation')
plt.legend()
plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC **Taux de déplacement**
# MAGIC 
# MAGIC Pendant la première vague en mars 2020, on peut également observer une grande diminution des déplacements peu import la sorte. Deux autres baisses sont enregitrées en début 2021 et début 2022 ce qui correspondait également à d'autres pics de vague, mais avec un impact moins important plus on avance dans le temps. Ce qui peut également être noté, c'est que le taux de déplacement en transports publics a mis plus de temps à reprendre ses habitudes, même si au final, il reste moins élevé que les autres moyen de transport. Il peut être supposé que cela est dû au fait que plus de personne reste en télétravail sans que ce soit obligatoire ou que certaines hautes écoles et universités ont continué de proposer des cours à distance même après que les mesures aient été levées.

# COMMAND ----------

f = plt.figure()
f.set_figwidth(30)
f.set_figheight(12)
plt.plot(df_driving['date'], df_driving['value'].astype(float), label = "Driving")
plt.plot(df_walking['date'], df_walking['value'].astype(float), label = "Walking")
plt.plot(df_transit['date'], df_transit['value'].astype(float), label = "Transit")
plt.title('Mobility data')
plt.xlabel('Date')
plt.ylabel('Fréquentation')
plt.legend()
plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC **Taux de congestion**
# MAGIC 
# MAGIC Cet indice montre le comment le trafic a pu changé lors de la pandémie. Il ne suit par une ligne très continue car durant les weekends, il est forcément plus bas étant donné que les gens ne travaillent pas ou moins. On peut tout de même observer une diminution plutôt flagrante lors de la première vague, et quelques diminutions lors des suivantes, mais moins flagrantes.

# COMMAND ----------

f = plt.figure()
f.set_figwidth(30)
f.set_figheight(12)
plt.plot(df_cong['date'], df_cong['value'].astype(float), label = "Congestion")
plt.title('Mobility data')
plt.xlabel('Date')
plt.ylabel('Fréquentation')
plt.legend()
plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC **Chiffres du COVID-19**
# MAGIC 
# MAGIC Concernant les chiffres clés de la pandémie, les hospitalisation, les déces et le nombre de cas sont séléectionnés pour être analysés car ils sont supposés être les plus pertinants. En effet, les autres chiffres comme ceux de la vaccination, n'ont finalement pas été utilisés.
# MAGIC 
# MAGIC Il peut être noté que les différents pics enregitrés correspondent aux différentes vagues. Le plus visible se trouvant dans le graphique des hospitalisations ci-dessous. En effet, les mesures ont souvent été prise en fonction de ces informations, car la Confédération voulait éviter les surcharges dans les hôpitaux.

# COMMAND ----------

f = plt.figure()
f.set_figwidth(30)
f.set_figheight(12)
plt.plot(df_icu['date'], df_icu['value'].astype(float), label = "ICU patients")
plt.plot(df_hosp['date'], df_hosp['value'].astype(float), label = "Hospitalized patients")
plt.title('COVID-19 data')
plt.xlabel('Date')
plt.ylabel('Nombre')
plt.legend()
plt.show()

# COMMAND ----------

f = plt.figure()
f.set_figwidth(30)
f.set_figheight(12)
plt.plot(df_new_deaths['date'], df_new_deaths['value'].astype(float), label = "New deaths")
plt.title('COVID-19 data')
plt.xlabel('Date')
plt.ylabel('Nombre')
plt.legend()
plt.show()

# COMMAND ----------

df_corr = df.loc[df['index'] == 'new_cases']
f = plt.figure()
f.set_figwidth(30)
f.set_figheight(12)
plt.plot(df_new_cases['date'], df_new_cases['value'].astype(float), label = "New cases")
plt.title('COVID-19 data')
plt.xlabel('Date')
plt.ylabel('Nombre')
plt.legend()
plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC **Mesures des autorités**
# MAGIC 
# MAGIC Le stringency index est une mesure crée par Our World in Data - OWID (https://ourworldindata.org/covid-stringency-index#:~:text=The%20stringency%20index%20is%20a,100%20(100%20%3D%20strictest)), permettant d'indiquer la dureté des restrictions mises en place par le gouvernement. Il est basé sur 9 mesures au total (comme la fermeture des écoles, des restaurants, etc.) et se calcule sur une échelle de 1 à 100 (100 étant le plus stricte).
# MAGIC 
# MAGIC Il est également facile de remarquer ici que les différents pics correspondent également aux vagues, avec une nette augmentation en mars 2020, une autre augmentation fin 2020 et enfin la levée de presque toutes les mesures en avril 2022. L'indice n'est probablement pas à 0 car le télétravail reste une recommendation.

# COMMAND ----------

f = plt.figure()
f.set_figwidth(30)
f.set_figheight(12)
plt.plot(df_stringency['date'], df_stringency['value'].astype(float), label = "Stringency index")
plt.title('COVID-19 data')
plt.xlabel('Date')
plt.ylabel('Nombre')
plt.legend()
plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC **Corrélations**
# MAGIC 
# MAGIC Après avoir pré-analysé les variables individuellement, on regarde les corrélations afin de détecter les liens qu'elles peuvent avoir entre elles.
# MAGIC 
# MAGIC **-1** : Une corrélation négative parfaite (en pente descendante)  
# MAGIC **-0.70** : Une corrélation linéaire fortement négative (en pente descendante)  
# MAGIC **-0.50** : Une corrélation négative modérée (en pente descendante)  
# MAGIC **-0.30** : Une corrélation linéaire négative faible (en pente descendante)  
# MAGIC **0** : Pas de corrélation linéaire      
# MAGIC **+0.30** : Une corrélation linéaire positive faible (en pente ascendante)  
# MAGIC **+0.50** : Une corrélation linéaire positive modérée (en pente ascendante)  
# MAGIC **+0.70** : Une corrélation linéaire positive forte (en pente ascendante)  
# MAGIC **+1** : Corrélation linéaire positive parfaite (en pente ascendante)
# MAGIC 
# MAGIC 
# MAGIC Grâce à cette heatmap, on peut avoir une idée des corrélations entre les variables, seules les indices de plus de 0.6 et plus sont pris en compte, car jugés assez significatifs. De manière générale, on peut voir que le les taux de déplacements corrèlent positivement ou négativement avec les taux de fréquentation de différents lieux, ce qui est normal puisqu'il faut se déplacer pour y accèder ou au contraire, rester à la maison quand le taud de déplacement est au plus bas. Le stringency index a plutôt l'air dépendnant des différents taux de déplacement lui aussi, ainsi que des taux de fréquentation des lieux de travail et de loisirs. Cela correspond effectivement aux différentes mesures mises en plces qui ont impacté en premier ces lieux. Cet index dépend également du nombre de cas, de morts et des hospitalisations. En effet, certaines mesures ont été prises en fonction de l'apparition de ce genre de chiffres qui atteignaient des seuils déterminés.
# MAGIC 
# MAGIC On peut voir également que le nombre de morts et d'hospitalisation impact le taux de fréquentation des lieux de loisirs ainsi que le taux de déplacement en transport ou en voiture. Cela pourrait s'expliquer soit par la peur de la population, soit par le fait que des mesures ont été mises en place à cause de ces chiffres comme le télétravail ou la fermeture de certains endroits.

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC -- Utilisation de la base de données silver pour la récupération des données pour la heatmap
# MAGIC 
# MAGIC use silver_db

# COMMAND ----------

# Sélection des données depuis la table de fusion pour faciliter l'analyse

df = sqlContext.sql("select i.sub_region, i.date, c.vague, i.i_reatail_and_recreation, i.i_grocery_and_pharmacy, i.i_parks, i.i_workplaces, i.i_residential, i.i_driving, i.i_walking, i.i_transit, i.i_new_cases, i.i_new_deaths, i.i_icu_patients, i.i_hosp_patients, i.i_stringency_index, c.homeworking, c.school_closed, c.resto_closed, c.groceries_closed, c.covid_pass_active from all_index i join calendar c on c.date_int = i.date_int where i.canton_abreviation = 'CH' order by i.date").toPandas()

df.dropna(axis=0, inplace=True)

# COMMAND ----------

# Sélection des données de la Delta table all_index de la base de données silver, car elle contient les index sous forme de colonne, ce qui facilite l'affichage de la heatmap

correlation_df = sqlContext.sql("select * from all_index").toPandas()

correlation = correlation_df.corr()
fig, ax = plt.subplots(figsize=(20, 10))
sns.heatmap(correlation, annot=True, cmap="RdYlGn", linewidths=.1 )

# COMMAND ----------

# MAGIC %md
# MAGIC # 3. Préparation des données <a class="anchor" id="Datapreparation"></a>

# COMMAND ----------

# MAGIC %md
# MAGIC Cette partie correspond au processus de _data engineering_ exécuté dans la suite de scripts qui a permis de construire le modèle final. La documentation la concernant se trouve dans les scripts correspondants.

# COMMAND ----------

# MAGIC %md
# MAGIC # 4. Analyse exploratoire des données  <a class="anchor" id="EDA"></a>

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC Les hypothèses pour la comparaison de deux variables sont testées ici. Premièrement, l'hypothèse nulle "La distance journalière parcourue n'a pas changé durant la pandémie" ne peut être vérifiée car ces données sont inexistantes dans les datasets utilisés.
# MAGIC 
# MAGIC Concernant les hypothèses suivantes, il a été choisi de ne garder que les deux qui ont le plus d'impact sur la variable choisie. Egalement pour celles-ci, elles ont été analysée de manière gloable durant toute la pandémie, car ce sont des t-test à 2 échantillons et  la variable analysée n'est effective qu'à un certain moment du calendrier (télétravail, fermeture des écoles ou le pass covid).

# COMMAND ----------

# MAGIC %md
# MAGIC #### Impact du télétravail sur les déplacements
# MAGIC 
# MAGIC On peut voir ici que le télétravail a forcément eu un impact sur les déplacements, en particulier les déplacement en voiture. En effet, une grande diminution des déplacements est enregistrée.

# COMMAND ----------

# MAGIC %md
# MAGIC **H0 : Le télétravail n'a pas eu d'impact sur les déplacements en voiture**

# COMMAND ----------

noHomworking = df[df.homeworking == 0 ] 
homworking = df[df.homeworking == 1 ] 

t, p  = stats.ttest_ind(noHomworking.any().to_numpy(), homworking.any().to_numpy(), equal_var = False)
d = cD.cohensd_2ind(noHomworking["i_driving"], homworking["i_driving"])
ttest_2sample_and_cohen(p, d)

# COMMAND ----------

# MAGIC %md
# MAGIC **H0 : Le télétravail n'a pas eu d'impact sur les déplacements en transport**

# COMMAND ----------

noHomworking = df[df.homeworking == 0] 
homworking = df[df.homeworking == 1] 

t, p  = stats.ttest_ind(noHomworking["i_transit"].to_numpy(), homworking["i_transit"].to_numpy(), equal_var = False)
d = cD.cohensd_2ind(noHomworking["i_transit"], homworking["i_transit"])
ttest_2sample_and_cohen(p, d)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Impact de la fermeture des écoles sur les déplacements
# MAGIC 
# MAGIC Concernant la fermeture des écoles, la taille d'effet est particulièrement importante. En effet, les écoles ont été fermées plus longtemps complétement alors que le télétravail était plus souvent recommandé, ce qui fait que certain secteurs ont quand même demandé aux gens de venir en présentiel.

# COMMAND ----------

# MAGIC %md
# MAGIC **H0 : La fermeture des écoles n'a pas eu d'impact sur les déplacements à pied**

# COMMAND ----------

schoolClosed = df[df.school_closed == 0 ] 
schoolOpened = df[df.school_closed == 1 ] 

t, p  = stats.ttest_ind(schoolOpened["i_walking"].to_numpy(), schoolClosed["i_walking"].to_numpy(), equal_var = False)
d = cD.cohensd_2ind(schoolOpened["i_walking"], schoolClosed["i_walking"])
ttest_2sample_and_cohen(p, d)

# COMMAND ----------

# MAGIC %md
# MAGIC **H0 : La fermeture des écoles n'a pas eu d'impact sur les déplacements en transport**

# COMMAND ----------

schoolClosed = df[df.school_closed == 0] 
schoolOpened = df[df.school_closed == 1] 

t, p  = stats.ttest_ind(schoolOpened['i_transit'].to_numpy(), schoolClosed['i_transit'].to_numpy(), equal_var = False)
d = cD.cohensd_2ind(schoolOpened["i_transit"], schoolClosed["i_transit"])
ttest_2sample_and_cohen(p, d)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Impact du certificat covid sur la fréquentation de certains lieux
# MAGIC 
# MAGIC Le certificat covid comme ressenti lors de la pandémie, a surtout été impactant pour les lieux de loisirs. Pour les lieux de travail aussi mais la taille d'effet n'est pas aussi importante que pour les restaurants et autre lieux de détente.

# COMMAND ----------

# MAGIC %md
# MAGIC **H0 : Le certificat covid n'a pas d'impact sur la fréquentation des restaurants et lieux de loisirs**

# COMMAND ----------

passInactive = df[df.covid_pass_active == 0 ] 
passActive = df[df.covid_pass_active == 1 ] 
 
t, p  = stats.ttest_ind(passInactive["i_reatail_and_recreation"].to_numpy(), passActive["i_reatail_and_recreation"].to_numpy(), equal_var = False)
d = cD.cohensd_2ind(passInactive["i_reatail_and_recreation"], passActive["i_reatail_and_recreation"])
ttest_2sample_and_cohen(p, d)

# COMMAND ----------

# MAGIC %md
# MAGIC **H0 : Le certificat covid n'a pas d'impact sur la fréquentation des lieux de travail**

# COMMAND ----------

passInactive = df[df.covid_pass_active == 0 ] 
passActive = df[df.covid_pass_active == 1 ] 
 
t, p  = stats.ttest_ind(passInactive["i_workplaces"].to_numpy(), passActive["i_workplaces"].to_numpy(), equal_var = False)
d = cD.cohensd_2ind(passInactive["i_workplaces"], passActive["i_workplaces"])
ttest_2sample_and_cohen(p, d)

# COMMAND ----------

# MAGIC %md
# MAGIC # 5. Modélisation <a class="anchor" id="Modelling"></a>
# MAGIC 
# MAGIC Lors de ces analyses, il a été également choisi de comparer la première vague avec la dernière pour voir l'évolution du comportement de la population lors de la pandémie.
# MAGIC 
# MAGIC Lors de la réalisation des régressions linéaires, les graphiques ne sont pas sortis assez propore, cela est du au non-respect de certaines exigences pour la regression qui n'ont probablement pas dû être respectées. Cela est dû à la distribution ou la propreté des données. Cela veut dire que les hypothèses ne peuvent pas être rejetées avec certitude, cependant elles peuvent supposer certaines explications.

# COMMAND ----------

# MAGIC %md
# MAGIC **H0 : La fréquentation des magasins de première nécessité et pharmacies est indépendante du nombre de décès, de contamination et des mesures**
# MAGIC 
# MAGIC Nous pouvons remarquer ici qu'en première vague envrion 18% de la variance est expliquée par ces variables. Cepandant, en dernière vague elle est redescendue à 7% envrion.

# COMMAND ----------

df_corr = df[df.vague == '1']

X = df_corr[['i_new_deaths', 'i_new_cases', 'i_stringency_index']]
y = df_corr[['i_grocery_and_pharmacy']]

model = sm.OLS(y, sm.add_constant(X))  
model_fit = model.fit()
print(model_fit.summary())

# COMMAND ----------

diagnostic_plots(X, y)

# COMMAND ----------

df_corr = df[df.vague == '5']

X = df_corr[['i_new_deaths', 'i_new_cases', 'i_stringency_index']]
y = df_corr[['i_grocery_and_pharmacy']]

model = sm.OLS(y, sm.add_constant(X))  
model_fit = model.fit()
print(model_fit.summary())

# COMMAND ----------

diagnostic_plots(X, y)

# COMMAND ----------

# MAGIC %md
# MAGIC **H0 : La fréquentation des magasins et restaurants est indépendante du nombre de décès et des mesures**
# MAGIC 
# MAGIC Nous pouvons remarquer ici qu'en première vague envrion 90% de la variance est expliquée par ces variables, ce sont des résultats impressionnant. Cepandant, en dernière vague elle est redescendue à 11% envrion.

# COMMAND ----------

df_corr = df[df.vague == '1']

X = df_corr[['i_new_deaths', 'i_stringency_index']]
y = df_corr[['i_reatail_and_recreation']]

model = sm.OLS(y, sm.add_constant(X))
model_fit = model.fit()
print(model_fit.summary())

# COMMAND ----------

diagnostic_plots(X, y)

# COMMAND ----------

df_corr = df[df.vague == '5']

X = df_corr[['i_new_cases', 'i_stringency_index']]
y = df_corr[['i_reatail_and_recreation']]

model = sm.OLS(y, sm.add_constant(X))
model_fit = model.fit()
print(model_fit.summary())

# COMMAND ----------

diagnostic_plots(X, y)

# COMMAND ----------

# MAGIC %md
# MAGIC # 6. Evaluation <a class="anchor" id="Evaluation"></a>

# COMMAND ----------

# MAGIC %md
# MAGIC Concernant les hypothèses sur le taux de fréquentation et de déplacement, il a été remarqué que le télétravail a particulièrement impacté les déplacements en voiture mais aussi en transport. La fermeture des écoles quant à elle, a également eu une indidence sur les déplacement en transport mais aussi sur les déplacements à pied avec une taille d'effet beaucoup plus importante que celle enregitrée pour le télétravail. On peut en déduire que comme le télétravail était recommandé parfois, les gens ont quand même dû se déplacer. Certains secteurs n'ont pas eu le choix de travailler également comme les hôpitaux ou certaines industries. Par contre, lorsque les écoles étaient fermées, il n'existait pas d'alternative, donc les écoliers et étudiants sont forcément restés à la maison pour suivre les cours à distance.
# MAGIC 
# MAGIC Rien de surprenant lors de la mise en place du certificat covid, l'impact sur les lieux de loisirs est très important. En effet, ce sont surtout les restaurants et les lieux détente où il a été mis en place le plus longtemps. Une incidence a également été enregitrée sur les lieux de travail, en effet, certaines entreprise ont imposé le certificat, ce qui explique cela.
# MAGIC 
# MAGIC Par à la fréquentation des magasins de première nécessité et des pharmacies, les résultats ne sont pas exceptionnellement significatifs comparé à ceux de la fréquentation des lieux de loisirs et des restaurants qui présentent une large différence en début et en fin de pandémie. Ces hypothèses nulles ne peuvent être rejetée à causes des exigences de la régression linéaire qui n'ont probablement pas été respectées (voir les graphiques de dignostique). Cependant cela explique tout de même beaucoup de choses. 
# MAGIC 
# MAGIC On peut en déduire que les mouvements de la population dépendent d'une part des mesures mises en place par la Confédération, ce qui est logique, des règles sont là et elles sont respectées. Mais d'un autre côté, il y a peut être une certaine peur de la par des gens quant à certains chiffres du COVID-19. En effet, lors de ces analyses, les chiffres ayant eu les meilleurs résultats est le nombre de décès par jour. Ce chiffe explique une bonne partie de la variance des variables de fréquentation testés.
# MAGIC 
# MAGIC Ce travail d'anaylse n'est qu'un début, une direction dans laquelle des recherches plus approfondies pourraient être menée. En effet, si les données étaient plus travaillées et plusieurs autres essais de variables ou de modèles de Machine Learning étaient testés, des résultats plus significatifs et correctes pourraient être obtenus.

# COMMAND ----------

# MAGIC %md
# MAGIC # 7. Déploiement  <a class="anchor" id="Deployment"></a>

# COMMAND ----------

# MAGIC %md
# MAGIC Ce chapitre n'est pas traité, étant donné que le projet est un PoC.