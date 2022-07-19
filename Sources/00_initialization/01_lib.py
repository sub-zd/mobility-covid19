# Databricks notebook source
pip install cohens_d cD

# COMMAND ----------

# Import des librairies nécessaires

import pandas as pd
import matplotlib.pyplot as plt
from pyspark.sql.functions import col
import cohens_d as cD

from statsmodels.graphics.gofplots import ProbPlot
plt.rc('font', size=14)
plt.rc('figure', titlesize=18)
plt.rc('axes', labelsize=15)
plt.rc('axes', titlesize=18)

# COMMAND ----------

# Création d'un dataframe depuis un fichier CSV

def csv_import(file_path):
    file_location = file_path
    file_type = "csv"

    infer_schema = "true"
    first_row_is_header = "true"
    delimiter = ","

    df = spark.read.format(file_type).option("inferSchema", infer_schema).option("header", first_row_is_header).option("sep", delimiter).load(file_location)
    
    return df

# COMMAND ----------

# Fonction de génération du calendrier

def generate_calendar(start, end):
      df = pd.DataFrame({'date': pd.date_range(start, end)})
      df['date_int'] = df['date'].dt.strftime("%Y%m%d").astype(int)
      df['day'] = df.date.dt.day
      df['day_of_week'] = df.date.dt.dayofweek
      df['month'] = df.date.dt.month
      df['year'] = df.date.dt.year
    
      return df

# COMMAND ----------

# Affichage des colonnes avec des valeurs manquantes avec le pourcentage sur le total

def missing_values_table(df):
    mis_val = df.isnull().sum()
    mis_val_percent = 100 * df.isnull().sum() / len(df)
    mis_val_table = pd.concat([mis_val, mis_val_percent], axis=1)
    mis_val_table_ren_columns = mis_val_table.rename(
    columns = {0 : 'Nombre de valeurs manquantes', 1 : '% sur le total'})
    mis_val_table_ren_columns = mis_val_table_ren_columns[mis_val_table_ren_columns.iloc[:,1] != 0].sort_values('% sur le total', ascending=False).round(1)
    print ("Il y a " + str(mis_val_table_ren_columns.shape[0]) + " colonnes sur " + str(df.shape[1]) + " qui ont des valeurs manquantes.")

    return mis_val_table_ren_columns

# COMMAND ----------

# Affichage du graphique "Scatter plot"

def scatter_plots(y, X, features) :
    for f in features :
        x = X[f]
        plt.title('Correlation ' + y.name + ' & ' + x.name)
        sns.regplot(x = x.name, y = y.name, data = pd.concat([x, y], axis = 1), x_jitter = .05)
        
        plt.show()

# COMMAND ----------

# Rejet ou non de l'hypothèse nulle et affichage de la taille d'effet

def ttest_2sample_and_cohen(p, d):
    alpha = 0.05

    if p < alpha : 
        print('Avec un seuil α = ' + str(alpha) + ', on rejète l\'hypothèse nulle')
    else : 
        print('Avec un seuil α = ' + str(alpha) + ', on ne rejète pas l\'hypothèse nulle')
    
    if d <= 0.2 : 
        print('Avec un d de Cohen = ' + str(d) + ' la taille d\'effet est petite')
    elif d <= 0.5 : 
        print('Avec un d de Cohen = ' + str(d) + ' la taille d\'effet est moyenne')
    else :
        print('Avec un d de Cohen = ' + str(d) + ' la taille d\'effet est importante')

# COMMAND ----------

# Affichage des 4 graphiques nécessaires à l'analyse de la régression linéaires
# https://robert-alvarez.github.io/2018-06-04-diagnostic_plots/

def graph(formula, x_range, label=None):
    x = x_range
    y = formula(x)
    plt.plot(x, y, label=label, lw=1, ls='--', color='red')


def diagnostic_plots(X, y, model_fit=None):
    if not model_fit:
        model_fit = sm.OLS(y, sm.add_constant(X)).fit()

    dataframe = pd.concat([X, y], axis=1)
    
    model_fitted_y = model_fit.fittedvalues
    model_residuals = model_fit.resid
    model_norm_residuals = model_fit.get_influence().resid_studentized_internal
    model_norm_residuals_abs_sqrt = np.sqrt(np.abs(model_norm_residuals))
    model_abs_resid = np.abs(model_residuals)
    model_leverage = model_fit.get_influence().hat_matrix_diag
    model_cooks = model_fit.get_influence().cooks_distance[0]
    
    plot_lm_1 = plt.figure()
    plot_lm_1.axes[0] = sns.residplot(x = model_fitted_y, y = dataframe.columns[-1], data = dataframe, lowess=True, scatter_kws={'alpha': 0.5}, line_kws={'color': 'red', 'lw': 1, 'alpha': 0.8})
    
    plot_lm_1.axes[0].set_title('Residuals vs Fitted')
    plot_lm_1.axes[0].set_xlabel('Fitted values')
    plot_lm_1.axes[0].set_ylabel('Residuals');
    
    abs_resid = model_abs_resid.sort_values(ascending=False)
    abs_resid_top_3 = abs_resid[:3]
    for i in abs_resid_top_3.index:
        plot_lm_1.axes[0].annotate(i, xy=(model_fitted_y[i], model_residuals[i]));
        
    QQ = ProbPlot(model_norm_residuals)
    plot_lm_2 = QQ.qqplot(line='45', alpha=0.5, color='#4C72B0', lw=1)
    plot_lm_2.axes[0].set_title('Normal Q-Q')
    plot_lm_2.axes[0].set_xlabel('Theoretical Quantiles')
    plot_lm_2.axes[0].set_ylabel('Standardized Residuals');
    
    abs_norm_resid = np.flip(np.argsort(np.abs(model_norm_residuals)), 0)
    abs_norm_resid_top_3 = abs_norm_resid[:3]
    for r, i in enumerate(abs_norm_resid_top_3):
        plot_lm_2.axes[0].annotate(i, xy=(np.flip(QQ.theoretical_quantiles, 0)[r], model_norm_residuals[i]));
    
    plot_lm_3 = plt.figure()
    plt.scatter(model_fitted_y, model_norm_residuals_abs_sqrt, alpha=0.5);
    sns.regplot(x = model_fitted_y, y = model_norm_residuals_abs_sqrt, scatter=False, ci=False, lowess=True, line_kws={'color': 'red', 'lw': 1, 'alpha': 0.8});
    plot_lm_3.axes[0].set_title('Scale-Location')
    plot_lm_3.axes[0].set_xlabel('Fitted values')
    plot_lm_3.axes[0].set_ylabel('$\sqrt{|Standardized Residuals|}$');
    
    abs_sq_norm_resid = np.flip(np.argsort(model_norm_residuals_abs_sqrt), 0)
    abs_sq_norm_resid_top_3 = abs_sq_norm_resid[:3]
    for i in abs_norm_resid_top_3:
        plot_lm_3.axes[0].annotate(i, xy=(model_fitted_y[i], model_norm_residuals_abs_sqrt[i]));
    
    plot_lm_4 = plt.figure();
    plt.scatter(model_leverage, model_norm_residuals, alpha=0.5);
    sns.regplot(x = model_leverage, y = model_norm_residuals, scatter=False, ci=False, lowess=True, line_kws={'color': 'red', 'lw': 1, 'alpha': 0.8});
    plot_lm_4.axes[0].set_xlim(0, max(model_leverage)+0.01)
    plot_lm_4.axes[0].set_ylim(-3, 5)
    plot_lm_4.axes[0].set_title('Residuals vs Leverage')
    plot_lm_4.axes[0].set_xlabel('Leverage')
    plot_lm_4.axes[0].set_ylabel('Standardized Residuals');
    
    leverage_top_3 = np.flip(np.argsort(model_cooks), 0)[:3]
    for i in leverage_top_3:
        plot_lm_4.axes[0].annotate(i, xy=(model_leverage[i], model_norm_residuals[i]));
        
    p = len(model_fit.params)
    graph(lambda x: np.sqrt((0.5 * p * (1 - x)) / x), np.linspace(0.001, max(model_leverage), 50), 'Cook\'s distance')
    graph(lambda x: np.sqrt((1 * p * (1 - x)) / x), np.linspace(0.001, max(model_leverage), 50))
    plot_lm_4.legend(loc='upper right');