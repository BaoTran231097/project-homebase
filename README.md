# OVERVIEW PROJECT: HOMEBASE

```
├── docker-compose.yml        # file deploy service
├── Dockerfile                # file deploy service
├── users.xml                 # config service clickhouse
├── notebook.ipynb            # file document and tasks completed
├── query_clickhouse.sql      # file query data clickhouse
├── wine_dataset              # folder dataset
│   ├── Index                 # File index dataset
│   ├── wine.data             # File dataset
│   └── wine.names            # File document dataset
├── dags                      # Folder inclune file DAG
│   └── pipeline.py           # File transfer data from PostgreSQL to ClickHouse.
└── ...
```

### Deploy service:
+ Database: Postgresql
  + user:postgres
  + password:postgres
  + database:airflow
+ Database: Clickhouse
  + user:clickhouse
  + password:clickhouse
  + database:clickhousedb
+ Ariflow-init
+ Airflow-webservice
+ airflow-scheduler

### Run docker-compose.yml:
    docker-compose up --build

# 1. Data Ingestion with Python

### 1.1 Download a dataset

Download dataset from here https://archive.ics.uci.edu/static/public/109/wine.zip

### 1.2 Import it into a Python environment.


```python
# import library
import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt 
import numpy as np
from datetime import datetime
```


```python
# read data from file dataset
df = pd.read_csv("wine_dataset/wine.data",  header=None)
df.head()
```




<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>0</th>
      <th>1</th>
      <th>2</th>
      <th>3</th>
      <th>4</th>
      <th>5</th>
      <th>6</th>
      <th>7</th>
      <th>8</th>
      <th>9</th>
      <th>10</th>
      <th>11</th>
      <th>12</th>
      <th>13</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>1</td>
      <td>14.23</td>
      <td>1.71</td>
      <td>2.43</td>
      <td>15.6</td>
      <td>127</td>
      <td>2.80</td>
      <td>3.06</td>
      <td>0.28</td>
      <td>2.29</td>
      <td>5.64</td>
      <td>1.04</td>
      <td>3.92</td>
      <td>1065</td>
    </tr>
    <tr>
      <th>1</th>
      <td>1</td>
      <td>13.20</td>
      <td>1.78</td>
      <td>2.14</td>
      <td>11.2</td>
      <td>100</td>
      <td>2.65</td>
      <td>2.76</td>
      <td>0.26</td>
      <td>1.28</td>
      <td>4.38</td>
      <td>1.05</td>
      <td>3.40</td>
      <td>1050</td>
    </tr>
    <tr>
      <th>2</th>
      <td>1</td>
      <td>13.16</td>
      <td>2.36</td>
      <td>2.67</td>
      <td>18.6</td>
      <td>101</td>
      <td>2.80</td>
      <td>3.24</td>
      <td>0.30</td>
      <td>2.81</td>
      <td>5.68</td>
      <td>1.03</td>
      <td>3.17</td>
      <td>1185</td>
    </tr>
    <tr>
      <th>3</th>
      <td>1</td>
      <td>14.37</td>
      <td>1.95</td>
      <td>2.50</td>
      <td>16.8</td>
      <td>113</td>
      <td>3.85</td>
      <td>3.49</td>
      <td>0.24</td>
      <td>2.18</td>
      <td>7.80</td>
      <td>0.86</td>
      <td>3.45</td>
      <td>1480</td>
    </tr>
    <tr>
      <th>4</th>
      <td>1</td>
      <td>13.24</td>
      <td>2.59</td>
      <td>2.87</td>
      <td>21.0</td>
      <td>118</td>
      <td>2.80</td>
      <td>2.69</td>
      <td>0.39</td>
      <td>1.82</td>
      <td>4.32</td>
      <td>1.04</td>
      <td>2.93</td>
      <td>735</td>
    </tr>
  </tbody>
</table>
</div>




```python
# Since the dataset doesn't have headers, we will add a header like 'Document wine_dataset/wine.name'.
df.columns = ['Classes',
              'Alcohol',
              'Malic acid',
              'Ash',
              'Alcalinity of ash',
              'Magnesium',
              'Total phenols',
              'Flavanoids',
              'Nonflavanoid phenols',
              'Proanthocyanins',
              'Color intensity',
              'Hue',
              'OD280/OD315 of diluted wines',
              'Proline']
```

# 2. Data Transformation with Python

### 2.1 Use Pandas to clean and transform the data.


```python
# Exploratory Data Analysis
df.describe().T
```




<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>count</th>
      <th>mean</th>
      <th>std</th>
      <th>min</th>
      <th>25%</th>
      <th>50%</th>
      <th>75%</th>
      <th>max</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>Classes</th>
      <td>178.0</td>
      <td>1.938202</td>
      <td>0.775035</td>
      <td>1.00</td>
      <td>1.0000</td>
      <td>2.000</td>
      <td>3.0000</td>
      <td>3.00</td>
    </tr>
    <tr>
      <th>Alcohol</th>
      <td>178.0</td>
      <td>13.000618</td>
      <td>0.811827</td>
      <td>11.03</td>
      <td>12.3625</td>
      <td>13.050</td>
      <td>13.6775</td>
      <td>14.83</td>
    </tr>
    <tr>
      <th>Malic acid</th>
      <td>178.0</td>
      <td>2.336348</td>
      <td>1.117146</td>
      <td>0.74</td>
      <td>1.6025</td>
      <td>1.865</td>
      <td>3.0825</td>
      <td>5.80</td>
    </tr>
    <tr>
      <th>Ash</th>
      <td>178.0</td>
      <td>2.366517</td>
      <td>0.274344</td>
      <td>1.36</td>
      <td>2.2100</td>
      <td>2.360</td>
      <td>2.5575</td>
      <td>3.23</td>
    </tr>
    <tr>
      <th>Alcalinity of ash</th>
      <td>178.0</td>
      <td>19.494944</td>
      <td>3.339564</td>
      <td>10.60</td>
      <td>17.2000</td>
      <td>19.500</td>
      <td>21.5000</td>
      <td>30.00</td>
    </tr>
    <tr>
      <th>Magnesium</th>
      <td>178.0</td>
      <td>99.741573</td>
      <td>14.282484</td>
      <td>70.00</td>
      <td>88.0000</td>
      <td>98.000</td>
      <td>107.0000</td>
      <td>162.00</td>
    </tr>
    <tr>
      <th>Total phenols</th>
      <td>178.0</td>
      <td>2.295112</td>
      <td>0.625851</td>
      <td>0.98</td>
      <td>1.7425</td>
      <td>2.355</td>
      <td>2.8000</td>
      <td>3.88</td>
    </tr>
    <tr>
      <th>Flavanoids</th>
      <td>178.0</td>
      <td>2.029270</td>
      <td>0.998859</td>
      <td>0.34</td>
      <td>1.2050</td>
      <td>2.135</td>
      <td>2.8750</td>
      <td>5.08</td>
    </tr>
    <tr>
      <th>Nonflavanoid phenols</th>
      <td>178.0</td>
      <td>0.361854</td>
      <td>0.124453</td>
      <td>0.13</td>
      <td>0.2700</td>
      <td>0.340</td>
      <td>0.4375</td>
      <td>0.66</td>
    </tr>
    <tr>
      <th>Proanthocyanins</th>
      <td>178.0</td>
      <td>1.590899</td>
      <td>0.572359</td>
      <td>0.41</td>
      <td>1.2500</td>
      <td>1.555</td>
      <td>1.9500</td>
      <td>3.58</td>
    </tr>
    <tr>
      <th>Color intensity</th>
      <td>178.0</td>
      <td>5.058090</td>
      <td>2.318286</td>
      <td>1.28</td>
      <td>3.2200</td>
      <td>4.690</td>
      <td>6.2000</td>
      <td>13.00</td>
    </tr>
    <tr>
      <th>Hue</th>
      <td>178.0</td>
      <td>0.957449</td>
      <td>0.228572</td>
      <td>0.48</td>
      <td>0.7825</td>
      <td>0.965</td>
      <td>1.1200</td>
      <td>1.71</td>
    </tr>
    <tr>
      <th>OD280/OD315 of diluted wines</th>
      <td>178.0</td>
      <td>2.611685</td>
      <td>0.709990</td>
      <td>1.27</td>
      <td>1.9375</td>
      <td>2.780</td>
      <td>3.1700</td>
      <td>4.00</td>
    </tr>
    <tr>
      <th>Proline</th>
      <td>178.0</td>
      <td>746.893258</td>
      <td>314.907474</td>
      <td>278.00</td>
      <td>500.5000</td>
      <td>673.500</td>
      <td>985.0000</td>
      <td>1680.00</td>
    </tr>
  </tbody>
</table>
</div>




```python
# Check data type
df.info()
```

    <class 'pandas.core.frame.DataFrame'>
    RangeIndex: 178 entries, 0 to 177
    Data columns (total 14 columns):
     #   Column                        Non-Null Count  Dtype  
    ---  ------                        --------------  -----  
     0   Classes                       178 non-null    int64  
     1   Alcohol                       178 non-null    float64
     2   Malic acid                    178 non-null    float64
     3   Ash                           178 non-null    float64
     4   Alcalinity of ash             178 non-null    float64
     5   Magnesium                     178 non-null    int64  
     6   Total phenols                 178 non-null    float64
     7   Flavanoids                    178 non-null    float64
     8   Nonflavanoid phenols          178 non-null    float64
     9   Proanthocyanins               178 non-null    float64
     10  Color intensity               178 non-null    float64
     11  Hue                           178 non-null    float64
     12  OD280/OD315 of diluted wines  178 non-null    float64
     13  Proline                       178 non-null    int64  
    dtypes: float64(11), int64(3)
    memory usage: 19.6 KB
    


```python
# Dislpays the datatypes of the reviews_df DataFrame objectL
df.dtypes
```




    Classes                           int64
    Alcohol                         float64
    Malic acid                      float64
    Ash                             float64
    Alcalinity of ash               float64
    Magnesium                         int64
    Total phenols                   float64
    Flavanoids                      float64
    Nonflavanoid phenols            float64
    Proanthocyanins                 float64
    Color intensity                 float64
    Hue                             float64
    OD280/OD315 of diluted wines    float64
    Proline                           int64
    dtype: object




```python
# Boolean - True if column has null value, False if no null values:
df.isnull().any()
```




    Classes                         False
    Alcohol                         False
    Malic acid                      False
    Ash                             False
    Alcalinity of ash               False
    Magnesium                       False
    Total phenols                   False
    Flavanoids                      False
    Nonflavanoid phenols            False
    Proanthocyanins                 False
    Color intensity                 False
    Hue                             False
    OD280/OD315 of diluted wines    False
    Proline                         False
    dtype: bool




```python
# The sum of null values per column:
df.isnull().sum()
```




    Classes                         0
    Alcohol                         0
    Malic acid                      0
    Ash                             0
    Alcalinity of ash               0
    Magnesium                       0
    Total phenols                   0
    Flavanoids                      0
    Nonflavanoid phenols            0
    Proanthocyanins                 0
    Color intensity                 0
    Hue                             0
    OD280/OD315 of diluted wines    0
    Proline                         0
    dtype: int64




```python
# Remove missing values
df.dropna()

# Remove duplicates
df.drop_duplicates()

```




<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>Classes</th>
      <th>Alcohol</th>
      <th>Malic acid</th>
      <th>Ash</th>
      <th>Alcalinity of ash</th>
      <th>Magnesium</th>
      <th>Total phenols</th>
      <th>Flavanoids</th>
      <th>Nonflavanoid phenols</th>
      <th>Proanthocyanins</th>
      <th>Color intensity</th>
      <th>Hue</th>
      <th>OD280/OD315 of diluted wines</th>
      <th>Proline</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>1</td>
      <td>14.23</td>
      <td>1.71</td>
      <td>2.43</td>
      <td>15.6</td>
      <td>127</td>
      <td>2.80</td>
      <td>3.06</td>
      <td>0.28</td>
      <td>2.29</td>
      <td>5.64</td>
      <td>1.04</td>
      <td>3.92</td>
      <td>1065</td>
    </tr>
    <tr>
      <th>1</th>
      <td>1</td>
      <td>13.20</td>
      <td>1.78</td>
      <td>2.14</td>
      <td>11.2</td>
      <td>100</td>
      <td>2.65</td>
      <td>2.76</td>
      <td>0.26</td>
      <td>1.28</td>
      <td>4.38</td>
      <td>1.05</td>
      <td>3.40</td>
      <td>1050</td>
    </tr>
    <tr>
      <th>2</th>
      <td>1</td>
      <td>13.16</td>
      <td>2.36</td>
      <td>2.67</td>
      <td>18.6</td>
      <td>101</td>
      <td>2.80</td>
      <td>3.24</td>
      <td>0.30</td>
      <td>2.81</td>
      <td>5.68</td>
      <td>1.03</td>
      <td>3.17</td>
      <td>1185</td>
    </tr>
    <tr>
      <th>3</th>
      <td>1</td>
      <td>14.37</td>
      <td>1.95</td>
      <td>2.50</td>
      <td>16.8</td>
      <td>113</td>
      <td>3.85</td>
      <td>3.49</td>
      <td>0.24</td>
      <td>2.18</td>
      <td>7.80</td>
      <td>0.86</td>
      <td>3.45</td>
      <td>1480</td>
    </tr>
    <tr>
      <th>4</th>
      <td>1</td>
      <td>13.24</td>
      <td>2.59</td>
      <td>2.87</td>
      <td>21.0</td>
      <td>118</td>
      <td>2.80</td>
      <td>2.69</td>
      <td>0.39</td>
      <td>1.82</td>
      <td>4.32</td>
      <td>1.04</td>
      <td>2.93</td>
      <td>735</td>
    </tr>
    <tr>
      <th>...</th>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
    </tr>
    <tr>
      <th>173</th>
      <td>3</td>
      <td>13.71</td>
      <td>5.65</td>
      <td>2.45</td>
      <td>20.5</td>
      <td>95</td>
      <td>1.68</td>
      <td>0.61</td>
      <td>0.52</td>
      <td>1.06</td>
      <td>7.70</td>
      <td>0.64</td>
      <td>1.74</td>
      <td>740</td>
    </tr>
    <tr>
      <th>174</th>
      <td>3</td>
      <td>13.40</td>
      <td>3.91</td>
      <td>2.48</td>
      <td>23.0</td>
      <td>102</td>
      <td>1.80</td>
      <td>0.75</td>
      <td>0.43</td>
      <td>1.41</td>
      <td>7.30</td>
      <td>0.70</td>
      <td>1.56</td>
      <td>750</td>
    </tr>
    <tr>
      <th>175</th>
      <td>3</td>
      <td>13.27</td>
      <td>4.28</td>
      <td>2.26</td>
      <td>20.0</td>
      <td>120</td>
      <td>1.59</td>
      <td>0.69</td>
      <td>0.43</td>
      <td>1.35</td>
      <td>10.20</td>
      <td>0.59</td>
      <td>1.56</td>
      <td>835</td>
    </tr>
    <tr>
      <th>176</th>
      <td>3</td>
      <td>13.17</td>
      <td>2.59</td>
      <td>2.37</td>
      <td>20.0</td>
      <td>120</td>
      <td>1.65</td>
      <td>0.68</td>
      <td>0.53</td>
      <td>1.46</td>
      <td>9.30</td>
      <td>0.60</td>
      <td>1.62</td>
      <td>840</td>
    </tr>
    <tr>
      <th>177</th>
      <td>3</td>
      <td>14.13</td>
      <td>4.10</td>
      <td>2.74</td>
      <td>24.5</td>
      <td>96</td>
      <td>2.05</td>
      <td>0.76</td>
      <td>0.56</td>
      <td>1.35</td>
      <td>9.20</td>
      <td>0.61</td>
      <td>1.60</td>
      <td>560</td>
    </tr>
  </tbody>
</table>
<p>178 rows × 14 columns</p>
</div>




```python
# Change data type
df['Proline'] = df['Proline'].astype('float64')
```


```python
# Check data type
df.info()
```

    <class 'pandas.core.frame.DataFrame'>
    RangeIndex: 178 entries, 0 to 177
    Data columns (total 14 columns):
     #   Column                        Non-Null Count  Dtype  
    ---  ------                        --------------  -----  
     0   Classes                       178 non-null    int64  
     1   Alcohol                       178 non-null    float64
     2   Malic acid                    178 non-null    float64
     3   Ash                           178 non-null    float64
     4   Alcalinity of ash             178 non-null    float64
     5   Magnesium                     178 non-null    int64  
     6   Total phenols                 178 non-null    float64
     7   Flavanoids                    178 non-null    float64
     8   Nonflavanoid phenols          178 non-null    float64
     9   Proanthocyanins               178 non-null    float64
     10  Color intensity               178 non-null    float64
     11  Hue                           178 non-null    float64
     12  OD280/OD315 of diluted wines  178 non-null    float64
     13  Proline                       178 non-null    float64
    dtypes: float64(12), int64(2)
    memory usage: 19.6 KB
    


```python
#Remove unnecessary columns from the DataFrame.

df = df.drop(['OD280/OD315 of diluted wines'], axis=1)
df.head(5)
```




<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>Classes</th>
      <th>Alcohol</th>
      <th>Malic acid</th>
      <th>Ash</th>
      <th>Alcalinity of ash</th>
      <th>Magnesium</th>
      <th>Total phenols</th>
      <th>Flavanoids</th>
      <th>Nonflavanoid phenols</th>
      <th>Proanthocyanins</th>
      <th>Color intensity</th>
      <th>Hue</th>
      <th>Proline</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>1</td>
      <td>14.23</td>
      <td>1.71</td>
      <td>2.43</td>
      <td>15.6</td>
      <td>127</td>
      <td>2.80</td>
      <td>3.06</td>
      <td>0.28</td>
      <td>2.29</td>
      <td>5.64</td>
      <td>1.04</td>
      <td>1065.0</td>
    </tr>
    <tr>
      <th>1</th>
      <td>1</td>
      <td>13.20</td>
      <td>1.78</td>
      <td>2.14</td>
      <td>11.2</td>
      <td>100</td>
      <td>2.65</td>
      <td>2.76</td>
      <td>0.26</td>
      <td>1.28</td>
      <td>4.38</td>
      <td>1.05</td>
      <td>1050.0</td>
    </tr>
    <tr>
      <th>2</th>
      <td>1</td>
      <td>13.16</td>
      <td>2.36</td>
      <td>2.67</td>
      <td>18.6</td>
      <td>101</td>
      <td>2.80</td>
      <td>3.24</td>
      <td>0.30</td>
      <td>2.81</td>
      <td>5.68</td>
      <td>1.03</td>
      <td>1185.0</td>
    </tr>
    <tr>
      <th>3</th>
      <td>1</td>
      <td>14.37</td>
      <td>1.95</td>
      <td>2.50</td>
      <td>16.8</td>
      <td>113</td>
      <td>3.85</td>
      <td>3.49</td>
      <td>0.24</td>
      <td>2.18</td>
      <td>7.80</td>
      <td>0.86</td>
      <td>1480.0</td>
    </tr>
    <tr>
      <th>4</th>
      <td>1</td>
      <td>13.24</td>
      <td>2.59</td>
      <td>2.87</td>
      <td>21.0</td>
      <td>118</td>
      <td>2.80</td>
      <td>2.69</td>
      <td>0.39</td>
      <td>1.82</td>
      <td>4.32</td>
      <td>1.04</td>
      <td>735.0</td>
    </tr>
  </tbody>
</table>
</div>




```python
#Change the names of columns for clarity.

df.columns=[i.replace(' ','_') .lower() for i in df.columns.to_list()]
df.head(5)
```




<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>classes</th>
      <th>alcohol</th>
      <th>malic_acid</th>
      <th>ash</th>
      <th>alcalinity_of_ash</th>
      <th>magnesium</th>
      <th>total_phenols</th>
      <th>flavanoids</th>
      <th>nonflavanoid_phenols</th>
      <th>proanthocyanins</th>
      <th>color_intensity</th>
      <th>hue</th>
      <th>proline</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>1</td>
      <td>14.23</td>
      <td>1.71</td>
      <td>2.43</td>
      <td>15.6</td>
      <td>127</td>
      <td>2.80</td>
      <td>3.06</td>
      <td>0.28</td>
      <td>2.29</td>
      <td>5.64</td>
      <td>1.04</td>
      <td>1065.0</td>
    </tr>
    <tr>
      <th>1</th>
      <td>1</td>
      <td>13.20</td>
      <td>1.78</td>
      <td>2.14</td>
      <td>11.2</td>
      <td>100</td>
      <td>2.65</td>
      <td>2.76</td>
      <td>0.26</td>
      <td>1.28</td>
      <td>4.38</td>
      <td>1.05</td>
      <td>1050.0</td>
    </tr>
    <tr>
      <th>2</th>
      <td>1</td>
      <td>13.16</td>
      <td>2.36</td>
      <td>2.67</td>
      <td>18.6</td>
      <td>101</td>
      <td>2.80</td>
      <td>3.24</td>
      <td>0.30</td>
      <td>2.81</td>
      <td>5.68</td>
      <td>1.03</td>
      <td>1185.0</td>
    </tr>
    <tr>
      <th>3</th>
      <td>1</td>
      <td>14.37</td>
      <td>1.95</td>
      <td>2.50</td>
      <td>16.8</td>
      <td>113</td>
      <td>3.85</td>
      <td>3.49</td>
      <td>0.24</td>
      <td>2.18</td>
      <td>7.80</td>
      <td>0.86</td>
      <td>1480.0</td>
    </tr>
    <tr>
      <th>4</th>
      <td>1</td>
      <td>13.24</td>
      <td>2.59</td>
      <td>2.87</td>
      <td>21.0</td>
      <td>118</td>
      <td>2.80</td>
      <td>2.69</td>
      <td>0.39</td>
      <td>1.82</td>
      <td>4.32</td>
      <td>1.04</td>
      <td>735.0</td>
    </tr>
  </tbody>
</table>
</div>




```python
# Create new columns based on existing data.
df['process_dt'] = datetime.now().strftime('%Y-%m-%d %H:00:00')
df.head(5)
```




<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>classes</th>
      <th>alcohol</th>
      <th>malic_acid</th>
      <th>ash</th>
      <th>alcalinity_of_ash</th>
      <th>magnesium</th>
      <th>total_phenols</th>
      <th>flavanoids</th>
      <th>nonflavanoid_phenols</th>
      <th>proanthocyanins</th>
      <th>color_intensity</th>
      <th>hue</th>
      <th>proline</th>
      <th>process_dt</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>1</td>
      <td>14.23</td>
      <td>1.71</td>
      <td>2.43</td>
      <td>15.6</td>
      <td>127</td>
      <td>2.80</td>
      <td>3.06</td>
      <td>0.28</td>
      <td>2.29</td>
      <td>5.64</td>
      <td>1.04</td>
      <td>1065.0</td>
      <td>2023-10-25 12:00:00</td>
    </tr>
    <tr>
      <th>1</th>
      <td>1</td>
      <td>13.20</td>
      <td>1.78</td>
      <td>2.14</td>
      <td>11.2</td>
      <td>100</td>
      <td>2.65</td>
      <td>2.76</td>
      <td>0.26</td>
      <td>1.28</td>
      <td>4.38</td>
      <td>1.05</td>
      <td>1050.0</td>
      <td>2023-10-25 12:00:00</td>
    </tr>
    <tr>
      <th>2</th>
      <td>1</td>
      <td>13.16</td>
      <td>2.36</td>
      <td>2.67</td>
      <td>18.6</td>
      <td>101</td>
      <td>2.80</td>
      <td>3.24</td>
      <td>0.30</td>
      <td>2.81</td>
      <td>5.68</td>
      <td>1.03</td>
      <td>1185.0</td>
      <td>2023-10-25 12:00:00</td>
    </tr>
    <tr>
      <th>3</th>
      <td>1</td>
      <td>14.37</td>
      <td>1.95</td>
      <td>2.50</td>
      <td>16.8</td>
      <td>113</td>
      <td>3.85</td>
      <td>3.49</td>
      <td>0.24</td>
      <td>2.18</td>
      <td>7.80</td>
      <td>0.86</td>
      <td>1480.0</td>
      <td>2023-10-25 12:00:00</td>
    </tr>
    <tr>
      <th>4</th>
      <td>1</td>
      <td>13.24</td>
      <td>2.59</td>
      <td>2.87</td>
      <td>21.0</td>
      <td>118</td>
      <td>2.80</td>
      <td>2.69</td>
      <td>0.39</td>
      <td>1.82</td>
      <td>4.32</td>
      <td>1.04</td>
      <td>735.0</td>
      <td>2023-10-25 12:00:00</td>
    </tr>
  </tbody>
</table>
</div>



### 2.2 Generate summary statistics for three key variables.

#### Variables: *Classes*


```python
test_classes = df['classes']
test_classes.head()
```




    0    1
    1    1
    2    1
    3    1
    4    1
    Name: classes, dtype: int64




```python
test_classes.shape
```




    (178,)




```python
type(test_classes)
```




    pandas.core.series.Series




```python
test_classes.value_counts()
```




    2    71
    1    59
    3    48
    Name: classes, dtype: int64




```python
# %%time

test_classes.value_counts().head(10).plot.bar()
```






    
![png](image/output_30_1.png)
    



```python
df_test_classes=pd.get_dummies(test_classes)
df_test_classes
```




<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>1</th>
      <th>2</th>
      <th>3</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>1</td>
      <td>0</td>
      <td>0</td>
    </tr>
    <tr>
      <th>1</th>
      <td>1</td>
      <td>0</td>
      <td>0</td>
    </tr>
    <tr>
      <th>2</th>
      <td>1</td>
      <td>0</td>
      <td>0</td>
    </tr>
    <tr>
      <th>3</th>
      <td>1</td>
      <td>0</td>
      <td>0</td>
    </tr>
    <tr>
      <th>4</th>
      <td>1</td>
      <td>0</td>
      <td>0</td>
    </tr>
    <tr>
      <th>...</th>
      <td>...</td>
      <td>...</td>
      <td>...</td>
    </tr>
    <tr>
      <th>173</th>
      <td>0</td>
      <td>0</td>
      <td>1</td>
    </tr>
    <tr>
      <th>174</th>
      <td>0</td>
      <td>0</td>
      <td>1</td>
    </tr>
    <tr>
      <th>175</th>
      <td>0</td>
      <td>0</td>
      <td>1</td>
    </tr>
    <tr>
      <th>176</th>
      <td>0</td>
      <td>0</td>
      <td>1</td>
    </tr>
    <tr>
      <th>177</th>
      <td>0</td>
      <td>0</td>
      <td>1</td>
    </tr>
  </tbody>
</table>
<p>178 rows × 3 columns</p>
</div>



#### Variables: *Alcohol*

Plotting Pie Chart to show distribution of positive and negative classes

Assuming that:
+ If Alcohol > 13.5 resluts Quality Good
+ If Alcohol <= 13.5 resluts Quality Bad


```python
# Change values in the Quality column as per requirement
df["quality"] = df["alcohol"]>13.5

# Diaplay the changed records
df[["quality"]].head()
```




<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>quality</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>True</td>
    </tr>
    <tr>
      <th>1</th>
      <td>False</td>
    </tr>
    <tr>
      <th>2</th>
      <td>False</td>
    </tr>
    <tr>
      <th>3</th>
      <td>True</td>
    </tr>
    <tr>
      <th>4</th>
      <td>False</td>
    </tr>
  </tbody>
</table>
</div>




```python
#Check number of entries in a cloumn
number_of_wines = df.shape[0]

# Alcohol > 13.5 resluts Quality Good
wine_quality_good = df.loc[(df['quality'] == True)] #Accessing Column using label
amount_of_good_wine = wine_quality_good.shape[0] #Returns Dimensionality of DataFrame in tuple format

# Alcohol <= 13.5 resluts Quality Bad
wine_quality_bad = df.loc[(df['quality'] == False)]
amount_of_bad_wine = wine_quality_bad.shape[0]

# Percentage of wines with good quality
good_wine_percentage = amount_of_good_wine*100/number_of_wines

# Print the results
print("Total number of wine: {}".format(number_of_wines))
print("Wines with rating 13.5 and above: {}".format(amount_of_good_wine))
print("Wines with rating less than 13.5: {}".format(amount_of_bad_wine))
print("Percentage of wines with quality 13.5 and above: {:.2f}%".format(good_wine_percentage))
```

    Total number of wine: 178
    Wines with rating 13.5 and above: 55
    Wines with rating less than 13.5: 123
    Percentage of wines with quality 13.5 and above: 30.90%
    


```python
# Data to plot
plt.figure(figsize = (8,8))
labels = 'Quality = Good', 'Quality = Not Good'
plt.title('Wine Quality Pie Chart')
sizes = [amount_of_good_wine, amount_of_bad_wine]
colors = ['green', 'red']

# Plot pie chart
plt.pie(sizes,  labels=labels, colors=colors,autopct='%1.1f%%', shadow=True, startangle=90)
plt.axis('equal')
plt.show()

```


    
![png](image/output_37_0.png)
    


#### Variables: *Alcalinity of ash*

+ Based on quality we will classify Alcalinity of ash
+ Then calculate the average Alkalinity of ash based on quality (Good and Bad)


```python
alcalinity_of_ash_quality_good =  np.average(df[df["quality"]==True]['alcalinity_of_ash'])
alcalinity_of_ash_quality_bad = np.average(df[df["quality"]==False]['alcalinity_of_ash'])

print("The average Alkalinity of ash based on quality Good : {}".format(alcalinity_of_ash_quality_good))
print("The average Alkalinity of ash based on quality Bad : {}".format(alcalinity_of_ash_quality_bad))
```

    The average Alkalinity of ash based on quality Good : 18.052727272727275
    The average Alkalinity of ash based on quality Bad : 20.13983739837398
    


```python
# Data to plot
plt.figure(figsize = (8,8))
labels = 'Alkalinity of ash based = Good', 'Alkalinity of ash based = Bad'
plt.title('Alcalinity of ash depend quality (Good and Bad)')
sizes = [alcalinity_of_ash_quality_good, alcalinity_of_ash_quality_bad]
colors = ['green', 'red']

# Plot pie chart
plt.pie(sizes,  labels=labels, colors=colors,autopct='%1.1f%%', shadow=True, startangle=90)
plt.axis('equal')
plt.show()
```


    
![png](image/output_41_0.png)
    


# 3. Database Storage with PostgreSQL

### 3.1 Create a PostgreSQL database and table.


```python
df.dtypes
```




    classes                   int64
    alcohol                 float64
    malic_acid              float64
    ash                     float64
    alcalinity_of_ash       float64
    magnesium                 int64
    total_phenols           float64
    flavanoids              float64
    nonflavanoid_phenols    float64
    proanthocyanins         float64
    color_intensity         float64
    hue                     float64
    proline                 float64
    process_dt               object
    quality                    bool
    dtype: object




```python
from sqlalchemy import create_engine

# Replace with your database connection details
db_url = "postgresql://postgres:postgres@localhost:5432/airflow"

# Create an SQLAlchemy engine
engine = create_engine(db_url)

try:
    # Execute a SQL query
    query = """
        CREATE TABLE IF NOT EXISTS wine_data (
            classes int,
            alcohol float,
            malic_acid float,
            ash float,
            alcalinity_of_ash float,
            magnesium int,
            total_phenols float,
            flavanoids float,
            nonflavanoid_phenols float,
            proanthocyanins float,
            color_intensity float,
            hue float,
            proline float,
            quality boolean,
            process_date timestamp
    );
    """
    result = engine.execute(query)

    # Fetch and print the results
    query = """
      select *
      from wine_data
      limit 5
    """
    result = engine.execute(query)
    print("Length of result: {}".format(len(result.all())))

    result.close()

except Exception as e:
    print("Error:", e)

```

    Length of result: 0
    

### 3.2 Store the cleaned dataset.


```python
table_name = "wine_data"
try:
    # Insert the DataFrame into the PostgreSQL table
    df.to_sql(table_name, engine, if_exists='replace', index=False)
    print(f"Data inserted into the '{table_name}' table successfully.")

except Exception as e:
    print("Error:", e)


try:
    # Execute a SQL query
    query = """
      select *
      from wine_data
      limit 5
    """
    result = engine.execute(query)

    # Fetch and print the results
    for row in result:
        print(row)

    result.close()

except Exception as e:
    print("Error:", e)

```

    Data inserted into the 'wine_data' table successfully.
    (1, 14.23, 1.71, 2.43, 15.6, 127, 2.8, 3.06, 0.28, 2.29, 5.64, 1.04, 1065.0, '2023-10-25 12:00:00', True)
    (1, 13.2, 1.78, 2.14, 11.2, 100, 2.65, 2.76, 0.26, 1.28, 4.38, 1.05, 1050.0, '2023-10-25 12:00:00', False)
    (1, 13.16, 2.36, 2.67, 18.6, 101, 2.8, 3.24, 0.3, 2.81, 5.68, 1.03, 1185.0, '2023-10-25 12:00:00', False)
    (1, 14.37, 1.95, 2.5, 16.8, 113, 3.85, 3.49, 0.24, 2.18, 7.8, 0.86, 1480.0, '2023-10-25 12:00:00', True)
    (1, 13.24, 2.59, 2.87, 21.0, 118, 2.8, 2.69, 0.39, 1.82, 4.32, 1.04, 735.0, '2023-10-25 12:00:00', False)
    

# 4. Data Transfer with Airflow

### 4.1 Write an Apache Airflow DAG to transfer data from PostgreSQL to ClickHouse.

```
import json, time, datetime, logging
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago
from airflow.utils.trigger_rule import TriggerRule
from airflow import DAG
import pandas as pd


default_database ="postgres_connect"

# function get data from database postgresql
def func_data_in_db():
    # the sql
    SQL = """
            select *
            from public.wine_data;
    """
    # connect
    db = PostgresHook()
    df = db.get_pandas_df(SQL)
    return json.dumps(df.to_dict(orient='records'))

# function transfer data
def func_transfer_data(data):
    data=pd.DataFrame(json.loads(data))
    # convert data['classes'] (1 -> A, 2->B, 3->C)
    data['classes'] = [ 'A' if i==1 else ('B' if i==2 else 'C') for i in data['classes']]
    # convert data['quality'] (True -> 1, False->2)
    data['quality'] = (data['quality'])*1
    return json.dumps(data.to_dict(orient='records'))

# function return log
def func_logging(mess: str):
    return mess

# function insert into data to database clickhouse
def func_insert_data_clickhouse(data):
    import requests
    import json
    from io import BytesIO

    # Define the ClickHouse server URL and the table name
    clickhouse_url = 'http://172.17.0.1:8123'
    table_name = 'clickhousedb.wine_data'

    # Set the full URL for the HTTP insert endpoint
    insert_url = f'{clickhouse_url}/default/?user=clickhouse&password=clickhouse&query=INSERT INTO {table_name} FORMAT CSV settings format_csv_allow_single_quotes=0'

    try:
        data=json.loads(data)
        df = pd.DataFrame(data)
        df = df[['classes', 'alcohol', 'malic_acid', 'ash', 'alcalinity_of_ash',
       'magnesium', 'total_phenols', 'flavanoids', 'nonflavanoid_phenols',
       'proanthocyanins', 'color_intensity', 'hue', 'proline','quality', 'process_dt']]
        file = df.to_csv(sep=',',index=False, header=False, line_terminator='\n',encoding='utf-8').encode('utf-8')
        # Send an HTTP POST request to insert the JSON data
    
        print(df)
        print(file)
        response = requests.post(insert_url, data=BytesIO(file))

        # Check the response status
        if response.status_code == 200:
            print("Data inserted successfully.")
        else:
            print(f"Data insertion failed with status code: {response.txt}")
    except Exception as e:
        raise(f"Error: {str(e)}")

    return 'Success'

default_args = {
    "owner": "airflow",
    'email': ['trangiabao231097@gmail.com'],
	'email_on_failure': True,
	'email_on_retry': True,
}

with DAG(
    dag_id="Pipeline_Data_In_Homebase",
    default_args=default_args,
    start_date=days_ago(2),
    schedule_interval='@hourly',
    tags=['HOMEBASE'],
) as dag:

    result_data_in_db = PythonOperator(task_id = "func_data_in_db", 
                        python_callable = func_data_in_db)
    
    result_transfer_data = PythonOperator(task_id = "func_transfer_data", 
        python_callable = func_transfer_data, 
        op_kwargs={'data': result_data_in_db.output} 
    )


    result_insert_data_clickhouse = PythonOperator(task_id = "func_insert_data_clickhouse", 
        python_callable = func_insert_data_clickhouse, 
        op_kwargs={'data': result_transfer_data.output} 
    )

    result_logging_success = PythonOperator(task_id = "insert_data_clickhouse_success", 
        python_callable = func_logging, 
        op_kwargs={'mess': "INSERT DATA SUCCESS"},
        trigger_rule=TriggerRule.ONE_SUCCESS,
    )
    
    result_logging_fail = PythonOperator(task_id = "insert_data_clickhouse_fail", 
        python_callable = func_logging, 
        op_kwargs={'mess': "INSERT DATA FAIL"} ,
        trigger_rule=TriggerRule.ONE_FAILED,
    )

    result_data_in_db >> result_transfer_data >> result_insert_data_clickhouse 
    result_insert_data_clickhouse >> [result_logging_success, result_logging_fail]
```

![image.png](image/1.png)

![image.png](image/2.png)

![image.png](image/3.png)

### 4.2 Schedule the DAG to run every hour.

```
with DAG(
    dag_id="Pipeline_Data_In_Homebase",
    default_args=default_args,
    start_date=days_ago(2),
    schedule_interval='@hourly',
    tags=['HOMEBASE'],
) as dag:
```

![image.png](image/4.png)

# 5. Data Querying with ClickHouse

### 5.1 Write ClickHouse SQL queries to answer: 1. How many unique values are in variable X?


```
    SELECT uniqExact(alcohol) AS unique_count
    FROM clickhousedb.wine_data
    LIMIT 1;
```

![image.png](image/5.png)

### 5.2 Write ClickHouse SQL queries to answer: 2. What is the average of variable Y grouped by variable Z?


```
SELECT classes, AVG(alcalinity_of_ash) AS avg_of_alcalinity_of_ash
FROM clickhousedb.wine_data
GROUP BY classes
```

![image.png](image/6.png)


```python

```
