# EPMT Machine Learning Project (Q2+Q3 2026 - 10% time commitment)

## 📋 Summary of Work
Data Curation:
  - Extracted Data from the EPMT database
  - Attempted to clean up data for ML Training

Model Prototyping to predict cpu_time:
  - Executed different ML algorithms using extracted datasets
  - Evaluated models using R_squared and MSE

**Notebooks:** [GitHub - ilaflott_epmt (epmt_play branch)](https://github.com/jjuyeonkim/ilaflott_epmt/tree/epmt_play)
Only a subset of the notebooks in directory were created as a result of this work. Some notebooks already existed.


**TODO - expand this paragraph**: With the best R_squared value at .44 and the mean squared error at <fill_in_blank>, the trained models as a part of this project aren't predicting cpu_time well. (https://en.wikipedia.org/wiki/Coefficient_of_determination, https://www.investopedia.com/terms/r/r-squared.asp)

## 📊 Dataset Curation & Features

### Dataset Overview
The data for this project was sourced entirely from the **EPMT database**.
* **Dataset 1:** 1,000 rows (pulled 1/29/2026)
* **Dataset 2:** 10,000 rows (pulled 1/29/2026)
* **Dataset 3:** 40,000 rows (pulled 2/7/2026)
* **Dataset 4:** 133,872 rows (pulled 3/19/2026)


### Data Splits & Features
* **Features Used:** 
Part of the work entailed looking at features and and seeing the results. Ideally, all notebooks would have used split out data for training, validation and testing. However, not all notebooks do this, especially early on.

Categorical features converted to numerical using one-hot encoding.

In general, the break up of the data for training, validation and testing, were allocated in the following way:
  * **Training Set:** `XX%`
  * **Validation Set:** `XX%`
  * **Testing Set:** `XX%`

### Outlier Handling and Normalization

IQR - Trying to address outliers
* Compute Q1 (25th percentile), Q3 (75th percentile), IQR (Q3-Q1)
* Pick some outlier thresholds, suggested
* lower: Q1-1.5*IQR
* upper: Q3-1.5*IQR
* Flag or remove/clip outliers

Normalization
* TODO

### Outlier Handling and Normalization

TODO

### Correlation Heatmaps

TODO

## 🤖 Algorithms & Models Attempted
The following machine learning architectures were evaluated during this project:

1. **LinearRegressor:** Baseline model to establish a performance floor.
2. **DecisionTree:** To capture non-linear relationships.
3. **RandomForest:** Ensemble method to reduce variance and overfitting.
4. **Boosting:** (e.g., Gradient Boosting/XGBoost) For sequential error correction.
5. **Multi-Layer Perceptron (MLP):** Neural network approach to map complex feature spaces.
6. **Voting Ensemble:** Combining the strengths of the top-performing models.
7. **SVM:** TODO
---

## 📈 Metrics & Results

### Performance Metrics
The models were evaluated using the following metrics:
* Mean Squared Error (MSE)
* R-squared ($R^2$ Score)

### Comparison Table

| Model | Training Accuracy | Validation Accuracy | Testing Accuracy | Notes |
| :--- | :---: | :---: | :---: | :--- |
| **LinearRegressor** | `0.00%` | `0.00%` | `0.00%` | ?? |
| **DecisionTree** | `0.00%` | `0.00%` | `0.00%` | ?? |
| **RandomForest** | `0.00%` | `0.00%` | `0.00%` | ?? |
| **Boosting** | `0.00%` | `0.00%` | `0.00%` | ?? |
| **Multi-Layer Perceptron**| `0.00%` | `0.00%` | `0.00%` | ?? |
| **Voting Ensemble** | `0.00%` | `0.00%` | `0.00%` | Combined model results |

> 🏆 **Best Accuracy Achieved:** `XX.XX%` using the **[Insert Best Model Name Here]**.

---

## 💻 Code Snippets
Below is an example snippet demonstrating how the models were trained and evaluated using sklearn.

```python
###### TODO: SWAP FOR RELEVANT SNIPPETS

# Example: Training the Voting Ensemble
from sklearn.ensemble import VotingRegressor
from sklearn.metrics import r2_score

# Initialize the ensemble with our top models
voting_model = VotingRegressor(estimators=[
    ('rf', random_forest_model),
    ('boost', boosting_model)
])

# Fit and predict
voting_model.fit(X_train, y_train)
predictions = voting_model.predict(X_test)

# Evaluate
accuracy = r2_score(y_test, predictions)
print(f"Voting Ensemble R2 Score: {accuracy:.4f}")
```

## Notebooks

The following notebooks were generated as a result of this project. The naming convention isn't important. The notebooks themselves contain code to extract data from the EPMT database, expore and extract features, and finally train and evaluate ML models in a feable attempt to predict cpu_time. There is, unfortunately, much repetition and redundancy in these notebooks. This table will hopefully provide an easier way to navigate through them and point out the less redundant parts.

| Index | Notebook | Description | Dataset Size | Num. Features | Best Results |
| :--- | :--- | :--- | :---: | :---: | :--- |
| 1 | [JK_EPMT_Play1.ipynb](https://github.com/jjuyeonkim/ilaflott_epmt/blob/epmt_play/notebooks/JK_EPMT_Play1.ipynb) | Queries a small set of EPMT Data and writes to a csv file. The focus of this notebook was the EPMT_JOB_TAGS annotations. A little bit of visualization is also added to the end. Linear Regression model was trained. The validation and testing sets are blurred. | 1,000 | 109 | LinearRegression<br>MSE: 9.4 x 10^14<br> $R^2$: 0.62<br>Training 75%<br>Validation Set: 25%<br> Testing Set:0% |
| 2 | [JK_EPMT_Play2.ipynb](https://github.com/jjuyeonkim/ilaflott_epmt/blob/epmt_play/notebooks/JK_EPMT_Play2.ipynb) | Same as JK_EPMT_Play1, except dropped exp_time. Example correlation. | 1000 | ? | LinearRegression -- $R^2$: 0.06 |
| 3 | [JK_EPMT_Play3.ipynb](https://github.com/jjuyeonkim/ilaflott_epmt/blob/epmt_play/notebooks/JK_EPMT_Play3.ipynb) | Further reducing cardinality of features | 10,000 | ? | Linear Regression--<br>MSE: 3.24 x 10^18<br>$R^2$: 0.03<br><br>SVM--<br>$R^2$: -4.6 |
| 4 | [JK_EPMT_ExploreFeatures](https://github.com/jjuyeonkim/ilaflott_epmt/blob/epmt_play/notebooks/JK_EPMT_ExploreFeatures.ipynb) | This notebook is trying to explore beyond the 'EPMT_JOB_TAGS' within 'annotations' for rows within the EPMT database. This notebook was used to query around 40k rows, but things were commented out or only partially finished. ML Experiments not run.| 100 to ~40k | N/A | N/A |
| 5 | [JK_EPMT_PullData.ipynb](https://github.com/jjuyeonkim/ilaflott_epmt/blob/epmt_play/notebooks/JK_EPMT_PullData.ipynb) | This notebook is trying to pull data from the epmt database and put them into csv files. I was chunking them because I seem to have trouble holding data in memory over 40k rows. I was able to save over 100k rows this way. Only a subset of tags are captured here. | N/A | N/A | N/A | N/A |
| 6 | [EPMTDataCleanup.ipynb](https://github.com/jjuyeonkim/ilaflott_epmt/blob/epmt_play/notebooks/EPMTDataCleanup.ipynb) | Taking 40k rows of EPMT data pulled from the database and written to file on Feb 7th, 2026, and cleaning that up so that it can be more easily used for Machine Learning. Normalization + IQR for outlier handling. Optimistic "cheating" case where we have trained a Linear Regression using features from the EPMT database that we won't have at inference time. Cheating features include: duration, time_waiting, read_bytes, write_bytes, minflt, majflt. These features were included to train a model that represents a really optimistic and unrealistic scenario, where we had runtime features to help us predict the cpu_time. | 40k | 216, 223 | $R^2$: 0.3 (less Cheating), 0.93 (Cheating a bit. Don't take this too seriously)<br>Training 75%<br>Validation Set: 25%<br> Testing Set:0% |
| 7 | [EPMTDataCleanup_Next123.ipynb](https://github.com/jjuyeonkim/ilaflott_epmt/blob/epmt_play/notebooks/EPMTDataCleanup_Next123.ipynb) | Following up from EPMTDataCleanup.ipynb; Try without group information; Maybe not needing script_name AND exp_name -- too correlated (likely) - Taking out script_name and keeping exp_name; bronx-23 --- try this one only; With this data, we're trying just a bunch of models on it to see what happens.  | 39,998 | 120 | Voting Ensemble including:<ul><li>LinearRegressor</li><li>DecisionTree</li><li>RandomForest</li><li>Boosting</li><li>Multi-Layer Perceptron</li></ul><br>Best R-squared was .44 <br><br>Training 75%<br>Validation Set: 25%<br> Testing Set:0%|
| 8 | [EPMTDataCleanup_Next5.ipynb](https://github.com/jjuyeonkim/ilaflott_epmt/blob/epmt_play/notebooks/EPMTDataCleanup_Next5.ipynb) | More EPMT Cleanup - Next Idea No. "5"; Following up from EPMTDataCleanup_Next123.ipynb; Try starting from a small set of features; We determined which features are highly correlated to cpu_time and added them one at a time to predict. NOTE: Only num_features=85 is shown in the full notebook, but you can replace the num_features value to see the the r_squared values in the table results:<br><code>target = 'cpu_time'<br>features = df.corr(numeric_only=True)['cpu_time'].sort_values(ascending=False).keys().tolist()[1:n_features+1]</code> | 40k | 1 - 121 |<table><tr><th>num_features</th><th>$R^2$</th><tr><td>1</td><td>0.11</td></tr><tr><td>2</td><td>0.17</td></tr><tr><td>3</td><td>0.18</td></tr><tr><td>5</td><td>.21</td></tr><tr><td>45</td><td>.34</td></tr><tr><td>70</td><td>.39</td></tr><tr><td>80</td><td>.43</td></tr><tr><td>85</td><td>.44</td></tr><tr><td>90</td><td>.44</td></tr><tr><td>100<td>.44</td></tr></table> Best r_squared: 0.44 |
| 9 | [EPMTDataCleanup_Next7.ipynb](https://github.com/jjuyeonkim/ilaflott_epmt/blob/epmt_play/notebooks/EPMTDataCleanup_Next7.ipynb) | GridSearchCV - Trying a little bit of hyper parameter tuning for RandomForestRegressor and HistGradientBoostingRegressor<br>Also, added back in SLURM_JOB_ACCOUNT (ex., gfdl_w), but it didn't seem to make a difference even though it's highly correlated to the cpu_time target. | 29,512 | 123 | Best $R^2$: 0.44<br><br>Training 75%<br>Validation Set: 25%<br> Testing Set:0% |
| 10 | [EPMTDataCleanup_Next11a.ipynb](https://github.com/jjuyeonkim/ilaflott_epmt/blob/epmt_play/notebooks/EPMTDataCleanup_Next11a.ipynb) | More data - 100k+ rows<br>Following up from EPMTDataCleanup_Next7.ipynb; Keeping only columns 'cpu_time', 'SLURM_JOB_ACCOUNT', 'exp_component', 'exp_name', 'exp_time', 'exp_platform', 'exp_target', 'exp_seg_months' with exit code 0. No model training/evaluation. | 106,447 | N/A | N/A |
| 11 | [EPMTDataCleanup_Next11b.ipynb](https://github.com/jjuyeonkim/ilaflott_epmt/blob/epmt_play/notebooks/EPMTDataCleanup_Next11b.ipynb) | One-hot encoded features based on exp_time, exp_target, exp_platform, exp_component, exp_name (c_XX, am_X), and SLURM_JOB_ACCOUNT features. | 106,447 | 56 | HistGradientBoostingRegressor<br><br>Best $R^2$: 0.42<br><br>Training 60%<br>Validation Set: 20%<br> Testing Set:20% |
| 12 | [EPMTDataCleanup_Next11e.ipynb](https://github.com/jjuyeonkim/ilaflott_epmt/blob/epmt_play/notebooks/EPMTDataCleanup_Next11e.ipynb) | One-hot encoded features based on exp_component and SLURM_JOB_ACCOUNT | 106,447 | 38 | VotingRegressor Ensemble including RandomForestRegressor, DecisionTree, MLPRegressor, HistGradientBoostingRegressor<br><br>Best $R^2$: 0.27<br><br>Training 60%<br>Validation Set: 20%<br> Testing Set:20% |

## Mis-steps and Possible Future Steps
If I were to redo how I spent my time, I might do things differently.

### Understand the data better
Actually running some post-processing jobs to understand better how data is being populated within the EPMT data base could have provided more insight on the data itself. If I could go back in time, I'd try this exercise before blindly extracting the data and using it.

### Expand Dataset collection over a longer period of time

Current EPMT database is limited to roughly 3 weeks worth of data at a given point in time. It may be useful to take time to write scripts to collect EPMT data over a longer period of time to use as training data. To clarify, this doesn’t mean a process for backing up EPMT data in its entirety. Instead, it may be writing scripts to parse through archival data or setting up cron jobs to pull from the existing EPMT data on regular 3 week intervals. If we collected data over a year, I estimate we could have 1-2 million rows. This would be a much larger data set.

### Try different ML algorithms

TODO: list some

### Try different Features

TODO: list some

* If we stored the actual scripts being run, we could potentially extract useful features to train on.

### Explore Better Metrics

I used R_squared and MSE for these notebooks, but there are more metrics out there to explore. 

TODO: list some

### More Time

More than 4 hours per week would likely have helped.

### And more and more...

TODO: Compile a list from the "Next Step" sections of the notebooks
* Try with and without group information
* Use only bronx-23 rows
* Break up "ocean" exp_component --- even further... 

DISCLAIMER: The template for this document was generated with the help of Gemini. It is currently being expanded, vetted, and populated with work results.