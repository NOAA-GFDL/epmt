# EPMT Machine Learning Project (Q2+Q3 2026 - 10% time commitment)

## 📋 Summary of Work
Data Curation:
  - Extracted Data from the EPMT database
  - Attempted to clean up data for ML Training

Model Prototyping:
  - Executed different ML algorithms using extracted datasets
  - Evaluated models using R_squared


**Notebooks:** [GitHub - ilaflott_epmt (epmt_play branch)](https://github.com/jjuyeonkim/ilaflott_epmt/tree/epmt_play)
This repository directory holds only a subset of jupyter notebooks created as a result of this work. Some notebooks already existed.


**TODO - expand this paragraph**: With the best R_squared value at .44 and the mean squared error at <fill_in_blank>, the trained models as a part of this project aren't predicting cpu_time well. (https://en.wikipedia.org/wiki/Coefficient_of_determination, https://www.investopedia.com/terms/r/r-squared.asp)

## 📊 Dataset Curation & Features

### Dataset Overview
The data for this project was sourced entirely from the **EPMT database**.
* **Subset 1:** 1,000 data points (pulled ??/??/2026)
* **Subset 2:** 40k data points (pulled 2/7/2026)
* **Subset 3:** 133,872 data points (pulled 3/19/2026)


### Data Splits & Features
* **Features Used:** [Insert brief description of the input features/columns used here]
* **Data Allocation:**
  * **Training Set:** `XX%`
  * **Validation Set:** `XX%`
  * **Testing Set:** `XX%`

---

## 🤖 Algorithms & Models Attempted
The following machine learning architectures were evaluated during this project:

1. **LinearRegressor:** Baseline model to establish a performance floor.
2. **DecisionTree:** To capture non-linear relationships.
3. **RandomForest:** Ensemble method to reduce variance and overfitting.
4. **Boosting:** (e.g., Gradient Boosting/XGBoost) For sequential error correction.
5. **Multi-Layer Perceptron (MLP):** Neural network approach to map complex feature spaces.
6. **Voting Ensemble:** Combining the strengths of the top-performing models.

---

## 📈 Metrics & Results

### Performance Metrics
The models were evaluated using the following metrics:
* [e.g., Root Mean Squared Error (RMSE)]
* [e.g., R-squared ($R^2$ Score)]

### Comparison Table

| Model | Training Accuracy | Validation Accuracy | Testing Accuracy | Notes |
| :--- | :---: | :---: | :---: | :--- |
| **LinearRegressor** | `0.00%` | `0.00%` | `0.00%` | Baseline performance |
| **DecisionTree** | `0.00%` | `0.00%` | `0.00%` | Overfit on training data |
| **RandomForest** | `0.00%` | `0.00%` | `0.00%` | Better generalization |
| **Boosting** | `0.00%` | `0.00%` | `0.00%` | High training time |
| **Multi-Layer Perceptron**| `0.00%` | `0.00%` | `0.00%` | Struggled with convergence|
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


| Index | Notebook | Description | Dataset Size | Num. Features | Best Results |
| :--- | :--- | :--- | :---: | :---: | :--- |
| 1 | [JK_EPMT_Play1.ipynb](https://github.com/jjuyeonkim/ilaflott_epmt/blob/epmt_play/notebooks/JK_EPMT_Play1.ipynb) | Query EPMT Data into a simple csv file. The focus of this notebook is the EPMT_JOB_TAGS annotations for now, but the data will eventually be expanded. A little bit of visualization is also added to the end. | 1000 | 109 | LinearRegression -- MSE: 947203385827177.75 / $R^2$: 0.62 |
| 2 | [JK_EPMT_Play2.ipynb](https://github.com/jjuyeonkim/ilaflott_epmt/blob/epmt_play/notebooks/JK_EPMT_Play2.ipynb) | Same as JK_EPMT_Play1, except dropped exp_time. Example correlation. | 1000 | ? | LinearRegression -- $R^2$: 0.06 |
| 3 | [JK_EPMT_Play3.ipynb](https://github.com/jjuyeonkim/ilaflott_epmt/blob/epmt_play/notebooks/JK_EPMT_Play3.ipynb) | ? | ? | ? | $R^2$: ? |
| 4 | [JK_EPMT_ExploreFeatures](https://github.com/jjuyeonkim/ilaflott_epmt/blob/epmt_play/notebooks/JK_EPMT_ExploreFeatures.ipynb) | This notebook is trying to explore beyond the 'EPMT_JOB_TAGS' within 'annotations' for rows within the EPMT database. This notebook was used to query around 40k rows, but things were commented out or only partially finished. ML Experiments not run.| 100 to ~40k | N/A | N/A |
| 5 | [JK_EPMT_PullData.ipynb](https://github.com/jjuyeonkim/ilaflott_epmt/blob/epmt_play/notebooks/JK_EPMT_PullData.ipynb) | ? | ? | ? | ? | $R^2$: ? |
| 6 | [EPMTDataCleanup.ipynb](https://github.com/jjuyeonkim/ilaflott_epmt/blob/epmt_play/notebooks/EPMTDataCleanup.ipynb) | Taking 40k rows of EPMT data pulled from the database and written to file on Feb 7th, 2026, and cleaning that up so that it can be more easily used for Machine Learning. Revised version will be written to file as csv, hopefully with data normalization and outliers removed. | ? | ? | ? | $R^2$: ? |
| 7 | [EPMTDataCleanup_Next123.ipynb](https://github.com/jjuyeonkim/ilaflott_epmt/blob/epmt_play/notebooks/EPMTDataCleanup_Next123.ipynb) | Following up from EPMTDataCleanup.ipynb; Try without group information; Maybe not needing script_name AND exp_name -- too correlated (likely) - Taking out script_name and keeping exp_name; bronx-23 --- try this one only; With this data, we're trying just a bunch of models on it to see what happens.  | ? | ? | <ul><li>LinearRegressor</li><li>DecisionTree</li><li>RandomForest</li><li>Boosting</li><li>Multi-Layer Perceptron</li><li>Voting Ensemble</li></ul><br>Best R-squared was .44 |
| 8 | [EPMTDataCleanup_Next5.ipynb](https://github.com/jjuyeonkim/ilaflott_epmt/blob/epmt_play/notebooks/EPMTDataCleanup_Next5.ipynb) | ? | ? |  ? |$R^2$: ? |
| 9 | [EPMTDataCleanup_Next7.ipynb](https://github.com/jjuyeonkim/ilaflott_epmt/blob/epmt_play/notebooks/EPMTDataCleanup_Next7.ipynb) | ? | ? | ? | $R^2$: ? |
| 10 | [EPMTDataCleanup_Next11a.ipynb](https://github.com/jjuyeonkim/ilaflott_epmt/blob/epmt_play/notebooks/EPMTDataCleanup_Next11a.ipynb) | ? | ? | ? |$R^2$: ? |
| 11 | [EPMTDataCleanup_Next11b.ipynb](https://github.com/jjuyeonkim/ilaflott_epmt/blob/epmt_play/notebooks/EPMTDataCleanup_Next11b.ipynb) | ? | ? | ? |$R^2$: ? |
| 12 | [EPMTDataCleanup_Next11e.ipynb](https://github.com/jjuyeonkim/ilaflott_epmt/blob/epmt_play/notebooks/EPMTDataCleanup_Next11e.ipynb) | ? | ? | ? |$R^2$: ? |


### TO BE PROCESSED:

* EPMTDataCleanup_Next5
EPMT Data "Cleanup" - Next Idea No. "5"
Following up from EPMTDataCleanup_Next123.ipynb
5. Try starting from a small set of features

We determined which features are highly correlated to cpu_time and added them one at a time to predict. NOTE: Only num_features=85 is shown below in the full notebook, but you can replace the num_features value to see the the r_squared values in the table below.

target = 'cpu_time'
features = df.corr(numeric_only=True)['cpu_time'].sort_values(ascending=False).keys().tolist()[1:n_features+1]

num_features	r_squared
1	0.11
2	0.17
3	0.18
5	.21
45	.34
70	.39
80	.43
85	.44
90	.44
100	.44
Dataset size: 30k
Num Features: 121
Best r_squared: 0.44


DISCLAIMER: The template for this document was generated with the help of Gemini. It is currently being expanded, vetted, and populated with work results.