# EPMT Machine Learning Project (Q2+Q3 2026 - 10% time commitment)

## Core Objective

Exploratory machine learning effort undertaken to predict process CPU utilization (`cpu_time`) using metadata and historical performance logs extracted from the **EPMT (Experiment Process / Metadata Tool) database**.

* The overarching goal is to build a model capable of forecasting a process's computational footprint (`cpu_time`) *prior to or at the start of execution*. This would have gotten us one step closer to enabling more intelligent workload scheduling and resource allocation on high-performance computing (HPC) clusters.
* **Project Scope:** This work focuses on two foundational phases:
  * **Data Curation:** Extracting raw data from the EPMT database and engineering clean feature sets for ML training.
  * **Model Prototyping:** Executing multiple machine learning algorithms and evaluating performance using Mean Squared Error (MSE) and Coefficient of Determination ($R^2$).

This document also exists for the purpose of making it easier for me to one day remember what I tried. The [notebooks](#notebooks) created for this work are listed below. Warning: These notebooks are a bit messy.

## SPOILER - Not the Greatest Results

Unfortunately, this work failed to produce a model that predicts cpu_time well. The best $R^2$ results were 0.42 using the HistGradientBoostingRegressor.

While this exploratory phase did not yield a model ready for production, it successfully established a strong baseline. This successfully explains 42% of the variance in CPU utilization. However, this may suggest that a significant portion of `cpu_time` variance is likely driven by features not yet explored from or not yet captured by the EPMT database.

## Dataset Curation & Features

### Dataset Evolution
The project iteratively scaled the dataset size over multiple data extractions from the **EPMT database**:
* **Dataset 1:** 1,000 rows (Extracted 01/29/2026)
* **Dataset 2:** 10,000 rows (Extracted 01/29/2026)
* **Dataset 3:** 40,000 rows (Extracted 02/07/2026)
* **Dataset 4:** 133,872 rows (Extracted 03/19/2026)

### Feature Engineering & Encoding
The primary features were parsed from within the `EPMT_JOB_TAGS` field (found inside the `annotations` column). 
* **Categorical Data:** High-cardinality strings were transformed using one-hot encoding or engineered variants to control feature dimensionality.
* **Numerical Data:** Extracted metric values were mapped directly to the feature matrix.

### Training, Validation, Testing Dataset Split

Initially, I didn't explicitly set aside a testing dataset, which is not ideal. I also varied the training set size from 50% to 75% of the dataset at first.

Eventually, I settled on a better split for training, validation, and testing:
* **Training Set (60%):** Used to optimize model weights and parameters.
* **Validation Set (20%):** Used for hyperparameter tuning and architecture selection.
* **Testing Set (20%):** Held back entirely as a clean, unbiased final performance check.

### Target & Feature Distribution Adjustments (ex: [EPMTDataCleanup](EPMTDataCleanup.ipynb))
* **Skew Correction:** Evaluated feature distributions using the Pandas `.skew()` method. Applied NumPy log transformations (`np.log1p`) to compensate for heavily right-skewed tail distributions.
* **Outlier Mitigation:** Identified extreme anomalies using the Interquartile Range (IQR) method:
  * $IQR = Q3 - Q1$
  * $\text{Lower Bound} = Q1 - 1.5 \times IQR$
  * $\text{Upper Bound} = Q3 + 1.5 \times IQR$
  * Data points falling outside these boundaries were flagged, clipped, or pruned.

### Correlation Analysis
Correlation heatmaps were utilized to visualize out the relationships between features and the target variable (`cpu_time`).  In some experiments, features not well correlated to the cpu_time were dropped.

## 🤖 Algorithms & Models Explored
The following machine learning architectures were explored during this project:

| Model / Architecture | Context | Documentation & Resources |
| :--- | :--- | :--- |
| **LinearRegressor** | Baseline. Gauge of linear relationships. | [Scikit-Learn LinearRegression](https://scikit-learn.org/stable/modules/generated/sklearn.linear_model.LinearRegression.html) |
| **Support Vector Regression (SVR)** | Non-linear feature maps using kernels. | [Scikit-Learn SVR](https://scikit-learn.org/stable/modules/generated/sklearn.svm.SVR.html) |
| **DecisionTree** | Splits data into logical, orthogonal paths based on explicit feature thresholds. | [Scikit-Learn DecisionTreeRegressor](https://scikit-learn.org/stable/modules/generated/sklearn.tree.DecisionTreeRegressor.html) |
| **RandomForest** | Averages independent trees to reduce variance and handle extreme job outliers | [Scikit-Learn RandomForestRegressor](https://scikit-learn.org/stable/modules/generated/sklearn.ensemble.RandomForestRegressor.html) |
| **HistGradientBoostingRegressor** | Sequentially builds trees to fix prior errors using binned feature histograms. | [Scikit-Learn HistGradientBoostingRegressor](https://scikit-learn.org/stable/modules/generated/sklearn.ensemble.HistGradientBoostingRegressor.html) |
| **Multi-Layer Perceptron (MLP)** | I think I just wanted to try at least one deep learning architecture... | [Scikit-Learn MLPRegressor](https://scikit-learn.org/stable/modules/generated/sklearn.neural_network.MLPRegressor.html) |
| **Voting Ensemble** | Combines different models | [Scikit-Learn VotingRegressor](https://scikit-learn.org/stable/modules/generated/sklearn.ensemble.VotingRegressor.html) |

---

## 📈 Metrics & Results

### Performance Metrics
The models were evaluated using the following metrics:
* Mean Squared Error (MSE)
* R-squared ($R^2$ Score)

---

## 💻 Code Snippets
Below are example snippets demonstrating how the models were split, trained and evaluated using sklearn.

```python
# Example: Training (60%), Validation (20%), Testing (20%) Dataset Split
from sklearn.model_selection import train_test_split

X_train_val, X_test, y_train_val, y_test = train_test_split(X, y, test_size=0.2, random_state=42)
X_train, X_val, y_train, y_val = train_test_split(X_train_val, y_train_val, test_size=0.25, random_state=42)
```

```python
# Example: Training the HistGradientBoostingRegressor
from sklearn.ensemble import HistGradientBoostingRegressor
from sklearn.metrics import mean_squared_error, r2_score

gb_model = HistGradientBoostingRegressor(max_iter=100, learning_rate=0.1, max_depth=10, random_state=42)
gb_model.fit(X_train, y_train)

gb_val_pred = gb_model.predict(X_val)
gb_mse = mean_squared_error(y_val, gb_val_pred)
gb_r2 = r2_score(y_val, gb_val_pred)

print(f"Mean Squared Error: {gb_mse:.2f}")
print(f"R-squared Score: {gb_r2:.2f}")
```

## Notebooks

The following notebooks were generated as a result of this project. The naming convention isn't important. The notebooks themselves contain code to extract data from the EPMT database, expore and extract features, and finally train and evaluate ML models in a feable attempt to predict cpu_time. There is, unfortunately, much repetition and redundancy in these notebooks. This table will hopefully provide an easier way to navigate through them and point out the less redundant parts.

| Index | Notebook | Description | Dataset Size | Num. Features | Best Results |
| :--- | :--- | :--- | :---: | :---: | :--- |
| 1 | [JK_EPMT_Play1.ipynb](https://github.com/jjuyeonkim/ilaflott_epmt/blob/epmt_play/notebooks/JK_EPMT_Play1.ipynb) | Queries a small set of EPMT Data and writes to a csv file. The focus of this notebook was the EPMT_JOB_TAGS annotations. A little bit of visualization is also added to the end. Linear Regression model was trained. The validation and testing sets are blurred. | 1,000 | 109 | LinearRegression<br>MSE: 9.4 x 10^14<br> $R^2$: 0.62<br>Training 75%<br>Validation Set: 25%<br> Testing Set:0% |
| 2 | [JK_EPMT_Play2.ipynb](https://github.com/jjuyeonkim/ilaflott_epmt/blob/epmt_play/notebooks/JK_EPMT_Play2.ipynb) | Same as JK_EPMT_Play1, except dropped exp_time. Playing around with correlation heatmaps. Huge drop in $R^2$ when removing exp_time, even though correlation to cpu_time isn't that big. [NOTE: I didn't realize until later that my training/validation set is different than in Play1. This could have also accounted for the drop in r_squared. This was dumb on my part.] | 1000 | 108 | LinearRegression -- $R^2$: 0.06<br> Training 50%<br>Validation Set: 50%<br> Testing Set:0% |
| 3 | [JK_EPMT_Play3.ipynb](https://github.com/jjuyeonkim/ilaflott_epmt/blob/epmt_play/notebooks/JK_EPMT_Play3.ipynb) | Further reduction compared to Play1 of features. Features based off of exp_time, exp_component, exp_platform, exp_target. | 10,000 | 54 | Linear Regression--<br>MSE: 3.24 x 10^18<br>$R^2$: 0.03<br><br>SVM--<br>$R^2$: -4.6<br> <br>Training 75%<br>Validation Set: 25%<br> Testing Set:0%|
| 4 | [JK_EPMT_ExploreFeatures](https://github.com/jjuyeonkim/ilaflott_epmt/blob/epmt_play/notebooks/JK_EPMT_ExploreFeatures.ipynb) | Explore beyond the 'EPMT_JOB_TAGS' within 'annotations' for rows within the EPMT database. Queried around 40k rows, but things were commented out or only partially finished. ML experiments not run in this notebook. Eventually, the additional features that were explored weren't used because the features were collected after the experiments were run (i.e., they couldn't be used for prediction). | 100 to ~40k | N/A | N/A |
| 5 | [EPMTDataCleanup.ipynb](https://github.com/jjuyeonkim/ilaflott_epmt/blob/epmt_play/notebooks/EPMTDataCleanup.ipynb) | Taking 40k rows of EPMT data pulled from the database and written to file on Feb 7th, 2026, and cleaning that up so that it can be more easily used for Machine Learning. Normalization + IQR for outlier handling. Optimistic "cheating" case where we have trained a Linear Regression using features from the EPMT database that we won't have at inference time. "Cheating" upper bound features include: duration, time_waiting, read_bytes, write_bytes, minflt, majflt. These features were included to train a model that represents a really optimistic and unrealistic scenario, where we had runtime features to help us predict the cpu_time. | 40k | 216 or 223 | $R^2$: 0.3 (less Cheating), 0.93 (Cheating a bit. Don't take this too seriously)<br>Training 75%<br>Validation Set: 25%<br> Testing Set:0% |
| 6 | [EPMTDataCleanup_Next123.ipynb](https://github.com/jjuyeonkim/ilaflott_epmt/blob/epmt_play/notebooks/EPMTDataCleanup_Next123.ipynb) | Following up from EPMTDataCleanup.ipynb; Removing GROUP information from features; Because script_name AND exp_name appear too correlated, removing script_name and keeping exp_name; Only using bronx-23 data. Final features based off of SLURM_NTASKS, exp_time, exp_seg_months, LOADEDMODULES, exp_component, exp_fre_mod, exp_name, exp_platform, exp_target.  | 39,998 | 120 | Voting Ensemble including:<ul><li>LinearRegressor</li><li>DecisionTree</li><li>RandomForest</li><li>Boosting</li><li>Multi-Layer Perceptron</li></ul><br>Best R-squared was .44 <br><br>Training 75%<br>Validation Set: 25%<br> Testing Set:0%|
| 7 | [EPMTDataCleanup_Next5.ipynb](https://github.com/jjuyeonkim/ilaflott_epmt/blob/epmt_play/notebooks/EPMTDataCleanup_Next5.ipynb) | More EPMT Cleanup - Next Idea No. "5"; Following up from EPMTDataCleanup_Next123.ipynb; Try starting from a small set of features; We determined which features are highly correlated to cpu_time and added them one at a time to predict. NOTE: Only num_features=85 is shown in the full notebook, but you can replace the num_features value to see the the r_squared values in the table results:<br><code>target = 'cpu_time'<br>features = df.corr(numeric_only=True)['cpu_time'].sort_values(ascending=False).keys().tolist()[1:n_features+1]</code> | 40k | 1 - 121 |<table><tr><th>num_features</th><th>$R^2$</th><tr><td>1</td><td>0.11</td></tr><tr><td>2</td><td>0.17</td></tr><tr><td>3</td><td>0.18</td></tr><tr><td>5</td><td>.21</td></tr><tr><td>45</td><td>.34</td></tr><tr><td>70</td><td>.39</td></tr><tr><td>80</td><td>.43</td></tr><tr><td>85</td><td>.44</td></tr><tr><td>90</td><td>.44</td></tr><tr><td>100<td>.44</td></tr></table> Best r_squared: 0.44 |
| 8 | [EPMTDataCleanup_Next7.ipynb](https://github.com/jjuyeonkim/ilaflott_epmt/blob/epmt_play/notebooks/EPMTDataCleanup_Next7.ipynb) | GridSearchCV - Trying a little bit of hyper parameter tuning for RandomForestRegressor and HistGradientBoostingRegressor<br>Also, added back in SLURM_JOB_ACCOUNT (ex., gfdl_w), but it didn't seem to make a difference even though it's highly correlated to the cpu_time target. | 29,512 | 123 | Best $R^2$: 0.44<br><br>Training 75%<br>Validation Set: 25%<br> Testing Set:0% |
| 9 | [JK_EPMT_PullData.ipynb](https://github.com/jjuyeonkim/ilaflott_epmt/blob/epmt_play/notebooks/JK_EPMT_PullData.ipynb) | Pulls more data from the epmt database and put them into csv files, chunking them because I had trouble holding data in memory over 40k rows. I was able to save over 100k rows this way, but only a subset of tags are captured here. | N/A | N/A | N/A | N/A |
| 10 | [EPMTDataCleanup_Next11a.ipynb](https://github.com/jjuyeonkim/ilaflott_epmt/blob/epmt_play/notebooks/EPMTDataCleanup_Next11a.ipynb) | Using more data (100k+) rows pulled using JK_EPMT_PullData<br>Following up from EPMTDataCleanup_Next7.ipynb; Keeping only columns 'cpu_time' (target), 'SLURM_JOB_ACCOUNT', 'exp_component', 'exp_name', 'exp_time', 'exp_platform', 'exp_target', 'exp_seg_months' with exit code 0. No model training/evaluation. | 106,447 | N/A | N/A |
| 11 | [EPMTDataCleanup_Next11b.ipynb](https://github.com/jjuyeonkim/ilaflott_epmt/blob/epmt_play/notebooks/EPMTDataCleanup_Next11b.ipynb) | Features based on exp_time, exp_target, exp_platform, exp_component, exp_name (c_XX, am_X), and SLURM_JOB_ACCOUNT features. | 106,447 | 56 | HistGradientBoostingRegressor<br><br>Best $R^2$: 0.42<br><br>Training 60%<br>Validation Set: 20%<br> Testing Set:20% |
| 12 | [EPMTDataCleanup_Next11e.ipynb](https://github.com/jjuyeonkim/ilaflott_epmt/blob/epmt_play/notebooks/EPMTDataCleanup_Next11e.ipynb) | Features based on exp_component and SLURM_JOB_ACCOUNT | 106,447 | 38 | VotingRegressor Ensemble including RandomForestRegressor, DecisionTree, MLPRegressor, HistGradientBoostingRegressor<br><br>Best $R^2$: 0.27<br><br>Training 60%<br>Validation Set: 20%<br> Testing Set:20% |







## Retrospective Analysis & Future Engineering Roadmap

Reflecting on the initial phase of this exploratory effort highlights several pivot points, data constraints, and potential for future iterations. Looking back now, I wish I'd done a few things differently.

### 1. Understand the Data Better
* **The Lesson:** Extracting data without understanding the underlying lifecycle of the database fields created a blind spot. 
* **Future Action:** Actually running some post-processing jobs to understand better how data is being populated within the EPMT data base could have provided more insight on the data itself. If I could go back in time, I'd try this exercise before blindly extracting the data and using it. Understanding the data generation process beforehand will prevent feeding the models noisy features.

### 2. Long-term Data Harvesting
* **The Lesson:** The current EPMT database is restricted to a 3-week window, limiting the models' exposure to broader system patterns.
* **Future Action:** Deploy automated lightweight cron jobs or data pipeline scripts to pull and append incremental 3-week snapshots over a rolling 12-month period. Scaling the data from ~133k rows to an estimated 1 to 2 million rows will provide the data volume necessary to properly train high-capacity deep learning models (MLPs) and capture long-term seasonal shifts in cluster utilization.

### 3. Try different ML Models/Algorithms
To break past the current $R^2$ ceiling of 0.42, future iterations could move beyond standard Scikit-Learn structures.

### 4. Save the Run Scripts
The current feature space is restricted to flat metadata tags. Predictive accuracy could be heavily boosted by capturing the actual scripts being submitted and transforming them into textual embeddings would give the model features from the actual computational logic of the job.

### 5. Transitioning to Tailored Optimization Metrics
Relying strictly on Mean Squared Error (MSE) and standard $R^2$ treats all prediction errors equally. For workload scheduling, errors are asymmetrical. Future iterations could implement and optimize for different metrics such as:
* **Mean Absolute Percentage Error (MAPE):** Evaluates error relative to the job size (e.g., being off by 5 minutes matters immensely for a 10-minute job, but is negligible for a 24-hour job).
* **Custom Loss Functions:** TO BE DETERMINED

### 6. More Time

More than 4 hours per week would likely have helped.

---

*Disclaimer: The foundational structural template and context for this retrospective report were generated with the assistance of Gemini, and subsequently vetted, expanded, and populated with actual engineering findings.*
