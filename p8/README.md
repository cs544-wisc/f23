# P8 (6% of grade): BigQuery, Loans Data

## Overview

In this project, we'll (a) study the geography of loans in WI using
BigQuery and (b) make predictions about loan amounts.  You'll be
combining data from several sources: a public BigQuery dataset
describing the geography of US counties, the HDMA loans dataset used
previously this semester, and pretend loan applications made via a
Google form (you'll make some submissions yourself and then
immediately analyze the results).

Learning objectives:
* combine data from a variety of sources to compute results in BigQuery
* query live results from a Google form/sheet
* perform spatial joins
* train and evaluate models using BigQuery

Before starting, please review the [general project directions](../projects.md).


## Clarifications/Correction

* none yet

## Setup

You'll create and submit a `p8.ipynb` notebook.  You'll answer 10
questions in the notebook.  If the output of a cell answers question
3, start the cell with a comment: `#q3`.  The autograder depends on
this to correlate parts of your output with specific questions.

Run JupyterLab directly on your VM (no Docker containers).  You'll need some packages:

```
pip3 install google-cloud-bigquery google-cloud-bigquery-storage pyarrow tqdm ipywidgets pandas matplotlib db-dtypes pandas-gbq
```

You'll also need to give your VM permission to access BigQuery and
Google Drive.  You can do so by pasting the following into the
terminal on your VM and following the directions. Please read the following cautions before running this command.

```
gcloud auth application-default login --scopes=openid,https://www.googleapis.com/auth/cloud-platform,https://www.googleapis.com/auth/drive.readonly
```

### :warning: Caution

1. While running the command, it will ask you to paste some link to your browser. If you have multiple Google accounts in your browser, and do not want this to select the default one, then do the following: 
    * paste the link in an incognito mode
    * login to the Google account of your choice
2. **Be careful**, because if a malicious party were to gain access to your
VM, they would have free access to all your Google Drive files (and
more).  For example, if your Jupyter is listening publicly (i.e.,
0.0.0.0 instead of localhost) and you have a weak password (or no
password), someone could gain access to your VM, and then these other
resources.

When you're not actively working, you may want to revoke (take away)
the permissions your VM has to minimize risk:

```bash
gcloud auth application-default revoke
```

If you're worried about exhausting the free tier and your educational
credits, you might also want to setup a quota here:

https://console.cloud.google.com/iam-admin/quotas

## Notebook

Clone your project-8 repo from Github to your VM. Inside your VM's repository folder, run the following command:

```bash
python3 -m jupyterlab --no-browser
```

Setup an SSH tunnel and connect (you'll need to copy/paste a token
from the terminal to the browser).  Then, create a notebook named
`p8.ipynb`. This is the only file we need from you.

You can create a BigQuery client like this in your `p8.ipynb`:

```python
from google.cloud import bigquery
bq = bigquery.Client()
```

You can do queries and get results in Pandas DataFrames like this (more on this later):

```python
q = bq.query(
"""
--- your query here ---
""")
q.to_dataframe()
```

The autograder will extract your output from these cells, so it won't
give points if not formatted correctly (extra spaces, split cells,
etc.).  For this project, answers are simple types (e.g., `int`s,
`float`s, `dict`s), so you'll need to do a little extra work to get
results out of the DataFrames you get from BigQuery.

## Part 1: County Data (Public Dataset)

For this part, you'll use the
`bigquery-public-data.geo_us_boundaries.counties` table.  This
contains names, IDs, boundaries, and more for every county in the
United States.

If we hadn't provided you with the name of the table, you could have found
it yourself as follows:

1. go to the GCP Marketplace by finding it in the menu or directly going to https://console.cloud.google.com/marketplace
2. using the Category, Type, and Price filters select "Maps", "Datasets", and "Free" respectively
3. click "Census Bureau US Boundaries"
4. click "VIEW DATASET"
5. from this view, you can expand the `geo_us_boundaries` dataset to see `counties` and other related tables; you can also browse the other datasets under the `bigquery-public-data` category

Note that there are also some corner cases in US geography, with
regions that are not part of a county.  For example, "St. Louis City"
is an independant city, meaning it's not part of St. Louis County.
The counties dataset contains some regions that are not
technically counties (though we will treat them as such for this
project).

#### Test your setup

Run the following in your notebook:

```python
q = bq.query(
"""
select count(*) as num_rows 
from bigquery-public-data.geo_us_boundaries.counties
""")
q.to_dataframe()
```

It should show something like this:

|     | num_rows |
| ---:| -------: |
| 0   |     3233 |

Now, let's answer some questions.

#### Q1: what is the `geo_id` for Dane county? (note that Madison is in Dane county). 

The output should be a string.

#### Q2: how many counties are there per state?

Answer for the five states with the most counties.  The dataset lacks
state names, so we'll use `state_fips_code` instead of names.

Your output should be a `dict` with 5 key/value pairs -- keys are the
FIPS codes and values are the counts.  Example:

```python
{'48': 254, '13': 159, '51': 133, '21': 120, '29': 115}
```

#### Q3: about how much should the queries for the last two questions cost?

Assumptions:
1. you don't have free credits
2. you've already exhausted BigQuery's 1 TB free tier
3. you're doing this computation in an Iowa data center

Hints:
1. look at the `total_bytes_billed` attribute of the query objects
2. when you re-run the queries, bytes billed is probably zero due to caching.  You can create a job config (to pass along with the query) to disable caching: `bigquery.QueryJobConfig(use_query_cache=False)`.  This will let you get realistic numbers for first-time runs.
3. look up the Iowa pricing per TB here: https://cloud.google.com/bigquery/pricing#on_demand_pricing

Answer with `dict` where keys identify which query, and values are the cost in dollars.  Example:

```python
{'q1': ????, 'q2': ????}
```

## Part 2: HDMA Data (Parquet in GCS)

<!-- Link Updated. Check! -->
Download
https://pages.cs.wisc.edu/~harter/cs544/data/hdma-wi-2021.parquet to
your VM.  This is a subset of the data from the CSV in
hdma-wi-2021.zip that we used in earlier projects.  We've done some
cleanup and conversion work for you to make the parquet file -- [see
here](cleanup.md) if you're interested about what exactly we've done.

<!-- TODO: should we specify a name? -->
Do the following two tasks outside your `p8.ipynb` notebook:
1. Create a private GCS bucket (named whatever you like, for example: `cs544_p8`). 
2. Upload the parquet file to your bucket.

Write code to create a dataset called `p8` in your GCP project.  Use
`exists_ok=True` so that you can re-run your code without errors.

Use a `load_table_from_uri` call to load the parquet data into a new
table called `hdma` inside your `p8` project.

#### Q4: what are the datasets in your GCP project?

Use this line of code to answer:

```python
[ds.dataset_id for ds in bq.list_datasets("????")] # PASTE your project name
```

You can see the GCP projects you own here: https://console.cloud.google.com/billing/projects

The output ought to contain the `p8` dataset.

#### Q5: how many loan applications are there in the HDMA data for each county?

Answer with a `dict` where the key is the county name and the value is
the count.  The `dict` should only contain the 10 counties with most
applications.  It should look like this:

```python
{'Milwaukee': 46570,
 'Dane': 38557,
 'Waukesha': 34159,
 'Brown': 15615,
 'Racine': 13007,
 'Outagamie': 11523,
 'Kenosha': 10744,
 'Washington': 10726,
 'Rock': 9834,
 'Winnebago': 9310}
```

You'll need to join your private table against the public counties
table to get the county name.

## Part 3: Application Data (Google Sheet Linked to Form)

Now let's pretend you have a very lucrative data science job and want to buy a vacation home in WI.  First, decide a few things:

1. what do you wish your income to be?  (be reasonable!)
2. how expensive of a house would you like to buy?
3. where in WI would you like the house to be?  (use a tool like Google maps to identify exact latitude/longitude coordinates)


Apply for your loan in the Google form here:
https://forms.gle/cf1R26MoGCmMriAN9. Feel free to apply multiple
times if a single vacation home is insufficient for your needs.


The form is linked to this spreadsheet (check that your loan applications show up): https://docs.google.com/spreadsheets/d/11UeIBqQylAyNUBsIO54p6WiYJWHayQMfHDbUWq1jGco/

Now, run some code to add the sheet as an external BigQuery table. The name of the table must be `applications`


```python
url = "https://docs.google.com/spreadsheets/d/11UeIBqQylAyNUBsIO54p6WiYJWHayQMfHDbUWq1jGco/"

external_config = bigquery.ExternalConfig("GOOGLE_SHEETS")
external_config.source_uris = [????]
external_config.options.skip_leading_rows = 1
external_config.autodetect = ????

table = bigquery.Table(????.table(????))
table.external_data_configuration = external_config

table = bq.create_table(table, exists_ok=True)
```

#### Q6: how many applications are there with your chosen income?

Your BigQuery results should give you at least 1, but it could be
more, and could change (depending on what income you chose, and how
many others with the same income have submitted applications).

#### Q7: how many applications are there in the Google sheet per WI county?

You'll need to do a spatial join using the `county_geom` column of the
`bigquery-public-data.geo_us_boundaries.counties` table to determine
the county name corresponding to each lat/lon point in the
spreadsheet.

Answer with a `dict` like this (key is county name and value is count):

```python
{'Dane': 1, ...}
```

Ignore any lat/lon points that get submitted to the form but fall
outside of WI.  The FIPS code (`state_fips_code`) for WI is `'55'` --
feel free to hardcode 55 in your query if it helps.

## Part 4: Machine Learning

Create a linear regression model (`model_type='LINEAR_REG'`) to
predict `loan_amount` based on `income`, and `loan_term` -- train it
on the HDMA data.

#### Q8: what is your model's `r2_score` on the HDMA dataset on which it was trained?

Note that you would normally split your data into train/test so that
overfitting doesn't give you an unrealistically good score -- to keep
the project simple; we aren't bothering with train/test splits this
time.

<!-- DONE: Added this to clarify -->
**Note:** If you encounter an error like `NotFound: 404 Not found: Model <project>:<dataset>.<model>`, make your notebook to wait for some time.
Sometimes it takes 1-2 minutes for BigQuery to notice that a model has been created.
To handle this case, you should write the following snippet before Q8 to make the notebook wait until BigQuery can see your model.

```python
import time
while True:
    if <your condition here>:  # Hint: use bq.list_models()
        break
    time.sleep(5)
```

#### Q9: what is the coefficient weight on the income column?

#### Q10: what ratio of the loan applications in the Google form are for amounts greater than the model would predict, given income?

For example, if 75% are greater, the answer would be 0.75.

Note that the model has two features: `income` and `loan_term`; the form
only collects income, so assume the loan term is 360 months (30 years)
for all the applications in the Google sheet.

## Testing

Download the latest [`tester.py`](../tester.py), [`nbutils.py`](../nbutils.py) and [`autograde.py`](./autograde.py). Run the following to check that your cell outputs are reasonable:

```bash
python3 autograde.py
```

It's OK if you hardcode some things in your notebook related to your
Google account (like your GCP project name).  We won't re-run the
notebooks this time.  We will just look at your code and check your
cell outputs.

## Submission

Check (and double-check :monocle_face:) that all the tests are passing
when you submit. Then, add p8.ipynb, commit, and push to GitHub.

Do not forget to revoke the permission to access your Google
drive. Run the following command in your VM.

```
gcloud auth application-default revoke
```
