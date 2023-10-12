# P2 (6% of grade): Predicting COVID Deaths with PyTorch

## Overview

In this project, we'll use PyTorch to create a regression model that
can predict how many deaths there will be for a WI census tract, given
the number of people who have tested positive, broken down by age.
The train.csv and test.csv files we provide are based on data from
https://dhsgis.wi.gov/, downloaded in Spring of 2023 (it appears the
dataset is no longer available).

Learning objectives:
* multiply tensors
* check whether GPUs are available
* optimize inputs to minimize outputs
* use optimization to optimize regression coefficients

Note that if you normally use VS Code with Jupyter, your setup for
this project will be tricky because you can't SSH into a container.
There are some notes here
(https://github.com/cs544-wisc/f23/tree/main/docs/vs-code-jupyter)
about how to connect -- ignore if you're using Jupyter through your
browser.

Before starting, please review the [general project directions](../projects.md).

## Corrections/Clarifications

* Sep 21: updated note about data source
* Sep 21: add note about VS code
* Sep 25: note that for Jupyter cell answers, you need to put the Python expression computing the answer on the list line of the cell (prints are for your own debugging info and do not count as cell output)

## Part 1: Setup

Build the Dockerfile we give you (feel to make edits if you like) to
create your environment.  Run the container, setup an SSH tunnel, and
open JupyterLab in your browser.  Create a notebook called `p2.ipynb`
in the `nb` directory.

Use train.csv and test.csv to construct four PyTorch float64 tensors:
`trainX`, `trainY`, `testX`, and `testY`.  Hints:

* you can use `pd.read_csv` to load CSVs to DataFrames
* you can use use `df.values` to get a numpy array from a DataFrame
* you can convert from numpy to PyTorch (https://pytorch.org/tutorials/beginner/basics/tensorqs_tutorial.html)
* you'll need to do some 2D slicing: the last column contains your y values and the others contain your X values

`trainX` (number of positive COVID tests per tract, by age group) should look like this:

```python
tensor([[ 24.,  51.,  44.,  ...,  61.,  27.,   0.],
        [ 22.,  31., 214.,  ...,   9.,   0.,   0.],
        [ 84., 126., 239.,  ...,  74.,  24.,   8.],
        ...,
        [268., 358., 277.,  ..., 107.,  47.,   7.],
        [ 81., 116.,  90.,  ...,  36.,   9.,   0.],
        [118., 156., 197.,  ...,  19.,   0.,   0.]], dtype=torch.float64)
```

`trainY` (number of COVID deaths per tract) should look like this (make sure it is vertical, not 1 dimensional!):

```python
tensor([[3.],
        [2.],
        [9.],
        ...,
        [5.],
        [2.],
        [5.]], dtype=torch.float64)
```

#### Q1: about how many bytes does trainX consume?

Don't count any overhead for the Python object -- just multiply the
element size by the number of elements.

#### Q2: what is the biggest difference we would have any one cell if we used float16 instead of float64?

Convert trainX to float16, then back again to float64.  Subtract the
resulting matrix from the original.  Find the biggest absolute
difference, and display as a Python float.

#### Q3: is a CUDA GPU available on your VM?

Write a code snippet to produce a True/False answer.

## Part 2: Prediction with Hardcoded Model

Let's predict the number of COVID deaths in the test dataset under the
assumption that the deathrate is 0.004 for those <60 and 0.03 for those >=60.
Encode these assumptions as coefficients in a tensor by pasting
the following:

```python
coef = torch.tensor([
        [0.0040],
        [0.0040],
        [0.0040],
        [0.0040],
        [0.0040],
        [0.0040], # POS_50_59_CP
        [0.0300], # POS_60_69_CP
        [0.0300],
        [0.0300],
        [0.0300]
], dtype=trainX.dtype)
coef
```

#### Q4: what is the predicted number of deaths for the first census tract?

Multiply the first row `testX` by the `coef` vector and use `.item()`
to print the predicted number of deaths in this tract.

#### Q5: what is the average number of predicted deaths, over the whole testX dataset?

## Part 3: Optimization

Let's say `y = x^2 - 8x + 19`.  We want to find the x value that minimizes y.

#### Q6: first, what is y when x is a tensor containing 0.0?

```python
x = torch.tensor(0.0)
y = ????
float(y)
```

#### Q7: what x value minimizes y?

Write an optimization loop that uses `torch.optim.SGD`.  You can
experiment with the training rate and number of iterations, as long as
you find a setup that gets approximately the right answer.

## Part 4: Linear Regression

Use the `torch.zeros` function to initialize a 2-dimensional `coef`
matrix of size and type that allows us to compute `trainX @ coef` (we
won't bother with a bias factor in this exercise).

#### Q8: what is the MSE (mean-square error) when we make predictions using this vector of zero coefficients?

You'll be comparing `trainX @ coef` to `trainY`


#### Optimization

In part 1, you used a hardcoded `coef` vector to predict COVID deaths.
Now, you will start with zero coefficients and optimize them.

Seed torch random number generation with **544**.

Setup a training dataset and data loader like this:

```python
ds = torch.utils.data.TensorDataset(trainX, trainY)
dl = torch.utils.data.DataLoader(ds, batch_size=50, shuffle=True)
```

Write a training loop to improve the coefficients.  Requirements:
* use the `torch.optim.SGD` optimizer
* use 0.000002 learning rate
* run for 500 epochs
* use `torch.nn.MSELoss` for your loss function

#### Q9: what is the MSE over the training data, using the coefficients resulting from the above training?

#### Q10: what is the MSE over the test data?

## Submission

You should commit your work in a notebook named `p2.ipynb`.

## Tester

After copying `../tester.py`, `../nbutils.py`, and `autograde.py` to
your repository (where your `nb/` directory is located), you can check
your notebook answers with this command:

```sh
python3 autograde.py
```
For the autograder to work, for each question, please include a line of comment at the beginning of code cell that outputs the answer. For example, the code cell for question 7 should look like
```python
#q7
...
```

Of course, the checker only looks at the answers, not how you got
them, so there may be further deductions (especially in the case of
hardcoding answers).
