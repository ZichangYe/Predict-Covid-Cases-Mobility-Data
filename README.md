# Predict-Covid-Cases-Mobility-Data

We are a group of Masters student in Data Science at New York University. In this work, we aim to predict the confirmed cases of Covid 19 in the state of New York as of May 15, 2020. We used the confirmed cases data from JHU, mobility data released by SafeGraph, and population data by county from the census.

Our approach includes feature engineering of county-wise import risk based on concepts of epidemiology, together with three modeling approaches combining machine learning approaches and epidemiology models adopted from related works. In particular, we used SIRD, RNN, and ERF functions to make predictions. We used logistic growth model as a baseline. Please refer to the [PDF](https://github.com/ZichangYe/Predict-Covid-Cases-Mobility-Data/blob/master/DSGA1003FinalProject.pdf) in this repo if interested in a detailed explanation of our methods.

Results:
* In most cases (21 out of 31 counties), mSIRD gives a better fit.

* The ERF models predict better in 14 counties out of the total of 62 counties in the test set, covering 16\% of total population and 46\% of total confirmed cases in New York. For counties where the ERF models are better, the MSE is improved by 45\%.

* In most counties, RNN beats our baseline model, Logistic Growth Model. 
