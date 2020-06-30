# Germany effective reproduction number (R_t) calculation for covid-19

This project is based on the [Finnish effective reproduction numner calculation for covid-19](https://github.com/futurice/covid-19-R_t-estimating).

This projects contains a jupyter notebook to calculate and visualise Germany R_t based on current RKI data in `germany.ipynb`.

The other part of this project (prepare_cases_function, calculate_r_function, plot_r_function) contains the three `AWS Lambda Functions` that run every 12 h and write the results and graphs to S3 storage. To be able to run these functions, you need to install locally the required libraries (`pandas, pytz, matplotlib`) as described in this [tutorial](https://medium.com/@korniichuk/lambda-with-pandas-fd81aa2ff25e) and use the `AWSLambda-Python3x-SciPy1x` provided layer. The workflow of running the 3 functions sequentially is built using `AWS Step Functions`. 

The original notebook and explanation of the calculation is in the notebook Realtime R0.ipynb is by Kevin Systrom and is further explained in the following blog post

http://systrom.com/blog/the-metric-we-need-to-manage-covid-19/