from scipy.stats import shapiro
import pandas as pd


def is_normal(column_data):
    # Shapiro-Wilk test
    _, p_value = shapiro(column_data)
    
    # Set a significance level (0.05 is commonly used)
    alpha = 0.05
    
    # Output True if p-value is higher than alpha (not rejecting null hypothesis of normality)
    return p_value > alpha



def handle_outliers_iqr(column_data):
    Q1 = column_data.quantile(0.25)
    Q3 = column_data.quantile(0.75)
    IQR = Q3 - Q1

    lower_bound = Q1 - 1.5 * IQR
    upper_bound = Q3 + 1.5 * IQR

    # Clipping outliers
    column_data = column_data.clip(lower=lower_bound, upper=upper_bound)
    return column_data



