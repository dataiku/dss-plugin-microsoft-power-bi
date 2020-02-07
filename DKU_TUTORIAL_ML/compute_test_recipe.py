# -*- coding: utf-8 -*-
import dataiku
import pandas as pd, numpy as np
from dataiku import pandasutils as pdu

# Read recipe inputs
customers_unlabeled_joined = dataiku.Dataset("customers_unlabeled_joined")
customers_unlabeled_joined_df = customers_unlabeled_joined.get_dataframe()

# Compute recipe outputs from inputs
# TODO: Replace this part by your actual code that computes the output, as a Pandas dataframe
# NB: DSS also supports other kinds of APIs for reading and writing data. Please see doc.

test_recipe_df = customers_unlabeled_joined_df # For this sample code, simply copy input to output

test_recipe_df["new_col"] = test_recipe_df["total_sum"] * 2

print("test")

# Write recipe outputs
test_recipe = dataiku.Dataset("test_recipe")
test_recipe.write_with_schema(test_recipe_df)
