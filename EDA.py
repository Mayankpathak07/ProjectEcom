import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns

data = pd.read_csv("Event_Data.csv")

 
discrete_cols = ["payment_type", "quantity"]

for i in discrete_cols:
    print("*"*50 + " "+ f"{i}" +" "+"*"*50)
    plt.figure(figsize = (6,6))
    data[i].value_counts().plot.barh()
    plt.xlabel("Count")
    plt.ylabel(i)
    plt.show()
    print()

 
for i in discrete_cols:
    print("*"*50 + " "+ f"{i}" +" "+"*"*50)
    plt.figure(figsize = (6,6))
    data[i].value_counts().plot.pie(autopct = "%.2f")
    plt.ylabel("")
    plt.show()
    print()


# ### Univariate Analysis on Continuous data

 
data["product_price"].plot.kde(color = "red")
plt.xlabel("Product Price")
plt.title("Checking the distribution of Product Price Column")
plt.show()

 
sns.boxplot(data = data, x = "product_price")
plt.title("Checking the Outliers in Product Price Column")
plt.show()


# ### Bi Variate Analysis


# #### For Continuous vs Discrete data

 
c = 1
for i in discrete_cols:
    plt.figure(figsize = (6,10))
    plt.subplot(4,1,c)
    sns.boxplot(data = data, x = i, y = "product_price")
    plt.xlabel(i)
    plt.ylabel("Product Price")
    plt.xticks(fontsize = 8)
    plt.title(f"Relation between Product Price column and {i} column")
    plt.show()
    print("*"*100)


# #### For Discrete vs Discrete columns

 
sns.countplot(data = data, x = "payment_type", hue = "quantity")
plt.legend(loc='center left', bbox_to_anchor=(1, 0.5))
plt.show()

   
# ## Exploratory Data Analysis on Olist Data

   
# ### Univariate Analysis on Discrete Data in olist_customers_dataset

 
olist_customers_dataset = pd.read_csv("olist_customers_cleaned_dataset.csv")

 
discrete_cols = ["customer_state"]

for i in discrete_cols:
    print("*"*50 + " "+ f"{i}" +" "+"*"*50)
    plt.figure(figsize = (6,6))
    olist_customers_dataset[i].value_counts().plot.barh()
    plt.xlabel("Count")
    plt.ylabel(i)
    plt.show()
    print()

 
   
# ### Discrete Univariate Analysis on olist_orders_dataset

 
olist_orders_dataset = pd.read_csv("olist_orders_cleaned_dataset.csv")


 
discrete_cols = ["order_status", "total_items", "payment_type", "max_installments", "avg_review_score"]

for i in discrete_cols:
    print("*"*50 + " "+ f"{i}" +" "+"*"*50)
    plt.figure(figsize = (6,6))
    olist_orders_dataset[i].value_counts().plot.barh()
    plt.xlabel("Count")
    plt.ylabel(i)
    plt.show()
    print()

   
# ### Continuous Univariate Analysis on olist_orders_dataset

 
olist_orders_dataset

 
continuous_cols = ["total_price", "total_freight_value", "total_payment_value"]

for i in continuous_cols:
    print("*"*50 + " "+ f"{i}" +" "+"*"*50)
    plt.figure(figsize = (6,6))
    olist_orders_dataset[i].plot.kde()
    plt.xlabel(i)
    plt.title(f"Checking the distribution of {i} Column")
    plt.show()
    print()


 
for i in continuous_cols:
    print("*"*50 + " "+ f"{i}" +" "+"*"*50)
    plt.figure(figsize = (6,6))
    olist_orders_dataset[i].plot.box()
    plt.show()
    print()

   
# ### Continuous Univariate Analysis on olist_products_dataset

 
olist_products_dataset = pd.read_csv("olist_products_cleaned_dataset.csv")


 
continuous_cols = ["product_name_length", "product_description_length", "product_weight_gm", "product_length_cm","product_height_cm","product_width_cm"]

for i in continuous_cols:
    print("*"*50 + " "+ f"{i}" +" "+"*"*50)
    plt.figure(figsize = (6,6))
    olist_products_dataset[i].plot.kde()
    plt.xlabel(i)
    plt.title(f"Checking the distribution of {i} Column")
    plt.show()
    print()

 
continuous_cols = ["product_name_length", "product_description_length", "product_weight_gm", "product_length_cm","product_height_cm","product_width_cm"]

for i in continuous_cols:
    print("*"*50 + " "+ f"{i}" +" "+"*"*50)
    plt.figure(figsize = (6,6))
    olist_products_dataset[i].plot.box()
    plt.show()
    print()

   
# ### Discrete Univariate Analysis on olist_products_dataset

 
discrete_cols = ["product_photos_quantity"]

for i in discrete_cols:
    print("*"*50 + " "+ f"{i}" +" "+"*"*50)
    plt.figure(figsize = (6,6))
    olist_products_dataset[i].value_counts().plot.barh()
    plt.xlabel("Count")
    plt.ylabel(i)
    plt.show()
    print()

   
# ### Discrete Univariate Analysis on olist_sellers_dataset
olist_sellers_dataset = pd.read_csv("olist_sellers_cleaned_dataset.csv")
 
plt.figure(figsize = (6,6))
olist_sellers_dataset["seller_state"].value_counts().plot.barh()
plt.show()