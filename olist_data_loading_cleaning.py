import pandas as pd
import numpy as np
import matplotlib.pyplot as plt





import os

List_of_files = os.listdir(r"C:\Users\Mayank Pathak\.cache\kagglehub\datasets\olistbr\brazilian-ecommerce\versions\2")


csv_files = []

for i in List_of_files:
    csv_files.append(pd.read_csv(r"C:\Users\Mayank Pathak\.cache\kagglehub\datasets\olistbr\brazilian-ecommerce\versions\2\{}".format(i)))


List_of_files


olist_customers_dataset = csv_files[0]

olist_geolocation_dataset = csv_files[1]

olist_orders_dataset = csv_files[2]

olist_order_items_dataset = csv_files[3]

olist_order_payments_dataset = csv_files[4]

olist_order_reviews_dataset = csv_files[5]

olist_products_dataset = csv_files[6]

olist_sellers_dataset = csv_files[7]

product_category_name_translation = csv_files[8]


# Cleaning the olist dataset


olist_customers_dataset.isna().sum()


olist_geolocation_dataset.isna().sum()


olist_orders_dataset.isna().sum()


olist_orders_dataset.drop(["order_delivered_carrier_date", "order_delivered_customer_date"], axis = 1, inplace = True)
# Deleting carrier date and customer date columns as it is not useful for analysis because of containing estimated delivery date column

# Not handling null values in order_approved_at column as the order was cancelled before approving.


olist_orders_dataset.isna().sum()


olist_order_items_dataset.isna().sum()


olist_order_payments_dataset.isna().sum()


olist_order_reviews_dataset.isna().sum()


olist_order_reviews_dataset


# Dropping review_comment_title column because of too high null count and review_comment_message as the data is inconsistent.

olist_order_reviews_dataset.drop(["review_comment_title", "review_comment_message"], axis = 1, inplace = True)


olist_order_reviews_dataset.isna().sum()


olist_products_dataset.isna().sum()


olist_products_dataset


olist_products_dataset[olist_products_dataset["product_category_name"].isna()]


olist_products_dataset.drop(list(olist_products_dataset[(olist_products_dataset["product_name_lenght"].isna())].index),
                            axis = 0, 
                            inplace = True)


olist_products_dataset[olist_products_dataset["product_weight_g"].isna()]


olist_products_dataset.drop(olist_products_dataset[olist_products_dataset["product_weight_g"].isna()].index.tolist(),
                            axis = 0,
                            inplace = True)


olist_products_dataset.drop(olist_products_dataset[olist_products_dataset["product_category_name"].isin(["pc_gamer", "portateis_cozinha_e_preparadores_de_alimentos"])].index.tolist(),
                            axis = 0,
                            inplace = True)


names_trans = {}

for i,j in zip(product_category_name_translation["product_category_name"],product_category_name_translation["product_category_name_english"]):
    names_trans[i] = j


names = []

for i in olist_products_dataset["product_category_name"]:
    names.append(names_trans.get(i))
    
olist_products_dataset.insert(2, "product_category", names)


olist_products_dataset.drop("product_category_name", axis = 1, inplace = True)


olist_products_dataset.isna().sum()


# Renaming the columns
olist_products_dataset = olist_products_dataset.rename(columns = {"product_name_lenght":"product_name_length", 
                               "product_description_lenght":"product_description_length",
                              "product_photos_qty":"product_photos_quantity",
                              "product_weight_g":"product_weight_gm"})


olist_products_dataset.isna().sum()


olist_sellers_dataset.isna().sum()


product_category_name_translation.isna().sum()


olist_customers_dataset.duplicated().sum()


olist_geolocation_dataset.duplicated().sum()


olist_geolocation_dataset.drop_duplicates(inplace = True)


olist_geolocation_dataset.duplicated().sum()


olist_order_items_dataset


olist_order_items_agg = olist_order_items_dataset.groupby("order_id").agg(total_items=("order_item_id", "count"),total_price=("price", "sum"),
total_freight_value = ("freight_value", "sum")                                                                          
).reset_index()

# Pushing useful information to olist_orders data

# Freight value is the shipping charges


olist_order_payments_agg = olist_order_payments_dataset.groupby("order_id").agg(total_payment_value=("payment_value", "sum"),payment_types_used=("payment_type", lambda x: x.mode().iloc[0]),
max_installments=("payment_installments", "max")
).reset_index()


olist_order_reviews_agg = olist_order_reviews_dataset.groupby("order_id").agg(avg_review_score=("review_score", "mean"),
).reset_index()


'''Pushing this useful aggregrated data to olist_orders dataset because of not having unique column in order_items, order_payments,
order_reviews so extracted useful data from them and pushing it to olist_orders dataset'''

olist_orders_dataset = pd.merge(olist_orders_dataset, olist_order_items_agg, on="order_id", how="left")

olist_orders_dataset = pd.merge(olist_orders_dataset, olist_order_payments_agg, on = "order_id", how = "left")

olist_orders_dataset = pd.merge(olist_orders_dataset, olist_order_reviews_agg, on = "order_id", how = "left")


olist_orders_dataset.isna().sum()


olist_orders_dataset[olist_orders_dataset["total_items"].isna()]


olist_orders_dataset.drop(olist_orders_dataset[olist_orders_dataset["total_items"].isna()].index.tolist(), 
                          axis = 0, inplace = True)


olist_orders_dataset.isna().sum()


olist_orders_dataset[olist_orders_dataset["total_payment_value"].isna()]


olist_orders_dataset.drop(30710, axis = 0 , inplace = True)


olist_orders_dataset[olist_orders_dataset["avg_review_score"].isna()]


Q1 = olist_orders_dataset["avg_review_score"].quantile(0.25)

Q3 = olist_orders_dataset["avg_review_score"].quantile(0.75)

IQR = Q3 - Q1

LL = Q1 - (1.5*IQR)

UL = Q3 + (1.5*IQR)

olist_orders_dataset[(olist_orders_dataset["avg_review_score"] < LL) | (olist_orders_dataset["avg_review_score"] > UL)]


olist_orders_dataset["avg_review_score"].fillna(olist_orders_dataset["avg_review_score"].median(), inplace = True)


olist_orders_dataset.isna().sum()


olist_orders_dataset = olist_orders_dataset.rename(columns = {"payment_types_used":"payment_type"})


olist_orders_dataset.duplicated().sum()


olist_order_reviews_dataset.duplicated().sum()


olist_orders_dataset.duplicated().sum()


olist_products_dataset.duplicated().sum()


olist_sellers_dataset.duplicated().sum()


olist_sellers_dataset.duplicated().sum()


product_category_name_translation.duplicated().sum()


olist_customers_dataset.dtypes


olist_order_items_dataset["shipping_limit_date"] = pd.to_datetime(olist_order_items_dataset["shipping_limit_date"])


olist_order_payments_dataset.dtypes


olist_order_reviews_dataset.dtypes


olist_order_reviews_dataset["review_creation_date"] = pd.to_datetime(olist_order_reviews_dataset["review_creation_date"])

olist_order_reviews_dataset["review_answer_timestamp"] = pd.to_datetime(olist_order_reviews_dataset["review_answer_timestamp"])


olist_orders_dataset.dtypes


olist_orders_dataset.head()


olist_orders_dataset["order_purchase_timestamp"] = pd.to_datetime(olist_orders_dataset["order_purchase_timestamp"])

olist_orders_dataset["order_approved_at"] = pd.to_datetime(olist_orders_dataset["order_approved_at"])

olist_orders_dataset["order_estimated_delivery_date"] = pd.to_datetime(olist_orders_dataset["order_estimated_delivery_date"])

olist_orders_dataset["total_items"] = olist_orders_dataset["total_items"].astype("int8")

olist_orders_dataset["max_installments"] = olist_orders_dataset["max_installments"].astype("int8")

olist_orders_dataset["avg_review_score"] = olist_orders_dataset["avg_review_score"].astype("int8")


olist_products_dataset.dtypes


olist_products_dataset["product_name_length"] = olist_products_dataset["product_name_length"].astype("int16")

olist_products_dataset["product_description_length"] = olist_products_dataset["product_description_length"].astype("int16")

olist_products_dataset["product_photos_quantity"] = olist_products_dataset["product_photos_quantity"].astype("int16")

olist_products_dataset["product_weight_gm"] = olist_products_dataset["product_weight_gm"].astype("int16")

olist_products_dataset["product_length_cm"] = olist_products_dataset["product_length_cm"].astype("int16")

olist_products_dataset["product_height_cm"] = olist_products_dataset["product_height_cm"].astype("int16")

olist_products_dataset["product_width_cm"] = olist_products_dataset["product_width_cm"].astype("int16")


olist_sellers_dataset.dtypes


product_category_name_translation.dtypes