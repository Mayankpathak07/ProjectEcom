from mod.conn import get_conn, run_query
import streamlit as st
import plotly.express as px
import plotly.graph_objects as go
from dotenv import load_dotenv
load_dotenv(".env.local")

st.sidebar.title(":red[E - Commerce Analytics]")

option = st.sidebar.radio("Select Dashboard", ["Customer Insights", "Sales Analytics", "Seller Performance", "Product Analytics"])

if option == "Sales Analytics":
    st.title(":blue[Sales Performance Overview]")
    type = st.selectbox("Analysis Type ?", ("Please Select", "Data mart", "Aggregation tables", "KPI"))

    if type == "Data mart":
        query = '''
        SELECT DATE(order_purchase_timestamp) AS Order_Date, 
               SUM(total_price) AS Total_Revenue, 
               COUNT(order_id) AS Total_Orders
        FROM my_database.olist_orders_cleaned_dataset
        GROUP BY Order_Date
        ORDER BY Order_Date DESC
        LIMIT 30;
        '''
        sales_30_df = run_query(query)
        fig = px.line(sales_30_df, x="Order_Date", y="Total_Revenue", title="Revenue Trend (Last 30 Days)")
        fig.update_traces(line=dict(color="red"))
        st.plotly_chart(fig)

        query = '''
        SELECT MONTH(order_purchase_timestamp) AS Month, 
               ROUND(SUM(total_price)) AS Total_Revenue, 
               COUNT(DISTINCT(order_id)) AS Total_Orders
        FROM my_database.olist_orders_cleaned_dataset
        GROUP BY Month
        ORDER BY Month;
        '''
        monthly_revenue = run_query(query)
        monthly_revenue["Month"] = monthly_revenue["Month"].map({1: "Jan", 2: "Feb", 3: "Mar", 4: "Apr", 5: "May", 6: "Jun", 7: "Jul", 8: "Aug", 9: "Sep", 10: "Oct", 11: "Nov", 12: "Dec"})
        fig = px.bar(monthly_revenue, y="Total_Revenue", x="Month", title="Monthly Revenue")
        st.plotly_chart(fig)

        query = '''
        SELECT p.product_category AS Product_Category, 
               ROUND(SUM(f.product_price * f.quantity)) AS Total_Revenue 
        FROM my_database.olist_products_cleaned_dataset p 
        INNER JOIN my_database.fact_table f ON p.product_id = f.product_id 
        GROUP BY p.product_category 
        ORDER BY Total_Revenue DESC 
        LIMIT 5;
        '''
        best_sell = run_query(query)
        fig = px.pie(best_sell, names="Product_Category", values="Total_Revenue", title="Revenue of Top 5 Products")
        st.plotly_chart(fig)

        query = '''
        SELECT payment_type AS Payment_Type, 
               COUNT(*) AS count 
        FROM my_database.olist_orders_cleaned_dataset 
        GROUP BY payment_type;
        '''
        pay = run_query(query)
        fig = px.pie(pay, names="Payment_Type", values="count", title="Payments users prefer")
        st.plotly_chart(fig)

    elif type == "Aggregation tables":
        query = '''
        SELECT DATE(order_purchase_timestamp) AS Order_Date, 
               COUNT(DISTINCT(order_id)) AS Total_Orders, 
               ROUND(SUM(total_price), 2) AS Total_Revenue
        FROM my_database.olist_orders_cleaned_dataset
        GROUP BY Order_Date;
        '''
        sales_table = run_query(query)
        st.subheader("Daily Sales Summary Table")
        st.table(sales_table)

    elif type == "KPI":
        query = '''
        SELECT ROUND(SUM(total_price), 2) AS actual_price 
        FROM my_database.olist_orders_cleaned_dataset;
        '''
        price = run_query(query)
        price = price.iloc[0, 0]
        fig = go.Figure(go.Indicator(
            mode="number",
            value=price,
            title={"text": "Total Revenue (₹)", "font": {"size": 24}},
            number={"font": {"size": 36}},
            domain={'x': [0, 1], 'y': [0, 1]}
        ))
        st.plotly_chart(fig)

elif option == "Customer Insights":
    st.title(":green[Customer Insights Dashboard]")
    type = st.selectbox("Analysis Type ?", ("Please Select", "Data mart", "Aggregation tables", "KPI"))

    if type == "Data mart":
        query = '''
        SELECT customer_id, 
               COUNT(order_id) AS total_orders, 
               ROUND(SUM(product_price * quantity)) AS total_revenue,
               (CASE 
                   WHEN COUNT(order_id) = 1 THEN 'New Customer'
                   WHEN COUNT(order_id) BETWEEN 2 AND 5 THEN 'Returning Customer'
                   ELSE 'Loyal Customer' 
                END) AS customer_segment 
        FROM my_database.fact_table
        GROUP BY customer_id 
        ORDER BY total_revenue DESC;
        '''
        customer_segments = run_query(query)
        fig = px.pie(data_frame=customer_segments, names="customer_segment", values="total_revenue", title="Revenue generated")
        st.plotly_chart(fig)

        query = '''
        SELECT c.customer_state AS Customer_State, 
               COUNT(o.order_id) AS Total_Orders 
        FROM my_database.olist_customers_cleaned_dataset c 
        INNER JOIN my_database.olist_orders_cleaned_dataset o ON c.customer_id = o.customer_id
        GROUP BY c.customer_state 
        ORDER BY Total_Orders DESC;
        '''
        cities = run_query(query)
        fig = px.bar(cities, x="Total_Orders", y="Customer_State", title="Number of Orders in each State")
        st.plotly_chart(fig)

    elif type == "Aggregation tables":
        query = '''
        SELECT c.customer_id AS Customer_ID, 
               COUNT(DISTINCT(o.order_id)) AS Total_Orders, 
               SUM(o.total_price) AS Total_Spent, 
               AVG(o.total_price) AS Average_order_value,
               MAX(o.order_purchase_timestamp) AS Last_Purchase_Date 
        FROM my_database.olist_customers_cleaned_dataset c 
        INNER JOIN my_database.olist_orders_cleaned_dataset o ON c.customer_id = o.customer_id
        GROUP BY c.customer_id;
        '''
        customer_behaviour = run_query(query)
        st.subheader("Customer Purchase Behaviour Table")
        st.table(customer_behaviour)

    elif type == "KPI":
        query = '''
        SELECT ROUND(SUM(total_price) / COUNT(DISTINCT(order_id)), 2) AS avg_order_value 
        FROM my_database.olist_orders_cleaned_dataset;
        '''
        avg = run_query(query)
        avg = avg.iloc[0, 0]
        fig = go.Figure(go.Indicator(
            mode="number",
            value=avg,
            title={"text": "Average Order Value (₹)", "font": {"size": 24}},
            number={"font": {"size": 36}},
            domain={'x': [0, 1], 'y': [0, 1]}
        ))
        st.plotly_chart(fig)

elif option == "Seller Performance":
    st.title(":orange[Seller Performance Overview]")
    type = st.selectbox("Analysis Type ?", ("Please Select", "Data mart", "Aggregation tables", "KPI"))

    if type == "Data mart":
        query = '''
        SELECT s.seller_state AS Seller_State, 
               COUNT(*) AS Count 
        FROM my_database.olist_sellers_cleaned_dataset s 
        GROUP BY s.seller_state;
        '''
        no_of_sellers = run_query(query)
        fig = px.bar(no_of_sellers, x="Count", y="Seller_State", title="Number of Sellers in each State")
        st.plotly_chart(fig)

        query = '''
        SELECT s.seller_state AS Seller_State, 
               ROUND(AVG(DATEDIFF(o.order_estimated_delivery_date, o.order_purchase_timestamp))) AS Average_Delivery_Time 
        FROM my_database.olist_sellers_cleaned_dataset s 
        INNER JOIN my_database.fact_table f ON s.seller_id = f.seller_id inner join
        olist_orders_cleaned_dataset o on f.order_id=o.order_id group by s.seller_state;
        '''
        avg_del = run_query(query)
        fig = px.bar(avg_del, x="Seller_State", y="Average_Delivery_Time", title="Average Delivery Time(in Days) taken by Sellers in each State")
        st.plotly_chart(fig)

    elif type == "Aggregation tables":
        query = '''
        SELECT s.seller_id AS Seller_ID, 
               COUNT(DISTINCT(o.order_id)) AS Total_Orders, 
               SUM(o.total_price) AS Total_Revenue, 
               ROUND(AVG(DATEDIFF(o.order_estimated_delivery_date, o.order_approved_at))) AS Average_Delivery_Time 
        FROM my_database.olist_sellers_cleaned_dataset s 
        INNER JOIN my_database.fact_table f ON s.seller_id = f.seller_id 
        inner join olist_orders_cleaned_dataset o on f.order_id=o.order_id
        WHERE o.order_estimated_delivery_date IS NOT NULL  
        GROUP BY s.seller_id;
        '''
        seller_perf = run_query(query)
        st.subheader("Seller Performance Table")
        st.table(seller_perf)

    elif type == "KPI":
        query = '''
        SELECT s.seller_id AS Seller_ID, 
               ROUND(AVG(DATEDIFF(o.order_estimated_delivery_date, o.order_approved_at))) AS Average_Delivery_Time 
        FROM my_database.olist_sellers_cleaned_dataset s 
        INNER JOIN my_database.fact_table f ON s.seller_id = f.seller_id inner join my_database.olist_orders_cleaned_dataset o
        on f.order_id = o.order_id
        WHERE o.order_estimated_delivery_date IS NOT NULL 
        GROUP BY s.seller_id 
        ORDER BY Average_Delivery_Time;
        '''
        Delivery_eff = run_query(query)
        st.subheader("Delivery Efficiency of Sellers")
        st.table(Delivery_eff)

elif option == "Product Analytics":
    st.title(":violet[Product Performance Dashboard]")
    type = st.selectbox("Analysis Type ?", ("Please Select", "Data mart", "Aggregation tables", "KPI"))

    if type == "Data mart":
        query = '''
        SELECT p.product_id AS Product_ID, 
               p.product_category AS Product_Category, 
               ROUND(SUM(f.product_price * f.quantity)) AS Total_Revenue, 
               COUNT(f.order_id) AS Total_Orders 
        FROM my_database.olist_products_cleaned_dataset p 
        INNER JOIN my_database.fact_table f ON p.product_id = f.product_id 
        GROUP BY p.product_id, p.product_category 
        ORDER BY Total_Revenue DESC 
        LIMIT 10;
        '''
        best_products = run_query(query)
        fig = px.pie(best_products, names="Product_Category", values="Total_Revenue", title="Top 10 Best Selling Products by Revenue")
        st.plotly_chart(fig)

        query = '''
        SELECT p.product_category AS Product_Category, 
               COUNT(f.order_id) AS Total_Orders, 
               SUM(f.quantity) AS Units_Sold, 
               ROUND(SUM(f.product_price * f.quantity)) AS Total_Revenue 
        FROM my_database.olist_products_cleaned_dataset p 
        INNER JOIN my_database.fact_table f ON p.product_id = f.product_id 
        GROUP BY p.product_category 
        ORDER BY Total_Revenue DESC 
        LIMIT 10;
        '''
        units_sold = run_query(query)
        fig = px.bar(units_sold, x="Product_Category", y="Units_Sold", title="Total Products Sold in each Category")
        st.plotly_chart(fig)

    elif type == "Aggregation tables":
        query = '''
        SELECT p.product_id AS Product_ID, 
               p.product_category AS Product_Category, 
               SUM(o.product_price) AS Total_Revenue, 
               COUNT(DISTINCT(o.order_id)) AS Total_Orders, 
               SUM(o.quantity) AS Total_Quantity 
        FROM my_database.olist_products_cleaned_dataset p 
        INNER JOIN my_database.fact_table o ON p.product_id = o.product_id 
        GROUP BY p.product_id, p.product_category;
        '''
        top_prod = run_query(query)
        st.subheader("Top Selling Products Aggregation Table")
        st.table(top_prod)

    elif type == "KPI":
        query = '''
        SELECT p.product_id AS Product_ID, 
               p.product_category AS Product_Category, 
               ROUND(SUM(o.total_price), 2) AS Total_Revenue 
        FROM my_database.olist_products_cleaned_dataset p 
        INNER JOIN my_database.fact_table f ON p.product_id = f.product_id 
        inner join olist_orders_cleaned_dataset o on f.order_id = o.order_id
        GROUP BY p.product_id, p.product_category 
        LIMIT 5;
        '''
        top_5 = run_query(query)
        st.subheader("Best Categories")
        st.table(top_5)