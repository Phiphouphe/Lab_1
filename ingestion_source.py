import json

with open('source/pos_sales_data_30k.json') as f:
    data = json.load(f)
    sales_dict = data[0]

    # print(sales_dict)
    print(sales_dict["products"][1])