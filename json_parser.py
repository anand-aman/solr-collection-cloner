import json
import requests


def make_payload(data):
    title = data['title']
    category_bread_cum = data['productcategory']
    if len(category_bread_cum) < 2:
        category_list = []
        categoryss = "NaN"
        productcategoryss = "NaN"
    else:
        category_list = category_bread_cum.split('>')
        categoryss = [category_list[len(category_list) - 1]]
        productcategoryss = [category_list[len(category_list) - 2]]

    payload = {
        "name_s": title,
        "cannoical_url_s": data['link'],
        "sku_s": data['id'],
        "product_cannoical_url_s": data['link'],
        "smallImage_s": data['image_link'],
        "page_reachable_b": True,
        "org_s": "boxxer",
        "status_b": False,
        "condition_s": data['condition'],
        "namel_t": title.lower(),
        "description_t": data['description'],
        "price_d": float(data['price']),
        "namel_s": title.lower(),
        "client_os_type_s": "magento",
        "image_base_url_s": data['parent_image_link'],
        "event_type_s": "productinfo",
        "orgID_s": "38af2fd0-bb5b-44bb-9750-4456849dc4d7",
        "visibility_b": data['can_order'],
        "product_id_i": data['id'],
        "thumbImage_s": data['image_link'],
        "typeid_s": "simple",
        "image_s": data['image_link'],
        "name_t": title,
        "brand_s": data['brand'],
        "sub_brand_s": "NaN",
        "stock_d": float(data['stock']),
        "weight_i": data['shipping_weight'],
        "discounted_price_d": float(data['price_excl']),
        "release_date_s": "NaN",
        "pupdated_at_dt": data['parent_updatedat'],
        "category_bread_crumb_t": category_bread_cum,
        "categories_ss": categoryss,
        "parent_catergoy_ss": productcategoryss,
        "catname_ss": categoryss,
        "complete_cat_ss": category_list,
        "ean_s": data['ean'],
        "gtin_s": data['gtin']
    }
    return payload


def parse_file():
    file = open("D:\Code\GitHub\solr-collection-cloner\\file.json", "r")
    file_data = file.read()
    print(type(file_data))

    # Parsing
    data = []
    stack = []
    doc = ""
    flag = False
    first = True
    for ch in file_data:
        if first:
            first = False
            continue
        if flag and ch == ',':
            flag = False
            continue

        if ch == '{':
            stack.append(ch)
        elif ch == '}':
            stack.pop()

        doc = doc + ch

        if len(stack) == 0:
            data.append(doc)
            doc = ""
            flag = True
    del data[0]
    data.pop()
    data.pop()
    length = len(data)
    print(length)
    file.close()
    return data


def post_data():
    document_list = parse_file()
    print("Post Data Started")
    solr_host = "http://188.166.3.10:8981/solr/product_profile1/update/json?commit=true"

    # Pushing 500 docs at a time into Solr
    count = 0
    docs = []
    for line in document_list:
        count += 1
        try:
            json_object = eval(line)  # To convert str to dict
            json_object = make_payload(json_object)

            # response = requests.post(url=solr_host, json=payload)
            # print(json_object)
            docs.append(json_object)
            if len(docs) == 500:
                payload = json.dumps(docs)
                response = requests.post(url=solr_host, data=payload)
                print(count)
                print(response)
                docs.clear()
        except Exception as e:
            print(e.__str__())
            continue
    print(count)
    payload = json.dumps(docs)
    response = requests.post(url=solr_host, data=payload)
    return response


