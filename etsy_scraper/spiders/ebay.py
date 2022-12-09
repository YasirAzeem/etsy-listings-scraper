from cmath import e
from urllib import request
import scrapy
from bs4 import BeautifulSoup
import re, json
import sys, random
import mysql.connector
import json
import datetime
import urllib.parse


class EbaySpider(scrapy.Spider):
    name = 'ebay'
    allowed_domains = ['ebay.com']
    start_urls = ['http://ebay.com/']
    count = 0
    conn = mysql.connector.connect(host='5.161.102.106',
                                   database='usedpickxyz_db1',
                                   user='usedpickxyz_db1',
                                   password='8iGF*#Yg@aLN')
    cursor = conn.cursor()
    proxy = 'http://user-amzbot-country-us:amzBotGzer0!!@all.dc.smartproxy.com:10000'
    def start_requests(self):
        kws = [x.replace(',','') for x in open('kws.txt','r').read().split('\n') if x]
        for kw in kws:
            url =  f"https://www.ebay.com/sch/i.html?_nkw={urllib.parse.quote_plus(kw)}&_sacat=0&_ipg=240&rt=nc&LH_PrefLoc=1"    
            yield scrapy.Request(url=url,callback=self.parse,meta={'kw':kw,'count':1})

    
    def upload_entry(self, item, retries=3):
    
        while retries:
            try:
                new_item = {}
                for i in list(item.keys()):
                    if not item.get(i):
                        item[i]=None
                name =  "ebay"
                conn = mysql.connector.connect(host='5.161.102.106',
                                    database='usedpickxyz_db1',
                                    user='usedpickxyz_db1',
                                    password='8iGF*#Yg@aLN')
                conn.autocommit = False
                cursor = conn.cursor(buffered=True)
                sql_update_query = f'''SELECT id from usedpickxyz_db1.brand_list where name = "{item['Brand']}"'''
                cursor.execute(sql_update_query)
                x = cursor.fetchone()
                if x:
                    brand_id, = x
                    if type(brand_id)==tuple:
                        brand_id, = brand_id
                else:
                    sql_update_query = f'''INSERT INTO usedpickxyz_db1.brand_list (name, created_at, updated_at) VALUES ("{item['Brand']}","{datetime.datetime.now()}","{datetime.datetime.now()}");'''
                    cursor.execute(sql_update_query)
                    brand_id = cursor.lastrowid
                if item.get('Color'):
                    if type(item['Color'])!=list:
                        colorss = item['Color'].split(',')
                    else:
                        colorss = item['Color']
                    
                    colors = ['"'+x+'"' for x in colorss]
                else:
                    colorss = []

                if colorss:

                    sql_update_query = f'''SELECT * from usedpickxyz_db1.color_list where name IN ({",".join(colors)});'''
                    cursor.execute(sql_update_query)
                    x = cursor.fetchall()
                    available_colors = {}
                    for i in x:
                        i = list(i)
                        available_colors[i[1]] = i[0]
                    colorid_list = [str(x) for x in list(available_colors.values())]
                    if len(colorss)==len(available_colors):
                        colorid_list = ",".join(colorid_list)
                    else:
                        colors_to_add = [c for c in colorss if c not in list(available_colors.keys())]
                        for clr in colors_to_add:
                            sql_update_query = f'''INSERT INTO usedpickxyz_db1.color_list (name, created_at, updated_at) VALUES ("{clr}","{datetime.datetime.now()}","{datetime.datetime.now()}");'''
                            cursor.execute(sql_update_query)
                            colorid_list.append(cursor.lastrowid)

                        colorid_list = ",".join([str(x) for x in colorid_list])
                else:
                    colorid_list = ""

                cat = item['categories']
                if cat:
                    all_cats_list = cat.split('>')
                    cats = ['"'+x+'"' for x in cat.split('>')]
                    child = cat.split('>')[-1]
                    sql_update_query = f'''SELECT * from usedpickxyz_db1.categories where category_name IN ({",".join(cats)}) AND type = "{name}";'''
                    cursor.execute(sql_update_query)
                    x = cursor.fetchall()
                    
                    cats_dict = {}
                    for c in x:
                        c = list(c)
                        cats_dict[c[1]] = c[0]
                    catid_list = [str(x) for x in list(cats_dict.values())]
                    if len(cat.split('>'))==len(cats_dict):
                        catid_list = ",".join(catid_list)
                    else:
                        cats_to_add = [c for c in cat.split('>') if c not in list(cats_dict.keys())]
                        isParent = 1
                        for i,clr in enumerate(cats_to_add):
                            if clr==child:
                                isParent = 0
                            parent_id = None
                            for k,ac in enumerate(all_cats_list):

                                if clr==ac:
                                    if k==0:
                                        break
                                    parent_id = cats_dict[all_cats_list[k-1]]
                                    break
                            if not parent_id:
                                sql_update_query = f'''INSERT INTO usedpickxyz_db1.categories (category_name, type, is_parent, parent_id, status, user_id, created_at, updated_at) VALUES ("{clr}", "{name}", {isParent}, null, 0, 1,"{datetime.datetime.now()}","{datetime.datetime.now()}");'''
                            else:
                                sql_update_query = f'''INSERT INTO usedpickxyz_db1.categories (category_name, type, is_parent, parent_id, status, user_id, created_at, updated_at) VALUES ("{clr}", "{name}", {isParent}, {parent_id}, 0, 1,"{datetime.datetime.now()}","{datetime.datetime.now()}");'''
                            cursor.execute(sql_update_query)
                            catid_list.append(cursor.lastrowid)
                            cats_dict[clr] = cursor.lastrowid
                        catid_list = ",".join([str(x) for x in catid_list])
                else:
                    catid_list = None

                sql_update_query = f'''SELECT id from usedpickxyz_db1.ebay_products where site_product_id = "{item['product_id']}";'''
                cursor.execute(sql_update_query)
                x = cursor.fetchone()
                for i in list(item.keys()):
                    if type(item[i])==list or type(item[i])==dict:
                        item[i] = json.dumps(item[i])
                    elif type(item[i])==tuple:
                        pass
                if x:
                    product_id, = x
                else:
                    sql_update_query = f'''INSERT INTO usedpickxyz_db1.ebay_products (product_reff_url, slug, title, price, site_product_id, images_url, shipping, brand_id, cat_id, material, color_id, site_specification_data, specification, short_description, long_description, rating, review_count, reviews, item_condition, created_at, updated_at) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s);'''
                    
                    cursor.execute(sql_update_query,(item['url'].split('?')[0], item['product_id'], item['title'],item['price'],int(item['product_id']), item['Image_URLs'], item['Shipping'], str(brand_id), str(catid_list), item['Material'], colorid_list, item['Site Specific Data'], item['Specifications'], item['Short Desc'], item['Long Desc'], item['rating'], item['rating_count'], item['reviews'], item['Condition'],datetime.datetime.now(),datetime.datetime.now()))
                    product_id = cursor.lastrowid
                
                
                sql_update_query = f'''SELECT id from usedpickxyz_db1.all_keywords where keyword = "{item['keyword']}";'''
                
                cursor.execute(sql_update_query)
                x = cursor.fetchone()
                if x:
                    kw_id, = x
                    
                    sql_update_query = f'''UPDATE usedpickxyz_db1.all_keywords SET updated_from_ebay = %s WHERE keyword = "%s";'''
                    cursor.execute(sql_update_query,(datetime.datetime.now(),kw_id))
                    
                else:
                    sql_update_query = f'''INSERT INTO usedpickxyz_db1.all_keywords (keyword, created_at, updated_from_etsy, updated_from_ebay, updated_from_amazon) VALUES ( %s, %s, %s, %s, %s);'''
                    cursor.execute(sql_update_query,(item['keyword'],datetime.datetime.now(),None,datetime.datetime.now(),None))
                    kw_id = cursor.lastrowid
                
                
                
                
                
                sql_update_query = f'''SELECT id from usedpickxyz_db1.keyword_ebay where keyword = "{kw_id}" AND product_id = {product_id};'''
                
                cursor.execute(sql_update_query)
                x = cursor.fetchone()
                if x:
                    pass
                else:
                    sql_update_query = f'''INSERT INTO usedpickxyz_db1.keyword_ebay (keyword, product_id, index_no) VALUES ("{kw_id}", {product_id}, {item["Index"]});'''
                    cursor.execute(sql_update_query)
                conn.commit()
                                    
            except mysql.connector.Error as error:
                
                retries -=1
                # reverting changes because of exception
                conn.rollback()
            finally:
                # closing database connection.
                if conn.is_connected():
                    cursor.close()
                    conn.close()
                    
                return





    def get_num(self,line):
        return re.findall(r"[-+]?(?:\d*\.\d+|\d+)",line)


    def parse(self, response):
        with open('source.html','wb') as f:
            f.write((response.body))
        
        soup = BeautifulSoup(response.body,'lxml')
        listings = soup.find_all('li',{'class':'s-item'})
        for inx, li in enumerate(listings):
            item = {}
            item['keyword'] = response.meta['kw']
            item['base'] = response.request.url
            item['Index'] = int(response.meta['count']) + inx
            try:
                item['title'] = li.find('span',{'role':'heading'}).text.strip()
            except:
                try:
                    item['title'] = li.find('a',{'class':'s-item__link'}).text.strip()
                except:
                    item['title'] = None
                    
                    continue
                    
            item['product_id'] = li.find('a',{'class':'s-item__link'}).get('href').split('itm/')[-1].split('?')[0]
            if item['title']=="Shop on eBay":
                continue
            item['url'] =  f"https://www.ebay.com/itm/{item['product_id']}/"
            if "Brand New" in str(li):
                item['Condition'] = "New"
            item['price'] = li.find('span',{'class':'s-item__price'})
            if item['price']:
                item['price'] = self.get_num(item['price'].text.replace(',',''))
                if item['price']:
                    item['price'] = item['price'][0]
                else:
                    item['price'] = None
            reviews = li.find('div',{'class':'s-item__reviews'})
            item['rating'] = None
            item['rating_count'] = 0
            if reviews:
                item['rating'] = reviews.find('span')
                if item['rating']:
                    item['rating'] = self.get_num(item['rating'].text)
                    if item['rating']:
                        item['rating'] = item['rating'][0]
                
                item['rating_count'] = li.find('span',{'class':"s-item__reviews-count"})
                if item['rating_count']:
                    item['rating_count']  = abs(int(self.get_num(item['rating_count'].text.replace(',','').strip())[0]))
            
            shipping_cost = -1
            shipping = li.find('span',{'class':"s-item__shipping s-item__logisticsCost"})
            if shipping:
                shipping_cost = shipping.text
                
                if "free" in shipping_cost.lower():
                    shipping_cost = 0
                elif shipping_cost.lower()=="shipping not specified":
                    shipping_cost = -1
                else:
                    sh = self.get_num(shipping_cost)
                    if sh:
                        shipping_cost = sh[0]

            item['Shipping'] = shipping_cost
            item['seller'] = None
            
            cj_num = random.randint(1,99999999)
            yield scrapy.Request(url=item['url'], callback=self.parse2, meta={'item':item,'cookiejar':cj_num})
            # yield item

        pagination = soup.find('a',{'aria-label':'Go to next search page'})
        if pagination:
            # next_page = pagination.find('a',{'aria-label':'Go to next search page'})
            # if next_page:
            url = pagination.get('href')
            if url:
                yield scrapy.Request(url=url, callback=self.parse,meta={'kw':response.meta['kw'],'count':int(response.meta['count'])+len(listings)})

    
    def parse2(self, response):
        s = re.sub('<br\s*?>', '\n', str(response.body))
        soup = BeautifulSoup(s.replace('\\n','\n'),'lxml')
        item = response.meta['item']
        if not item.get('condition'):
            item['Condition'] = soup.find('div',{'class':'d-item-condition-value'})
            if item['Condition']:
                item['Condition'] = item['Condition'].find('span').text
                if "Like New" in item['Condition']:
                    item['Condition'] = "Like New"
                if "New" in item['Condition']:
                    item['Condition'] = "New"
                if "Pre-owned" in item['Condition']:
                    item['Condition'] = "Pre-owned"
                if "Used" in item['Condition']:
                    item['Condition'] = "Used"
                if "not specified" in item['Condition']:
                    item['Condition'] = None
                
            
        item['title'] = soup.find('h1').text.strip()
        isAvailable = False
        if "no longer available" not in str(soup).lower():
            isAvailable = True
        emoji_pattern = re.compile(
        u"(\ud83d[\ude00-\ude4f])|"  # emoticons
        u"(\ud83c[\udf00-\uffff])|"  # symbols & pictographs (1 of 2)
        u"(\ud83d[\u0000-\uddff])|"  # symbols & pictographs (2 of 2)
        u"(\ud83d[\ude80-\udeff])|"  # transport & map symbols
        u"(\ud83c[\udde0-\uddff])"  # flags (iOS)
        "+", flags=re.UNICODE)

        if emoji_pattern.search(item['title']):
            item['title'] = item['title'].encode('unicode-escape')

        
        item['Image_URLs'] = ",".join([x.find('img').get('src') for x in soup.find_all('li', {'class':'v-pic-item'}) if x.find('img')])
        if not item['Image_URLs']:
            item['Image_URLs'] = soup.find(id='icImg')
            if item['Image_URLs']:
                item['Image_URLs'] = item['Image_URLs'].get('src')
        brand = soup.find('span',{'item-prop':'brand'})
        if brand:
            brand = brand.find('span').text.strip()
        item['Brand'] = brand
        material = None
        color = None
        weight = None
        length = None
        height = None
        width = None
        specs = soup.find_all('div',{'class':'ux-labels-values__labels'})
        for sp in specs:
            if "material" in sp.text.lower():
                material = sp.find_next('div',{'class':'ux-labels-values__values'}).text.strip()
            if "color" in sp.text.lower():
                color = sp.find_next('div',{'class':'ux-labels-values__values'}).text.strip()
            if "weight" in sp.text.lower():
                weight = sp.find_next('div',{'class':'ux-labels-values__values'}).text.strip()
            if "height" in sp.text.lower():
                height = sp.find_next('div',{'class':'ux-labels-values__values'}).text.strip()
            if "width" in sp.text.lower():
                width = sp.find_next('div',{'class':'ux-labels-values__values'}).text.strip()
            if "length" in sp.text.lower():
                length = sp.find_next('div',{'class':'ux-labels-values__values'}).text.strip()

        item['Material'] = material
        more_specs = soup.find('div',{'data-testid':'x-about-this-item'})
        more_specs_dict = {}
        if more_specs:
            
            rows = more_specs.find_all('div',{'class':'ux-labels-values__labels'})
            for row in rows:
                label = row.text.split(':')[0].strip()
                more_specs_dict[label] = row.find_next('div',{'class':'ux-labels-values__values'}).text.strip()

        instockqty = soup.find(id='qtySubTxt')
        if instockqty:
            instockqty = instockqty.text.split()[0]
            
        sales = soup.find('div',{'class':'soldwithfeedback'})
        if sales:
            sales = sales.text.split()[0].replace(',','')
        else:
            sales = None
        expirationTime = None
        if soup.find(id='MaxBidId'):
            isAuction = True
            expirationTime = soup.find('span',{'class':'vi-tm-left'})
            if expirationTime:
                expirationTime = expirationTime.text.replace('\\t','').replace('\\r','').replace('\n','').strip()
        else:
            isAuction = False
        description = soup.find(id='descriptioncontent')
        if description:
            description = description.text.strip().strip()
        else:
            description = soup.find('div',{'class':'ux-layout-section__textual-display--description'})
            if description:
                description = description.text.strip().strip()


        item['Long Desc'] = description
        item['Short Desc'] = ""
        cats = soup.find('nav',{'aria-labelledby':'listedInCat'})
        if cats:
            cats = cats.find_all('a')
        
        try:
            if cats:
                item['categories'] = ">".join([x.text for x in cats])
            else:
                item['categories'] = None
        except:
            item['categories'] = None
        
        
        
        item['Color'] = color    
        mbuy_dict =[]
        multibuy = soup.find_all('a',{'class':'vi-vpqp-pills'})
        if multibuy:
            for a in multibuy:
                mb = {}
                amount = self.get_num(a.find('div').text)
                if amount:
                    mb['quantity'] = amount[0]
                    try:
                        mb['price'] = self.get_num(a.find('div',{'class':'vpqp-price'}).text)[0]
                    except:
                        mb['price'] = None
                    mbuy_dict.append(mb)
        if not mbuy_dict:
            multibuy = soup.find_all('span',{'class':'vi-volume'})
            if multibuy:
                for a in multibuy:
                    mb = {}
                    amount = self.get_num(a.text)
                    if amount:
                        mb['quantity'] = amount[0]
                        mb['price'] = self.get_num(a.find_next('span',{'class':'vi-vprice'}).text)[0]
                    mbuy_dict.append(mb)
        item['store_name'] = soup.find('div',{'data-testid':'str-title'})
        if item['store_name']:
            item['store_name'] = item['store_name'].text.strip()
        item['Specifications'] = json.dumps({"Weight":weight,"Length":length, "Width":width, "Height": height})
        item['Site Specific Data'] = json.dumps({'store_name':item['store_name'],'item_specs':more_specs_dict,'quantity_sold':sales, "inStock": isAvailable, "quantity_based_prices": mbuy_dict, "available": instockqty, "auction": {"active":isAuction, "expiration":expirationTime}})
        item['reviews'] = []
        reviews_list = soup.find('div',{'class':'reviews'})
        if reviews_list:
            reviews_list = reviews_list.find_all('div',{'itemprop':' review'})
            for rev in reviews_list:
                review = {}
                author = rev.find('a',{'class':'review-item-author'})
                if author:
                    review['review_by']=author.text.strip()
                review['review_datetime'] = rev.find('span',{'itemprop':'datePublished'}.text)
                review['text'] = rev.find('p',{'itemprop':'reviewBody'}).text
                review['rating'] = rev.find('div',{'class':'ebay-star-rating'}).get('aria-label').split()[0]
                item['reviews'].append(review)
        item['reviews_count'] = len(item['reviews'])
        self.upload_entry(item)
        yield item
        
   