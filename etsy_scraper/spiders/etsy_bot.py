from urllib import request
import scrapy
from bs4 import BeautifulSoup
import re, json
import sys, random
import mysql.connector
import json
import datetime
import urllib.parse
from slugify import slugify
from threading import Thread


class EtsyBotSpider(scrapy.Spider):
    name = 'etsy'
    allowed_domains = ['etsy.com']
    # start_urls = ['https://www.etsy.com/search?q=Plastic+Doilies']
    count = 0
    conn = mysql.connector.connect(host='154.38.160.70',
                                    database='sql_usedpick_com',
                                    user='sql_usedpick_com',
                                    password='e5empmmWBjBEr5s6')
    cursor = conn.cursor()
    proxy = 'http://user-wakber:waqas123@all.dc.smartproxy.com:10000'
    def start_requests(self):
        kws = [x.replace(',','') for x in open('kws.txt','r').read().split('\n') if x]
        
        for kw in kws[2000:]:
            url =  f"https://www.etsy.com/search?q={urllib.parse.quote_plus(kw)}"    
            yield scrapy.Request(url=url,callback=self.parse,meta={'kw':kw,'count':1 })

    def upload_entry(self, item, retries=1):    
        while retries:
            try:
                for i in list(item.keys()):
                    if not item.get(i):
                        item[i]=None
                name =  "etsy"
                conn = mysql.connector.connect(host='154.38.160.70',
                                    database='sql_usedpick_com',
                                    user='sql_usedpick_com',
                                    password='e5empmmWBjBEr5s6')
                conn.autocommit = False
                cursor = conn.cursor(buffered=True)
                try:
                    item_slug = slugify(item['Brand'])
                except:
                    item_slug = ""
                sql_update_query = f'''SELECT id from sql_usedpick_com.brand_list where name = "{item['Brand']}"'''
                cursor.execute(sql_update_query)
                x = cursor.fetchone()
                if x:
                    brand_id, = x
                    if type(brand_id)==tuple:
                        brand_id, = brand_id
                    # sql_update_query = f'''INSERT INTO sql_usedpick_com.brand_list (name,slug) VALUES ("{item['Brand']}","{item_slug}");'''
                    # cursor.execute(sql_update_query)
                else:
                    sql_update_query = f'''INSERT INTO sql_usedpick_com.brand_list (name, slug) VALUES ("{item['Brand']}","{item_slug}");'''
                    cursor.execute(sql_update_query)
                    brand_id = cursor.lastrowid
                cat = item['categories']
                
                if cat:
                    all_cats_list = [x.strip() for x in cat.split('>') if x][1:]
                    indx_dict = {}
                    for kn, cat in enumerate(all_cats_list):
                        indx_dict[cat] = kn
                    cats = ['"'+x.strip()+'"' for x in all_cats_list if x]
                    
                    sql_update_query = f'''SELECT * from sql_usedpick_com.categories where category_name IN ({",".join(cats)}) AND type = "{name}";'''
                    cursor.execute(sql_update_query)
                    x = cursor.fetchall()
                    
                    cats_dict = {}
                    for c in x:
                        c = list(c)
                        cats_dict[c[1]] = c[0]
                    catid_list = [str(x) for x in list(cats_dict.values())]
                    if len(all_cats_list)==len(list(cats_dict.keys())):
                        catid_list = ",".join(catid_list)
                    else:
                        cats_to_add = [c for c in all_cats_list if c not in list(cats_dict.keys())]
                        for i,clr in enumerate(cats_to_add):
                            ct_slug = slugify(clr)
                            parent_id = None
                            for k,ac in enumerate(all_cats_list):
                                if clr==ac:
                                    if k==0:
                                        break
                                    parent_id = cats_dict.get(all_cats_list[k-1])
                                    if not parent_id:
                                        parent_id = catid_list[-1]
                                    break
                            if not parent_id:
                                sql_update_query = f'''INSERT INTO sql_usedpick_com.categories (category_name, slug, type, depth, parent_id, status, user_id) VALUES ("{clr}","{ct_slug}","{name}", {indx_dict[clr]}, null, 0, 1);'''
                            else:
                                sql_update_query = f'''INSERT INTO sql_usedpick_com.categories (category_name, slug, type, depth, parent_id, status, user_id) VALUES ("{clr}", "{ct_slug}","{name}", {indx_dict[clr]},{parent_id}, 0, 1);'''
                            
                            cursor.execute(sql_update_query)
                            catid_list.append(cursor.lastrowid)
                            cats_dict[clr] = cursor.lastrowid
                        catid_list = ",".join([str(x) for x in catid_list])
                else:
                    catid_list = None

                sql_update_query = f'''SELECT id from sql_usedpick_com.etsy_products where site_product_id = "{item['product_id']}";'''
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
                    sql_update_query = f'''INSERT INTO sql_usedpick_com.etsy_products (product_reff_url, slug, title, price, site_product_id, images_url, shipping, brand_id, site_specification_data, specification, short_description, long_description, rating, review_count, reviews, item_condition) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s);'''
                    cursor.execute(sql_update_query,(item['url'].split('?')[0], slugify(item['title']), item['title'],item['price'],int(item['product_id']), item['Image_URLs'], item['Shipping'], str(brand_id), item['Site Specific Data'], item['Specifications'], item['Short Desc'], item['Long Desc'], item['rating'], item['rating_count'], item['reviews'], item['Condition']))
                    product_id = cursor.lastrowid
                
                kw_slug = slugify(item['keyword'])
                sql_update_query = f'''SELECT id from sql_usedpick_com.all_keywords where keyword = "{item['keyword']}";'''
                
                cursor.execute(sql_update_query)
                x = cursor.fetchone()
                if x:
                    kw_id, = x
                    
                    sql_update_query = f'''UPDATE sql_usedpick_com.all_keywords SET updated_from_etsy = %s WHERE id = "%s";'''
                    cursor.execute(sql_update_query,(datetime.datetime.now(),kw_id))
                    sql_update_query = f'''UPDATE sql_usedpick_com.all_keywords SET slug = %s WHERE id = "%s";'''
                    cursor.execute(sql_update_query,(kw_slug,kw_id))
                    
                else:
                    sql_update_query = f'''INSERT INTO sql_usedpick_com.all_keywords (keyword, slug, updated_from_etsy) VALUES ( %s, %s, %s);'''
                    cursor.execute(sql_update_query,(item['keyword'],kw_slug,datetime.datetime.now()))
                    kw_id = cursor.lastrowid
                
                
                
                sql_update_query = f'''SELECT id from sql_usedpick_com.etsy_keyword where keyword_id = "{kw_id}" AND product_id = {product_id};'''
                
                cursor.execute(sql_update_query)
                x = cursor.fetchone()
                if x:
                    pass
                else:
                    sql_update_query = f'''INSERT INTO sql_usedpick_com.etsy_keyword (keyword_id, product_id, index_no) VALUES ("{kw_id}", {product_id}, {item["Index"]});'''
                    cursor.execute(sql_update_query)

                all_category_ids =      [int(x.strip()) for x in catid_list.split(',') if x]

                

                # Categories Table Update

                sql_update_query = f'''SELECT id from sql_usedpick_com.etsy_product_categories where product_id = {product_id} AND keyword_id = {kw_id};'''
                cursor.execute(sql_update_query)
                x = cursor.fetchone()
                if x:
                    pass
                else:
                    data = []
                    for cid in all_category_ids:
                        data.append((kw_id, product_id, cid),)
                    sql_update_query = f'''INSERT INTO sql_usedpick_com.etsy_product_categories (keyword_id, product_id, category_id) VALUES (%s, %s, %s);'''
                    cursor.executemany(sql_update_query,data)

                # filters = json.loads(item['Site Specific Data'])
                # if filters.get('item_specs'):
                #     specs = filters.get('item_specs')
                #     for spec in list(specs.keys()):
                #         try:
                #             try:
                #                 sql_update_query = f'''INSERT INTO sql_usedpick_com.attribute_list (name, slug) VALUES (%s, %s);'''
                #                 cursor.execute(sql_update_query,(spec, slugify(spec)))
                #                 filter_id = cursor.lastrowid
                #             except Exception as e:
                #                 # print('Spec Loop ',e)
                #                 sql_update_query = f'''SELECT id FROM sql_usedpick_com.attribute_list WHERE name = "{spec}";'''
                #                 cursor.execute(sql_update_query)
                #                 filter_id, = cursor.fetchone()
                #             value = specs[spec]
                #             if spec == "item_condition":
                #                 continue
                #             sql_update_query = f'''INSERT IGNORE INTO sql_usedpick_com.etsy_attribute_details (product_id, keyword_id, filter_id, filter_value) VALUES (%s, %s, %s, %s);'''
                #             cursor.execute(sql_update_query,(product_id, kw_id, filter_id, value))
                #         except:
                #             pass


                if item.get('Color'):
                    if type(item['Color'])!=list:
                        colorss = item['Color'].split(',')
                    else:
                        colorss = item['Color']
                    
                    colors = ['"'+x+'"' for x in colorss]
                else:
                    colorss = []

                if colorss:
                    filter_id = 1
                    for color in colorss:
                        sql_update_query = f'''INSERT IGNORE INTO sql_usedpick_com.etsy_attribute_details (product_id, keyword_id, filter_id, filter_value) VALUES (%s, %s, %s, %s);'''
                        cursor.execute(sql_update_query,(product_id, kw_id, filter_id, color))


                if item.get('Material'):
                    if type(item['Material'])!=list:
                        colorss = item['Material'].split(',')
                    else:
                        colorss = item['Material']
                    
                    colors = ['"'+x+'"' for x in colorss]
                else:
                    colorss = []

                if colorss:
                    filter_id = 2
                    for color in colorss:
                        sql_update_query = f'''INSERT IGNORE INTO sql_usedpick_com.etsy_attribute_details (product_id, keyword_id, filter_id, filter_value) VALUES (%s, %s, %s, %s);'''
                        cursor.execute(sql_update_query,(product_id, kw_id, filter_id, color))
                
                conn.commit()
                                    
            except mysql.connector.Error as error:
                print(error)    
                retries -=1
                # reverting changes because of exception
                conn.rollback()
            
            except Exception as e:
                print(e)

            finally:
                # closing database connection.
                if conn.is_connected():
                    cursor.close()
                    conn.close()
                    
                return




    def get_num(self,line):
        return re.findall(r"[-+]?(?:\d*\.\d+|\d+)",line)


    def parse(self, response):
        
        soup = BeautifulSoup(response.body,'lxml')
        listings = soup.find_all('li', {'class':'wt-list-unstyled'})
        for inx, li in enumerate(listings):
            item = {}
            item['keyword'] = response.meta['kw']
            item['base'] = response.request.url
            item['Index'] = int(response.meta['count']) + inx
            try:
                item['title'] = li.find('h2').text.strip()
            except:
                try:
                    item['title'] = li.find('a',{'class':'listing-link'}).text.strip()
                except:
                    item['title'] = None
                    
                    continue
                    
            item['url'] =  li.find('a',{'class':'listing-link'}).get('href')
            
            item['product_id'] = item['url'].split('listing/')[-1].split('/')[0]
            
            item['price'] = li.find('span',{'class':'currency-value'})
            if item['price']:
                item['price'] = self.get_num(item['price'].text.replace(',',''))[0]
            item['rating'] = li.find('input',{'name':'rating'})
            if item['rating']:
                item['rating'] = self.get_num(item['rating'].get('value'))[0]
            item['rating_count'] = li.find('span',{'class':"wt-text-body-01"})
            if item['rating_count']:
                item['rating_count']  = abs(int(self.get_num(item['rating_count'].text.strip())[0]))
            item['seller'] = li.find('p',{'class':'wt-text-caption wt-text-truncate wt-text-gray wt-mb-xs-1'})
            if item['seller']:
                item['seller']=item['seller'].text.strip().split('\n')[2]
            cj_num = random.randint(1,99999999)
            yield scrapy.Request(url=item['url'], callback=self.parse2, meta={'item':item,'cookiejar':cj_num, })
                                                
        pagination = soup.find_all('li',{'class':'wt-action-group__item-container'})[-1]
        if pagination:
            url = pagination.find('a').get('href')
            if url:
                if int(response.meta['count'])+len(listings)<300:
                    yield scrapy.Request(url=url, callback=self.parse,meta={'kw':response.meta['kw'],'count':int(response.meta['count'])+len(listings)})
                else:
                    return

    
    def parse2(self, response):
        s = re.sub('<br\s*?>', '\n', str(response.body))
        soup = BeautifulSoup(s.replace('\\n','\n'),'lxml')
        item = response.meta['item']
        try:
            item['title'] = soup.find('h1').text.strip()
        except:
            return
        isAvailable = True
        oos = soup.find('div',{'data-buy-box-region':'price'})
        if "out of stock" in str(oos).lower():
            isAvailable = False
        emoji_pattern = re.compile(
        u"(\ud83d[\ude00-\ude4f])|"  # emoticons
        u"(\ud83c[\udf00-\uffff])|"  # symbols & pictographs (1 of 2)
        u"(\ud83d[\u0000-\uddff])|"  # symbols & pictographs (2 of 2)
        u"(\ud83d[\ude80-\udeff])|"  # transport & map symbols
        u"(\ud83c[\udde0-\uddff])"  # flags (iOS)
        "+", flags=re.UNICODE)

        if emoji_pattern.search(item['title']):
            item['title'] = item['title'].encode('unicode-escape')
        try:        
            item['Image_URLs'] = ",".join([x.find('img').get('data-src-zoom-image') for x in soup.find('ul', {'class':'carousel-pane-list'}).find_all('li') if x.find('img')])
        except:
            item['Image_URLs'] = ""
        shipping_cost = -1
        shipping = soup.find(id='desktop-shipping-content-toggle')
        if shipping:
            spans = shipping.find_all('span')
            for s in spans:
                if s.text=="Cost to ship":
                    shipping_cost = self.get_num(s.find_next('p').text.replace(',',''))
                    if shipping_cost:
                        shipping_cost = shipping_cost[0]
                    else:
                        shipping_cost = -1
                    break
        if shipping_cost=="Free":
            shipping_cost = 0
        item['Shipping'] = shipping_cost
        brand = soup.find('div',{'data-action':'follow-shop-listing-header'})
        if brand:
            brand = brand.find('span').text.strip()
        item['Brand'] = brand
        material = soup.find(id="legacy-materials-product-details")
        if material:
            material = material.text.split(':')[-1].strip()

        item['Material'] = material

        all_vars = {}
        variations = soup.find('div',{'data-selector':'listing-page-variations'})
        if variations:
            variations = variations.find_all('div',{'class':'wt-validation'})
            for var in variations:
                all_vars[var.find('label').text.strip()] = []
                for v in var.find_all('option')[1:]:
                    all_vars[var.find('label').text.strip()].append(v.text.split('(')[0].strip())
        
        unique_points = soup.find(id="product-details-content-toggle")
        isHandmade = False
        isVintage = False
        if unique_points:
            divs = soup.find_all('div',{'class':'wt-ml-xs-2'})
            for d in divs:
                if "handmade" in d.text.lower():
                    isHandmade = True
                if "vintage" in d.text.lower():
                    isVintage = True
                    
        isRare = False
        if "Rare find" in soup.text:
            isRare = True
            
        sales = soup.find('div',{'data-action':'follow-shop-listing-header'})
        if sales:
            sales = sales.find('span',{'class':'wt-text-caption'})
            if sales:
                sales = sales.text.split()[0].replace(',','')
        else:
            sales = None
        description = soup.find('div',{'data-id':'description-text'})
        if description:
            description = description.text.strip().strip()

        item['Long Desc'] = description
        item['Short Desc'] = ""
        item['categories'] = ">".join([x.text for x in soup.find('div',{'class':'wt-text-caption wt-text-center-xs wt-text-left-lg'}).find_all('a')])
        csfr = soup.find('meta',{"name":"csrf_nonce"}).get('content')
        shop_id = str(soup).split('"shop_id":')[-1].split(',')[0]
        
        color = None
        weight = None
        for ky in list(all_vars.keys()):
            if "weight" in ky.lower():
                weight = all_vars[ky]
            if "color" in ky.lower() or "colour" in ky.lower():
                color = all_vars[ky]
        
        item['Color'] = color
        item['Condition'] = ""
        


        length = None
        width = None
        height = None
        lis = soup.find_all('li',{'class':'wt-list-unstyled'})
        for li in lis:
            if "Width:" in str(li):
                width = li.text.split(':')[-1].strip()
            if "Length:" in str(li):
                length = li.text.split(':')[-1].strip()
            if "Height:" in str(li):
                height = li.text.split(':')[-1].strip()
                
        item['Specifications'] = json.dumps({"Weight":weight,"Length":length, "Width":width, "Height": height})
        item['Site Specific Data'] = json.dumps({'item_specs':{'quantity_sold':sales, "handmade": isHandmade, "vintage": isVintage, "rare": isRare}, "variations": all_vars, "available": isAvailable})
        item['reviews'] = []
        payload = {
                    "log_performance_metrics": "false",
                    "specs[reviews][]": "Etsy\Web\ListingPage\Reviews\ApiSpec",
                    "specs[reviews][1][listing_id]": str(item['product_id']),
                    "specs[reviews][1][shop_id]": str(shop_id),
                    "specs[reviews][1][render_complete]": "true",
                    "specs[reviews][1][active_tab]": "same_listing_reviews",
                    "specs[reviews][1][should_lazy_load_images]": "false",
                    "specs[reviews][1][should_use_pagination]": "true",
                    "specs[reviews][1][page]": '1',
                    "specs[reviews][1][should_show_variations]": "false",
                    "specs[reviews][1][is_reviews_untabbed_cached]": "false",
                    "specs[reviews][1][was_landing_from_external_referrer]": "false",
                    "specs[reviews][1][sort_option]": "Relevancy"}

        
        # yield scrapy.http.JsonRequest('https://www.etsy.com/api/v3/ajax/bespoke/member/neu/specs/reviews', callback=self.rev_parse, data=payload, meta={'cookiejar': response.meta['cookiejar'],'item':item, 'pl':payload, 'csfr':csfr},headers={"x-csrf-token": csfr, "user-agent":"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/105.0.0.0 Safari/537.36"})
        # yield scrapy.http.JsonRequest('https://www.etsy.com/api/v3/ajax/bespoke/member/neu/specs/reviews', callback=self.rev_parse, data=payload, meta={'cookiejar': response.meta['cookiejar'],'item':item, 'pl':payload, 'csfr':csfr},headers={"x-csrf-token": csfr, "user-agent":"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/105.0.0.0 Safari/537.36"})
        yield scrapy.FormRequest(url='https://www.etsy.com/api/v3/ajax/bespoke/member/neu/specs/reviews',
                    formdata=payload,
                     method='POST',
                    callback=self.rev_parse, meta={'cookiejar': response.meta['cookiejar'],'handle_httpstatus_all': True,'item':item, 'pl':payload, 'csfr':csfr,},headers={"x-csrf-token": csfr})
        

    def rev_parse(self, response):
        item = response.meta['item']
        try:
            obb = (json.loads(response.body))
        except:
            item['reviews_count'] = len(item['reviews'])
            self.upload_entry(item)
            yield item
            return
        if not obb['output'] or len(item['reviews'])>4:
            
            item['reviews_count'] = len(item['reviews'])
            self.upload_entry(item)
            yield item
            return
        
        rev_soup = BeautifulSoup(json.loads(response.body)['output']['reviews'],'lxml')

        all_revs = rev_soup.find_all('div',{'class':'wt-grid__item-xs-12'})
        
        reviews = []
        for rev in all_revs:
            review = {}
            stars = rev.find('input',{'name':'rating'}).get('value')
            review['rating'] = stars
            review['review'] = rev.find('p').text.strip()
            try:
                review['author'] = rev.find('p',{'class':'wt-text-caption wt-text-gray'}).find('a').text.strip()
            except:
                review['author'] = ""
            review['review_date'] = rev.find('p',{'class':'wt-text-caption wt-text-gray'}).text.strip().replace(review['author'],"").strip()
            if review['review']=="Purchased item:&nbsp":
                review['review'] = None
            if review not in reviews:
                reviews.append(review)
        
        if reviews:
            for r in reviews:
                item['reviews'].append(r)
            pl = response.meta['pl']
            pl['specs[reviews][1][page]'] = str(int(pl['specs[reviews][1][page]']) + 1)
            yield scrapy.FormRequest(url='https://www.etsy.com/api/v3/ajax/bespoke/member/neu/specs/reviews',
                    formdata=pl,
                     method='POST',
                    callback=self.rev_parse, meta={'cookiejar': response.meta['cookiejar'],'handle_httpstatus_all': True,'item':item, 'pl':pl, 'csfr':response.meta['csfr']},headers={"x-csrf-token": response.meta['csfr']})
        else:
            item['reviews_count'] = len(item['reviews'])
            # t = Thread(target=self.upload_entry,args=(item,),daemon=True)
            # t.start()
            self.upload_entry(item)
            yield item