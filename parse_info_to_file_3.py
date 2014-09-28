#coding:utf-8

import os
import requests
from django.utils.encoding import *
from lxml import etree
import httplib
import traceback
import MySQLdb
import json
import time
import datetime
import redis
import simplejson
from redis import Redis
import simplejson as json

def func_category(parse_obj,namespaces):
    node = parse_obj.xpath("g:BrowseNodes/g:BrowseNode",namespaces = namespaces)
    if len(node) == 0:
        return ''
    node = node[0]
    node_name = node.xpath("g:Name",namespaces = namespaces)[0].text
    nodes = [] if node.xpath("g:IsCategoryRoot",namespaces = namespaces) else [node_name]
    ancestor = node.xpath("g:Ancestors/g:BrowseNode",namespaces = namespaces)[0] if node.xpath("g:Ancestors/g:BrowseNode",namespaces = namespaces) else False
    while ancestor:
        if ancestor.xpath("g:IsCategoryRoot",namespaces = namespaces):
            ancestor = ancestor.xpath("g:Ancestors/g:BrowseNode",namespaces = namespaces)[0] if ancestor.xpath("g:Ancestors/g:BrowseNode",namespaces = namespaces) else False
            continue
        ancestor_name = ancestor.xpath("g:Name",namespaces = namespaces)[0].text if ancestor.xpath("g:Name",namespaces = namespaces) else ancestor.xpath("g:BrowseNodeId",namespaces = namespaces)[0].text
        nodes.insert(0,ancestor_name)
        ancestor = ancestor.xpath("g:Ancestors/g:BrowseNode",namespaces = namespaces)[0] if ancestor.xpath("g:Ancestors/g:BrowseNode",namespaces = namespaces) else False
    category = '>'.join(nodes)
    return category

def func_spec_array(parse_obj,namespaces):
    spec_array = {}
    items = parse_obj.xpath("g:Variations/g:Item",namespaces = namespaces)
    for item in items:
        asin = item.xpath("g:ASIN",namespaces = namespaces)[0].text
        attributes = item.xpath("g:VariationAttributes/g:VariationAttribute",namespaces = namespaces)
        for attr in attributes:
            attri = attr.xpath("g:Name",namespaces = namespaces)[0].text.lower()
            value = attr.xpath("g:Value",namespaces = namespaces)[0].text.lower()  if attr.xpath("g:Value",namespaces = namespaces) else '__'+asin+'__'
            if spec_array.has_key(attri):
                if spec_array[attri].count(value) == 0:
                    spec_array[attri].append(value)
            else:
                spec_array[attri] = [value]
    return spec_array

def func_spu_recommend_purls(parse_obj,namespaces):
    spu_recommend_purls = []
    products = parse_obj.xpath("g:SimilarProducts/g:SimilarProduct",namespaces = namespaces)
    for product in products:
        asin = product.xpath("g:ASIN",namespaces = namespaces)[0].text
        url = 'www.amazon.com/dp/'+asin+'/?psc=1'
        spu_recommend_purls.append(url)
    return spu_recommend_purls


def func_sku_items_all(parse_obj,namespaces):
    skus = []
    recommand_urls = []
    is_self_sale = '0'
    items = parse_obj.xpath("g:Variations/g:Item",namespaces = namespaces)
    for item in items:
        asin = item.xpath("g:ASIN",namespaces = namespaces)[0].text
        sku_url = 'http://www.amazon.com/dp/'+asin+'/?psc=1'
        title = item.xpath("g:ItemAttributes/g:Title",namespaces = namespaces)[0].text if item.xpath("g:ItemAttributes/g:Title",namespaces = namespaces) else ''
        list_price = str(int(item.xpath("g:ItemAttributes/g:ListPrice/g:Amount",namespaces = namespaces)[0].text)/100.0) if item.xpath("g:ItemAttributes/g:ListPrice/g:Amount",namespaces = namespaces) else '-1'
        sku_images = []
        sku_image_list = []
        if item.xpath("g:LargeImage/g:URL",namespaces = namespaces):
            sku_images = item.xpath("g:LargeImage/g:URL",namespaces = namespaces)
        elif item.xpath("g:MediumImage/g:URL",namespaces = namespaces):
            sku_images = item.xpath("g:MediumImage/g:URL",namespaces = namespaces)
        elif item.xpath("g:SmallImage/g:URL",namespaces = namespaces):
            sku_images = item.xpath("g:SmallImage/g:URL",namespaces = namespaces)
        for image in sku_images:
            sku_image_list.append(image.text)
        attrs = {}
        attributes = item.xpath("g:VariationAttributes/g:VariationAttribute",namespaces = namespaces)
        for attr in attributes:
            attri = attr.xpath("g:Name",namespaces = namespaces)[0].text.lower()
            value = attr.xpath("g:Value",namespaces = namespaces)[0].text.lower() if attr.xpath("g:Value",namespaces = namespaces) else '__DEFAULT__'
            attrs[attri] = value

        s_price = '-1'
        stock = '-1'
        offers = []
        offs = item.xpath("g:Offers/g:Offer",namespaces = namespaces)
        for offer in offs:
            is_avalible = '0'
            if offer.xpath("g:OfferListing/g:Availability",namespaces = namespaces):
                is_avalible = '-1'
            elif offer.xpath("g:OfferListing/g:AvailabilityAttributes/g:AvailabilityType",namespaces = namespaces):
                is_avalible = '-1'
            if offer.xpath("g:OfferListing/g:SalePrice",namespaces = namespaces):
                sell_price = str(int(offer.xpath("g:OfferListing/g:SalePrice/g:Amount",namespaces = namespaces)[0].text)/100.0)
            else:
                sell_price = str(int(offer.xpath("g:OfferListing/g:Price/g:Amount",namespaces = namespaces)[0].text)/100.0) if offer.xpath("g:OfferListing/g:Price/g:Amount",namespaces = namespaces) else list_price
            merchant = offer.xpath("g:Merchant/g:Name",namespaces = namespaces)[0].text
            if merchant.find('Amazon')>=0:
                is_self_sale = '1'
                s_price = sell_price
                stock = is_avalible
            offers.append({'merchant_name':merchant,'merchant_price':sell_price,'merchant_stock':is_avalible})
        vip_price = '-1'
        deal_price = '-1'
        if s_price == '-1':
            s_price = offers[0]['merchant_price'] if offers else list_price
        if not offers:
            stock = '0'
        elif stock == '-1':
            stock = offers[0]['merchant_stock']
        height,length,width,h_unit,weight,weight_unit = '','','','','',''
        item_demen = item.xpath("g:ItemAttributes/g:ItemDimensions",namespaces = namespaces)
        if item_demen:
            item_demen = item_demen[0]
            height = item_demen.xpath("g:Height",namespaces = namespaces)[0].text if item_demen.xpath("g:Height",namespaces = namespaces) else ''
            length = item_demen.xpath("g:Length",namespaces = namespaces)[0].text if item_demen.xpath("g:Length",namespaces = namespaces) else ''
            width = item_demen.xpath("g:Width",namespaces = namespaces)[0].text if item_demen.xpath("g:Width",namespaces = namespaces) else ''
            h_unit = item_demen.xpath("g:Height",namespaces = namespaces)[0].attrib['Units'] if item_demen.xpath("g:Height",namespaces = namespaces) else ''
            weight = item_demen.xpath("g:Weight",namespaces = namespaces)[0].text if item.xpath("g:Weight",namespaces = namespaces) else ''
            weight_unit = item.xpath("g:Weight",namespaces = namespaces)[0].attrib['Units'] if item.xpath("g:Weight",namespaces = namespaces) else ''
        package = item.xpath("g:ItemAttributes/g:PackageDimensions",namespaces = namespaces)
        if len(package)>0:
            package = package[0]
            height = package.xpath("g:Height",namespaces = namespaces)[0].text if package.xpath("g:Height",namespaces = namespaces) else ''
            length = package.xpath("g:Length",namespaces = namespaces)[0].text if package.xpath("g:Length",namespaces = namespaces) else ''
            width = package.xpath("g:Width",namespaces = namespaces)[0].text if package.xpath("g:Width",namespaces = namespaces) else ''
            h_unit = package.xpath("g:Height",namespaces = namespaces)[0].attrib['Units'] if package.xpath("g:Height",namespaces = namespaces) else ''
            weight = package.xpath("g:Weight",namespaces = namespaces)[0].text if package.xpath("g:Weight",namespaces = namespaces) and not weight else ''
            weight_unit = package.xpath("g:Weight",namespaces = namespaces)[0].attrib['Units'] if package.xpath("g:Weight",namespaces = namespaces) and not weight_unit else ''
        measure = {'width':width,'length':length,'height':height,'unit':h_unit}
        weight = {'value':weight,'unit':weight_unit}

        skus.append({'sku_url':sku_url,'sku_id':asin,'sku_title':title,'sku_intro':'','vip_price':vip_price,'list_price':list_price,'sell_price':s_price,'deal_price':deal_price,'stock':stock,'sku_merchant_sales':offers,'spec_attr':attrs,'weight':weight,'sku_images':sku_image_list,'measure':measure,'sku_recommend_purls':recommand_urls,'sibling_sku_items':[],'is_self_sale':is_self_sale})
    flag = 1
    if len(skus)>1:
        for sku in skus:
            if not sku['spec_attr']:
                flag = 0
    return flag,skus

def func_image(item,namespaces):
    images = []
    image_list = []
    if item.xpath("g:LargeImage/g:URL",namespaces = namespaces):
        images = item.xpath("g:LargeImage/g:URL",namespaces = namespaces)
    elif item.xpath("g:MediumImage/g:URL",namespaces = namespaces):
        images = item.xpath("g:LargeImage/g:URL",namespaces = namespaces)
    elif item.xpath("g:SmallImage/g:URL",namespaces = namespaces):
        images = item.xpath("g:LargeImage/g:URL",namespaces = namespaces)
    for image in images:
        image_list.append(image.text)
    return image_list


def func_no_skus(spu_id,spu_url,spu_title,spu_images,spu_intro,list_price,spu_recommand_urls,item,namespaces):
    #该spu无下属sku，需要把spu的属性给sku
    is_self_sale = '0'
    s_price = '-1'
    offers = []
    stock = '-1'
    offs = item.xpath("g:Offers/g:Offer",namespaces = namespaces)
    for offer in offs:
        is_avalible = '0'
        if offer.xpath("g:OfferListing/g:Availability",namespaces = namespaces):
            is_avalible = '-1'
        elif offer.xpath("g:OfferListing/g:AvailabilityAttributes/g:AvailabilityType",namespaces = namespaces):
            is_avalible = '-1'
        if offer.xpath("g:OfferListing/g:SalePrice"):
            sell_price = str(int(offer.xpath("g:OfferListing/g:SalePrice/g:Amount/text()").extract()[0])/100.0)
        else:
            sell_price = str(int(offer.xpath("g:OfferListing/g:Price/g:Amount",namespaces = namespaces)[0].text)/100.0) if offer.xpath("g:OfferListing/g:Price/g:Amount",namespaces = namespaces) else list_price
        merchant = offer.xpath("g:Merchant/g:Name",namespaces = namespaces)[0].text if offer.xpath("g:Merchant/g:Name",namespaces = namespaces) else ''
        if merchant.find('Amazon')>=0:
            is_self_sale = '1'
            s_price = sell_price
            stock = is_avalible
        offers.append({'merchant_name':merchant,'merchant_price':sell_price,'merchant_stock':is_avalible})
    if s_price == '-1':
        s_price = offers[0]['merchant_price'] if offers else list_price
    if not offers:
        stock = '0'
    elif stock == '-1':
        stock = offers[0]['merchant_stock']
    height,length,width,h_unit,weight,weight_unit = '','','','','',''
    item_demen = item.xpath("g:ItemAttributes/g:ItemDimensions",namespaces = namespaces)
    if item_demen:
        item_demen = item_demen[0]
        height = item_demen.xpath("g:Height",namespaces = namespaces)[0].text if item_demen.xpath("g:Height",namespaces = namespaces) else ''
        length = item_demen.xpath("g:Length",namespaces = namespaces)[0].text if item_demen.xpath("g:Length",namespaces = namespaces) else ''
        width = item_demen.xpath("g:Width",namespaces = namespaces)[0].text if item_demen.xpath("g:Width",namespaces = namespaces) else ''
        h_unit = item_demen.xpath("g:Height",namespaces = namespaces)[0].attrib['Units'] if item_demen.xpath("g:Height",namespaces = namespaces) else ''
        weight = item_demen.xpath("g:Weight",namespaces = namespaces)[0].text if item.xpath("g:Weight",namespaces = namespaces) else ''
        weight_unit = item.xpath("g:Weight",namespaces = namespaces)[0].attrib['Units'] if item.xpath("g:Weight",namespaces = namespaces) else ''
    package = item.xpath("g:ItemAttributes/g:PackageDimensions",namespaces = namespaces)
    if package:
        package = package[0]
        height = package.xpath("g:Height",namespaces = namespaces)[0].text if package.xpath("g:Height",namespaces = namespaces) and not height else ''
        length = package.xpath("g:Length",namespaces = namespaces)[0].text if package.xpath("g:Length",namespaces = namespaces) and not length else ''
        width = package.xpath("g:Width",namespaces = namespaces)[0].text if package.xpath("g:Width",namespaces = namespaces) and not width else ''
        h_unit = package.xpath("g:Height",namespaces = namespaces)[0].attrib['Units'] if package.xpath("g:Height",namespaces = namespaces) and not h_unit else ''
        weight = package.xpath("g:Weight",namespaces = namespaces)[0].text if package.xpath("g:Weight",namespaces = namespaces) and not weight else ''
        weight_unit = package.xpath("g:Weight",namespaces = namespaces)[0].attrib['Units'] if package.xpath("g:Weight",namespaces = namespaces) and not weight_unit else ''
    measure = {'width':width,'length':length,'height':height,'unit':h_unit}
    weight = {'value':weight,'unit':weight_unit}
    attrs = {}
    attributes = item.xpath("g:VariationAttributes/g:VariationAttribute",namespaces = namespaces)
    for attr in attributes:
        attri = attr.xpath("g:Name",namespaces = namespaces)[0].text.lower()
        value = attr.xpath("g:Value",namespaces = namespaces)[0].text.lower() if attr.xpath("g:Value",namespaces = namespaces) else '__DEFAULT__'
        attrs[attri] = value
    skus = [{'sku_url':spu_url,'sku_id':spu_id,'sku_title':spu_title,'sku_intro':spu_intro,'vip_price':'-1','list_price':list_price,'sell_price':s_price,'deal_price':'-1','stock':stock,'sku_merchant_sales':offers,'spec_attr':attrs,'weight':weight,'sku_images':spu_images,'measure':measure,'sku_recommend_purls':spu_recommand_urls,'sibling_sku_items':[],'is_self_sale':is_self_sale}]

    return skus



count = 0
cache = redis.Redis('amazonspider.ubtbui.0001.use1.cache.amazonaws.com',6379)
#conn = MySQLdb.connect(host = 'superspider.cxhhmjmrvwn3.us-east-1.rds.amazonaws.com',user = 'superspider',passwd = 'tHkejiss',db = 'amazon_spider',charset='utf8')
#cursor = conn.cursor()
namespaces = {'g':'http://webservices.amazon.com/AWSECommerceService/2011-08-01'}
lists = os.walk('/data_pcvol/xmls/2014-9-16/')
warn = open('/data_pcvol/amazon_data/parse_warn_info.txt','a')
ff = open('/data_pcvol/amazon_data/amazon_data_log_3.txt','a')
for p,dirs,files in lists:
    for fi in files:
        path = p+'/'+fi
        print path
        if cache.hexists('parsed_file',fi):
            continue
        with open(path) as f:
            try:
                tree = etree.parse(f)
                root = tree.getroot()
                #errors = root.xpath('//Errors',namespaces = namespaces)
                #if len(errors)>0:
                #    print 'errors , ',path+'\\'+fi
                #continue
                items = root.xpath('//g:Items/g:Item',namespaces = namespaces)
                for item in items:
                    print 'time1 :',datetime.datetime.now()
                    message = []   #错误信息
                    msg = ''       #警告信息
                    spu_id = item.xpath("g:ASIN",namespaces = namespaces)[0].text if item.xpath("g:ASIN",namespaces = namespaces) else ''
                    spec_array = func_spec_array(item,namespaces)
                    if str(spec_array).find('__')>=0:
                        warn.write('spu_asin = %s , Warning : the sku has a attr without value' % spu_id)
                    spu_recommand_urls = func_spu_recommend_purls(item,namespaces)
                    currency_code = 'USD'
                    brand = item.xpath("g:ItemAttributes/g:Brand",namespaces = namespaces)[0].text if item.xpath("g:ItemAttributes/g:Brand",namespaces = namespaces) else ''
                    
                    #cursor.execute('select is_parsed from pa where parent_asin = %s',(spu_id,))
                    #x = cursor.fetchone()
                    #if x is None or x[0]:
                    #    print 'x : ',x
                    #    continue
                    c = cache.hget('amazon_info',spu_id)
                    if c is not None:
                        cache.hincrby('amazon_info',spu_id,1)
                        #cache.hset('amazon_info',spu_id ,int(c)+1)
                        continue
                    cache.hset('amazon_info',spu_id ,1)
                    spu_url = 'http://www.amazon.com/dp/'+spu_id+'/?psc=1'
                    spu_title = item.xpath("g:ItemAttributes/g:Title",namespaces = namespaces)[0].text if item.xpath("g:ItemAttributes/g:Title",namespaces = namespaces) else ''
                    spu_images = func_image(item,namespaces)
                    spu_intro = item.xpath("g:EditorialReviews/g:EditorialReview/g:Content",namespaces = namespaces)[0].text if item.xpath("g:EditorialReviews/g:EditorialReview/g:Content",namespaces = namespaces) else ''
                    spu_intro = smart_str(spu_intro)
                    category = func_category(item,namespaces)
                    list_price = str(int(item.xpath("g:ItemAttributes/g:ListPrice/g:Amount",namespaces = namespaces)[0].text)/100.0) if item.xpath("g:ItemAttributes/g:ListPrice/g:Amount",namespaces = namespaces) else '-1'
                    flag,skus = func_sku_items_all(item,namespaces)
                    if flag == 0:
                        message.append('1')
                    if len(skus) == 0:
                        skus = func_no_skus(spu_id,spu_url,spu_title,spu_images,spu_intro,list_price,spu_recommand_urls,item,namespaces)
                    if len(skus)>1:
                        i = 0
                        while i<len(skus)-1:
                            if cmp(skus[i]['spec_attr'],skus[i+1]['spec_attr']) == 0:
                                warn.write('spu_asin = %s , Warning : Two skus has the same spec_attr,please specify' % spu_id)
                                del skus[i] if float(skus[i]['sell_price']) < float(skus[i+1]['sell_price']) else del skus[i+1]
                                i = i-1
                            i = i+1
                    
                    d = {'source_url':'','spu_url':spu_url,'spu_id':spu_id,'spu_title':spu_title,'spu_intro':spu_intro,'category':category,'currency_code':'USD','spec_array':spec_array,'spec_array_keys':spec_array.keys(),'spu_images':spu_images,'brand':brand,'sku_items':skus}
                    if not d['spu_id']:
                        flag = 0
                        message.append('2')
                    if not d['spu_title']:
                        flag = 0
                        message.append('3')
                    
                    if not d['spu_intro']:
                        message.append('10')
                    if not d['category']:
                        message.append('11')
                    if not d['spec_array'].keys():
                        message.append('12')
                    if not d['spu_images']:
                        message.append('13')
                    if not d['brand']:
                        message.append('14')
                    if not skus:
                        message.append('15')
                    
                    for sku in skus:
                        sku_msg = ''
                        if not sku['sku_id']:
                            flag = 0
                            message.append('4')
                            continue
                        if not sku['sku_title']:
                            flag = 0
                            message.append('5')
                        #if not sku['sku_merchant_sales']:
                        #    flag = 0
                        #    message.append('6')
                        if sku['sell_price'] == '-1':
                            flag = 0
                            message.append('7')
                    if flag != 0:
                        message.append('0')
                    
                    if flag == 0:
                        #cursor.execute('update pa set is_parsed = %s , state = %s , error_code = %s where parent_asin = %s',(1,'error',set(message),spu_id))
                        #conn.commit()
                        #send_log_srv('error', '', message, spu_id)
                        msg = '\t'.join(['error',' ',str(message),spu_id])
                        ff.write(msg+'\n')
                        print 'flag = 0'
                        continue
                    print 'time2 : ',datetime.datetime.now()
                    headers = {"Content-type":"application/json"}
                    params = (
                    {'code': 0, 'message': '', 'data': {'data':d}}
                    )
                    count = int(cache.get('count'))
                    dr = str(count/10000)
                    if not os.path.exists('/data_pcvol/amazon_data/'+dr):
                        os.makedirs('/data_pcvol/amazon_data/'+dr)
                    pth = '/data_pcvol/amazon_data/'+dr+'/'+spu_id+'.txt'
                    f = open(pth,'w+')
                    f.write(simplejson.dumps(params))
                    f.close()
                    #cursor.execute('update pa set is_parsed = %s , state = %s , path = %s , error_code = %s where parent_asin = %s',(1,'success',dr,set(message),spu_id))
                    #conn.commit()
                    #send_log_srv('success', dr, message, spu_id)
                    msg = '\t'.join(['success',dr,str(message),spu_id])
                    ff.write(msg+'\n')
                    cache.incr('count')
            except Exception,e:
                print e
                #print 'error : ',path+'/'+fi
                import traceback
                print traceback.print_exc()
            cache.hset('parsed_file',fi,1)
#cursor.close()
#conn.close()
print 'count : ',count
ff.close()
warn.close()