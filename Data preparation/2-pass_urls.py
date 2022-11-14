# Databricks notebook source
# MAGIC %pip install reverse_geocoder
# MAGIC %pip install country_converter
# MAGIC 
# MAGIC %pip install opencv-python
# MAGIC %pip install --upgrade pip
# MAGIC %pip install scikit-image

# COMMAND ----------

all_country_urls = spark.read.parquet("/mnt/lsde/group08/Country_dictionary/SpatialInfo.parquet/")
all_country_urls.display()

# COMMAND ----------

con_list = ['UA', 'KI', 'BS', 'NA', 'SO', 'LY', 'AU', 'ES', 'IR', 'VE', 'ML', 'GS', 'AS', 'MP', 'AF', 'CD', 'AO', 'RO', 'CG', 'SV', 'NP', 'AI', 'JM', 'AE', 'FO', 'DO', 'GY', 'KE', 'UG', 'KG', 'SS', 'PR', 'EH', 'DK', 'AZ', 'HR', 'BN', 'HK', 'BA', 'GE', 'LV', 'HT', 'KH', 'LU', 'BQ', 'TJ', 'ST', 'MM', 'SC', 'SL', 'CX', 'UZ', 'NC', 'ZA', 'CM', 'CC', 'GQ', 'TD', 'MU', 'GP', 'MA', 'NO', 'TF', 'VN', 'GN', 'MS', 'TG', 'QA', 'XK', 'KW', 'AX', 'PT', 'CV', 'CL', 'CA', 'BR', 'DE', 'EC', 'NR', 'IN', 'TN', 'IT', 'SE', 'MZ', 'SH', 'IE', 'GA', 'BB', 'LB', 'DJ', 'SI', 'BE', 'DZ', 'FI', 'ZM', 'MN', 'PG', 'LR', 'NE', 'TR', 'MR', 'MY', 'CK', 'NU', 'SD', 'FJ', 'VU', 'UY', 'PW', 'PM', 'LA', 'NL', 'IQ', 'GH', 'TW', 'BD', 'HN', 'GT', 'CR', 'KR', 'PA', 'CU', 'KM', 'TM', 'ER', 'SR', 'LT', 'MK', 'EE', 'VG', 'AT', 'CZ', 'KY', 'AW', 'VI', 'IL', 'GM', 'RW', 'CH', 'SK', 'GD', 'VC', 'IM', 'SB', 'RE', 'CN', 'RU', 'ID', 'FK', 'PK', 'KZ', 'TZ', 'US', 'PN', 'AG', 'PH', 'FM', 'EG', 'BO', 'TK', 'KN', 'TC', 'BW', 'MW', 'BY', 'GU', 'GR', 'KP', 'WS', 'BZ', 'TH', 'BJ', 'ZW', 'SN', 'OM', 'HU', 'NI', 'YT', 'AM', 'ME', 'SX', 'LC', 'CW', 'DM', 'BT', 'JE', 'SZ', 'CI', 'PL', 'GL', 'TV', 'PY', 'BM', 'GB', 'AQ', 'FR', 'MG', 'ET', 'BF', 'GW', 'NZ', 'CO', 'CF', 'NF', 'PS', 'MT', 'MD', 'MQ', 'TL', 'BG', 'LS', 'CY', 'TT', 'BH', 'RS', 'AL', 'JO', 'SG', 'BI', 'PF', 'MX', 'SJ', 'TO', 'SA', 'YE', 'GF', 'WF', 'MV', 'MH', 'PE', 'JP', 'LK', 'SY', 'NG', 'AR']

con_name_list = ['Ukraine', 'Kiribati', 'Bahamas', 'Namibia', 'Somalia', 'Libya', 'Australia', 'Spain', 'Iran', 'Venezuela', 'Mali', 'South Georgia and South Sandwich Is.', 'American Samoa', 'Northern Mariana Islands', 'Afghanistan', 'DR Congo', 'Angola', 'Romania', 'Congo Republic', 'El Salvador', 'Nepal', 'Anguilla', 'Jamaica', 'United Arab Emirates', 'Faeroe Islands', 'Dominican Republic', 'Guyana', 'Kenya', 'Uganda', 'Kyrgyz Republic', 'South Sudan', 'Puerto Rico', 'Western Sahara', 'Denmark', 'Azerbaijan', 'Croatia', 'Brunei Darussalam', 'Hong Kong', 'Bosnia and Herzegovina', 'Georgia', 'Latvia', 'Haiti', 'Cambodia', 'Luxembourg', 'Bonaire, Saint Eustatius and Saba', 'Tajikistan', 'Sao Tome and Principe', 'Myanmar', 'Seychelles', 'Sierra Leone', 'Christmas Island', 'Uzbekistan', 'New Caledonia', 'South Africa', 'Cameroon', 'Cocos (Keeling) Islands', 'Equatorial Guinea', 'Chad', 'Mauritius', 'Guadeloupe', 'Morocco', 'Norway', 'French Southern Territories', 'Vietnam', 'Guinea', 'Montserrat', 'Togo', 'Qatar', 'Kosovo', 'Kuwait', 'Aland Islands', 'Portugal', 'Cabo Verde', 'Chile', 'Canada', 'Brazil', 'Germany', 'Ecuador', 'Nauru', 'India', 'Tunisia', 'Italy', 'Sweden', 'Mozambique', 'St. Helena', 'Ireland', 'Gabon', 'Barbados', 'Lebanon', 'Djibouti', 'Slovenia', 'Belgium', 'Algeria', 'Finland', 'Zambia', 'Mongolia', 'Papua New Guinea', 'Liberia', 'Niger', 'Turkey', 'Mauritania', 'Malaysia', 'Cook Islands', 'Niue', 'Sudan', 'Fiji', 'Vanuatu', 'Uruguay', 'Palau', 'St. Pierre and Miquelon', 'Laos', 'Netherlands', 'Iraq', 'Ghana', 'Taiwan', 'Bangladesh', 'Honduras', 'Guatemala', 'Costa Rica', 'South Korea', 'Panama', 'Cuba', 'Comoros', 'Turkmenistan', 'Eritrea', 'Suriname', 'Lithuania', 'North Macedonia', 'Estonia', 'British Virgin Islands', 'Austria', 'Czech Republic', 'Cayman Islands', 'Aruba', 'United States Virgin Islands', 'Israel', 'Gambia', 'Rwanda', 'Switzerland', 'Slovakia', 'Grenada', 'St. Vincent and the Grenadines', 'Isle of Man', 'Solomon Islands', 'Reunion', 'China', 'Russia', 'Indonesia', 'Falkland Islands', 'Pakistan', 'Kazakhstan', 'Tanzania', 'United States', 'Pitcairn', 'Antigua and Barbuda', 'Philippines', 'Micronesia, Fed. Sts.', 'Egypt', 'Bolivia', 'Tokelau', 'St. Kitts and Nevis', 'Turks and Caicos Islands', 'Botswana', 'Malawi', 'Belarus', 'Guam', 'Greece', 'North Korea', 'Samoa', 'Belize', 'Thailand', 'Benin', 'Zimbabwe', 'Senegal', 'Oman', 'Hungary', 'Nicaragua', 'Mayotte', 'Armenia', 'Montenegro', 'Sint Maarten', 'St. Lucia', 'Curacao', 'Dominica', 'Bhutan', 'Jersey', 'Eswatini', "Cote d'Ivoire", 'Poland', 'Greenland', 'Tuvalu', 'Paraguay', 'Bermuda', 'United Kingdom', 'Antarctica', 'France', 'Madagascar', 'Ethiopia', 'Burkina Faso', 'Guinea-Bissau', 'New Zealand', 'Colombia', 'Central African Republic', 'Norfolk Island', 'Palestine', 'Malta', 'Moldova', 'Martinique', 'Timor-Leste', 'Bulgaria', 'Lesotho', 'Cyprus', 'Trinidad and Tobago', 'Bahrain', 'Serbia', 'Albania', 'Jordan', 'Singapore', 'Burundi', 'French Polynesia', 'Mexico', 'Svalbard and Jan Mayen Islands', 'Tonga', 'Saudi Arabia', 'Yemen', 'French Guiana', 'Wallis and Futuna Islands', 'Maldives', 'Marshall Islands', 'Peru', 'Japan', 'Sri Lanka', 'Syria', 'Nigeria', 'Argentina']

# COMMAND ----------

import country_converter as coco
continent_list = coco.convert(names=con_list, src ='ISO2',to='continent')

print(continent_list)

# COMMAND ----------

print(len(con_list))
print(len(con_name_list))
print(len(continent_list))

country_number = len(continent_list)
print(country_number)

# COMMAND ----------

import os
import time
import numpy as np
import cv2
from skimage import io

# COMMAND ----------

# display(dbutils.fs.mkdirs("/FileStore/group08/"))

# COMMAND ----------

face_cascade = cv2.CascadeClassifier('/dbfs/FileStore/group08/haarcascade_frontalface_default.xml')
eye_cascade = cv2.CascadeClassifier('/dbfs/FileStore/group08/haarcascade_eye.xml')
def detectface(url):     
    try:
        img = io.imread(url)
        gray = cv2.cvtColor(img,cv2.COLOR_BGR2GRAY)
    except BaseException:
        return 0
    else:
        faces = face_cascade.detectMultiScale(gray, 1.1,2,cv2.CASCADE_SCALE_IMAGE,(50,50),(300,300))
        detectfaces_number = 0
        for face in faces:
            if face.any():
#                 print('face detected')
                detectfaces_number += 1
            x,y,w,h = face
            img = cv2.rectangle(img,(x,y),(x+w,y+h),(255,0,0),2)
            roi_gray = gray[y:y+h, x:x+w]
            roi_color = img[y:y+h, x:x+w]
            eyes = eye_cascade.detectMultiScale(roi_gray)
            for (ex,ey,ew,eh) in eyes:
                cv2.rectangle(roi_color,(ex,ey),(ex+ew,ey+eh),(0,255,0),2)
        return detectfaces_number
   

# COMMAND ----------

for i in range(121,123):
    print(i)
    
for i in range(0,5):
    print(i)

# COMMAND ----------

# for i in range(32,50):
for i in range(33,137):
    print(i)
    country_name = con_list[i]
    continent_name = continent_list[i]
    
    print(country_name)
    print(continent_name)
    country_urls = spark.read.parquet("/mnt/lsde/group08/Country_dictionary/SpatialInfo.parquet/continent="+continent_name+"/country="+country_name)
    urls = country_urls.select(country_urls[0])
    url_number = urls.count()
    print(url_number)
    
    urls_rows = urls.head(url_number)
    print(type(urls_rows))
    url_list = []
    for i in range(url_number):
        url_list.append(urls_rows[i]['url'])
    
    print(len(url_list))
#     print(url_list)
    
    start = time.time()
    #有人脸的图片数量
    qualified_img_num = 0
    qualified_img_url_List = []
    for url in url_list:
#         print("here")
        face_num = detectface(url)
        if 3>=face_num >= 1:
            qualified_img_num += 1
            qualified_img_url_List.append(url)
            print(qualified_img_num)
        if qualified_img_num == 500:
            break

    print(qualified_img_url_List)
    end = time.time()
    print(f'use:{end-start}seconds')
    
    if(len(qualified_img_url_List)>1):
        def get_host(item):
            number = 1
            url =item
            return (number,url)
        rdd = sc.parallelize(qualified_img_url_List)
        qualified_img_url_DF = rdd.map(get_host).toDF()
        qualified_img_url_DF = qualified_img_url_DF.toDF('number','url')#'url'
        display(qualified_img_url_DF)

        face_images_urls1 = qualified_img_url_DF.select(qualified_img_url_DF[1])
        face_images_urls = face_images_urls1.join(all_country_urls,"url")
        # append overwrite
       
    face_images_urls.repartition("continent","country").write.mode("append").partitionBy("continent","country").parquet("/mnt/lsde/group08/Country_dictionary/FaceImages500.parquet")
    
    

# COMMAND ----------

# for i in range(32,235):
# for i in range(0,32):
# (121,137)
for i in range(137,185):
    print(i)
    country_name = con_list[i]
    continent_name = continent_list[i]
    
    print(country_name)
    print(continent_name)
    country_urls = spark.read.parquet("/mnt/lsde/group08/Country_dictionary/SpatialInfo.parquet/continent="+continent_name+"/country="+country_name)
    urls = country_urls.select(country_urls[0])
    url_number = urls.count()
    print(url_number)
    
    urls_rows = urls.head(url_number)
    print(type(urls_rows))
    url_list = []
    for i in range(url_number):
        url_list.append(urls_rows[i]['url'])
    
    print(len(url_list))
#     print(url_list)
    
    start = time.time()
    #有人脸的图片数量
    qualified_img_num = 0
    qualified_img_url_List = []
    for url in url_list:
#         print("here")
        face_num = detectface(url)
        if 3>=face_num >= 1:
            qualified_img_num += 1
            qualified_img_url_List.append(url)
            print(qualified_img_num)
        if qualified_img_num == 500:
            break

    print(qualified_img_url_List)
    end = time.time()
    print(f'use:{end-start}seconds')
    
    if(len(qualified_img_url_List)>1):
        def get_host(item):
            number = 1
            url =item
            return (number,url)
        rdd = sc.parallelize(qualified_img_url_List)
        qualified_img_url_DF = rdd.map(get_host).toDF()
        qualified_img_url_DF = qualified_img_url_DF.toDF('number','url')#'url'
        # display(qualified_img_url_DF)

        face_images_urls1 = qualified_img_url_DF.select(qualified_img_url_DF[1])
        face_images_urls = face_images_urls1.join(all_country_urls,"url")
        # append overwrite
       
    face_images_urls.repartition("continent","country").write.mode("append").partitionBy("continent","country").parquet("/mnt/lsde/group08/Country_dictionary/FaceImages500.parquet")
    
    
