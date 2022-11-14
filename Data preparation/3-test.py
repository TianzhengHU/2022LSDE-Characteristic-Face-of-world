# Databricks notebook source
con_list = ['UA', 'KI', 'BS', 'NA', 'SO', 'LY', 'AU', 'ES', 'IR', 'VE', 'ML', 'GS', 'AS', 'MP', 'AF', 'CD', 'AO', 'RO', 'CG', 'SV', 'NP', 'AI', 'JM', 'AE', 'FO', 'DO', 'GY', 'KE', 'UG', 'KG', 'SS', 'PR', 'EH', 'DK', 'AZ', 'HR', 'BN', 'HK', 'BA', 'GE', 'LV', 'HT', 'KH', 'LU', 'BQ', 'TJ', 'ST', 'MM', 'SC', 'SL', 'CX', 'UZ', 'NC', 'ZA', 'CM', 'CC', 'GQ', 'TD', 'MU', 'GP', 'MA', 'NO', 'TF', 'VN', 'GN', 'MS', 'TG', 'QA', 'XK', 'KW', 'AX', 'PT', 'CV', 'CL', 'CA', 'BR', 'DE', 'EC', 'NR', 'IN', 'TN', 'IT', 'SE', 'MZ', 'SH', 'IE', 'GA', 'BB', 'LB', 'DJ', 'SI', 'BE', 'DZ', 'FI', 'ZM', 'MN', 'PG', 'LR', 'NE', 'TR', 'MR', 'MY', 'CK', 'NU', 'SD', 'FJ', 'VU', 'UY', 'PW', 'PM', 'LA', 'NL', 'IQ', 'GH', 'TW', 'BD', 'HN', 'GT', 'CR', 'KR', 'PA', 'CU', 'KM', 'TM', 'ER', 'SR', 'LT', 'MK', 'EE', 'VG', 'AT', 'CZ', 'KY', 'AW', 'VI', 'IL', 'GM', 'RW', 'CH', 'SK', 'GD', 'VC', 'IM', 'SB', 'RE', 'CN', 'RU', 'ID', 'FK', 'PK', 'KZ', 'TZ', 'US', 'PN', 'AG', 'PH', 'FM', 'EG', 'BO', 'TK', 'KN', 'TC', 'BW', 'MW', 'BY', 'GU', 'GR', 'KP', 'WS', 'BZ', 'TH', 'BJ', 'ZW', 'SN', 'OM', 'HU', 'NI', 'YT', 'AM', 'ME', 'SX', 'LC', 'CW', 'DM', 'BT', 'JE', 'SZ', 'CI', 'PL', 'GL', 'TV', 'PY', 'BM', 'GB', 'AQ', 'FR', 'MG', 'ET', 'BF', 'GW', 'NZ', 'CO', 'CF', 'NF', 'PS', 'MT', 'MD', 'MQ', 'TL', 'BG', 'LS', 'CY', 'TT', 'BH', 'RS', 'AL', 'JO', 'SG', 'BI', 'PF', 'MX', 'SJ', 'TO', 'SA', 'YE', 'GF', 'WF', 'MV', 'MH', 'PE', 'JP', 'LK', 'SY', 'NG', 'AR']

con_name_list = ['Ukraine', 'Kiribati', 'Bahamas', 'Namibia', 'Somalia', 'Libya', 'Australia', 'Spain', 'Iran', 'Venezuela', 'Mali', 'South Georgia and South Sandwich Is.', 'American Samoa', 'Northern Mariana Islands', 'Afghanistan', 'DR Congo', 'Angola', 'Romania', 'Congo Republic', 'El Salvador', 'Nepal', 'Anguilla', 'Jamaica', 'United Arab Emirates', 'Faeroe Islands', 'Dominican Republic', 'Guyana', 'Kenya', 'Uganda', 'Kyrgyz Republic', 'South Sudan', 'Puerto Rico', 'Western Sahara', 'Denmark', 'Azerbaijan', 'Croatia', 'Brunei Darussalam', 'Hong Kong', 'Bosnia and Herzegovina', 'Georgia', 'Latvia', 'Haiti', 'Cambodia', 'Luxembourg', 'Bonaire, Saint Eustatius and Saba', 'Tajikistan', 'Sao Tome and Principe', 'Myanmar', 'Seychelles', 'Sierra Leone', 'Christmas Island', 'Uzbekistan', 'New Caledonia', 'South Africa', 'Cameroon', 'Cocos (Keeling) Islands', 'Equatorial Guinea', 'Chad', 'Mauritius', 'Guadeloupe', 'Morocco', 'Norway', 'French Southern Territories', 'Vietnam', 'Guinea', 'Montserrat', 'Togo', 'Qatar', 'Kosovo', 'Kuwait', 'Aland Islands', 'Portugal', 'Cabo Verde', 'Chile', 'Canada', 'Brazil', 'Germany', 'Ecuador', 'Nauru', 'India', 'Tunisia', 'Italy', 'Sweden', 'Mozambique', 'St. Helena', 'Ireland', 'Gabon', 'Barbados', 'Lebanon', 'Djibouti', 'Slovenia', 'Belgium', 'Algeria', 'Finland', 'Zambia', 'Mongolia', 'Papua New Guinea', 'Liberia', 'Niger', 'Turkey', 'Mauritania', 'Malaysia', 'Cook Islands', 'Niue', 'Sudan', 'Fiji', 'Vanuatu', 'Uruguay', 'Palau', 'St. Pierre and Miquelon', 'Laos', 'Netherlands', 'Iraq', 'Ghana', 'Taiwan', 'Bangladesh', 'Honduras', 'Guatemala', 'Costa Rica', 'South Korea', 'Panama', 'Cuba', 'Comoros', 'Turkmenistan', 'Eritrea', 'Suriname', 'Lithuania', 'North Macedonia', 'Estonia', 'British Virgin Islands', 'Austria', 'Czech Republic', 'Cayman Islands', 'Aruba', 'United States Virgin Islands', 'Israel', 'Gambia', 'Rwanda', 'Switzerland', 'Slovakia', 'Grenada', 'St. Vincent and the Grenadines', 'Isle of Man', 'Solomon Islands', 'Reunion', 'China', 'Russia', 'Indonesia', 'Falkland Islands', 'Pakistan', 'Kazakhstan', 'Tanzania', 'United States', 'Pitcairn', 'Antigua and Barbuda', 'Philippines', 'Micronesia, Fed. Sts.', 'Egypt', 'Bolivia', 'Tokelau', 'St. Kitts and Nevis', 'Turks and Caicos Islands', 'Botswana', 'Malawi', 'Belarus', 'Guam', 'Greece', 'North Korea', 'Samoa', 'Belize', 'Thailand', 'Benin', 'Zimbabwe', 'Senegal', 'Oman', 'Hungary', 'Nicaragua', 'Mayotte', 'Armenia', 'Montenegro', 'Sint Maarten', 'St. Lucia', 'Curacao', 'Dominica', 'Bhutan', 'Jersey', 'Eswatini', "Cote d'Ivoire", 'Poland', 'Greenland', 'Tuvalu', 'Paraguay', 'Bermuda', 'United Kingdom', 'Antarctica', 'France', 'Madagascar', 'Ethiopia', 'Burkina Faso', 'Guinea-Bissau', 'New Zealand', 'Colombia', 'Central African Republic', 'Norfolk Island', 'Palestine', 'Malta', 'Moldova', 'Martinique', 'Timor-Leste', 'Bulgaria', 'Lesotho', 'Cyprus', 'Trinidad and Tobago', 'Bahrain', 'Serbia', 'Albania', 'Jordan', 'Singapore', 'Burundi', 'French Polynesia', 'Mexico', 'Svalbard and Jan Mayen Islands', 'Tonga', 'Saudi Arabia', 'Yemen', 'French Guiana', 'Wallis and Futuna Islands', 'Maldives', 'Marshall Islands', 'Peru', 'Japan', 'Sri Lanka', 'Syria', 'Nigeria', 'Argentina']

continent_list=['Europe', 'Oceania', 'America', 'Africa', 'Africa', 'Africa', 'Oceania', 'Europe', 'Asia', 'America', 'Africa', 'Antarctica', 'Oceania', 'Oceania', 'Asia', 'Africa', 'Africa', 'Europe', 'Africa', 'America', 'Asia', 'America', 'America', 'Asia', 'Europe', 'America', 'America', 'Africa', 'Africa', 'Asia', 'Africa', 'America', 'Africa', 'Europe', 'Asia', 'Europe', 'Asia', 'Asia', 'Europe', 'Asia', 'Europe', 'America', 'Asia', 'Europe', 'America', 'Asia', 'Africa', 'Asia', 'Africa', 'Africa', 'Asia', 'Asia', 'Oceania', 'Africa', 'Africa', 'Asia', 'Africa', 'Africa', 'Africa', 'America', 'Africa', 'Europe', 'Africa', 'Asia', 'Africa', 'America', 'Africa', 'Asia', 'Europe', 'Asia', 'Europe', 'Europe', 'Africa', 'America', 'America', 'America', 'Europe', 'America', 'Oceania', 'Asia', 'Africa', 'Europe', 'Europe', 'Africa', 'Africa', 'Europe', 'Africa', 'America', 'Asia', 'Africa', 'Europe', 'Europe', 'Africa', 'Europe', 'Africa', 'Asia', 'Oceania', 'Africa', 'Africa', 'Asia', 'Africa', 'Asia', 'Oceania', 'Oceania', 'Africa', 'Oceania', 'Oceania', 'America', 'Oceania', 'America', 'Asia', 'Europe', 'Asia', 'Africa', 'Asia', 'Asia', 'America', 'America', 'America', 'Asia', 'America', 'America', 'Africa', 'Asia', 'Africa', 'America', 'Europe', 'Europe', 'Europe', 'America', 'Europe', 'Europe', 'America', 'America', 'America', 'Asia', 'Africa', 'Africa', 'Europe', 'Europe', 'America', 'America', 'Europe', 'Oceania', 'Africa', 'Asia', 'Europe', 'Asia', 'America', 'Asia', 'Asia', 'Africa', 'America', 'Oceania', 'America', 'Asia', 'Oceania', 'Africa', 'America', 'Oceania', 'America', 'America', 'Africa', 'Africa', 'Europe', 'Oceania', 'Europe', 'Asia', 'Oceania', 'America', 'Asia', 'Africa', 'Africa', 'Africa', 'Asia', 'Europe', 'America', 'Africa', 'Asia', 'Europe', 'America', 'America', 'America', 'America', 'Asia', 'Europe', 'Africa', 'Africa', 'Europe', 'America', 'Oceania', 'America', 'America', 'Europe', 'Antarctica', 'Europe', 'Africa', 'Africa', 'Africa', 'Africa', 'Oceania', 'America', 'Africa', 'Oceania', 'Asia', 'Europe', 'Europe', 'America', 'Asia', 'Europe', 'Africa', 'Asia', 'America', 'Asia', 'Europe', 'Europe', 'Asia', 'Asia', 'Africa', 'Oceania', 'America', 'Europe', 'Oceania', 'Asia', 'Asia', 'America', 'Oceania', 'Asia', 'Oceania', 'America', 'Asia', 'Asia', 'Asia', 'Africa', 'America']

# COMMAND ----------

# /dbfs/mnt/lsde/group08/Country_dictionary/FaceImages200.parquet/continent=Africa/country=AO
test  = spark.read.parquet("/mnt/lsde/group08/Country_dictionary/FaceImages500.parquet")
test.display()

# COMMAND ----------

# /dbfs/mnt/lsde/group08/Country_dictionary/FaceImages200.parquet/continent=Africa/country=AO
number = test.count()

# COMMAND ----------

print(number)

# COMMAND ----------

test.groupBy("country").count().display()

# COMMAND ----------

for i in range(10,11):
    country_name = con_list[i]
    continent_name = continent_list[i]
    print(i)
    a_country_urls = test.filter(test[3]==country_name)
#     a_country_urls  = spark.read.parquet("/mnt/lsde/group08/Country_dictionary/FaceImages500.parquet/continent="+continent_name+"/country="+country_name)
    a_country_urls.display()
    urls = a_country_urls.select(a_country_urls[0])
    url_number = urls.count()
    print(url_number)
    
    urls_rows = urls.head(url_number)
    print(type(urls_rows))
    url_list = []
    for i in range(url_number):
        url_list.append(urls_rows[i]['url'])
    print(country_name)
    print(continent_name)
    print(len(url_list))
    print(url_list)

# COMMAND ----------

# /dbfs/mnt/lsde/group08/Country_dictionary/FaceImages200.parquet/continent=Africa/country=AO
i=6
country_name = con_list[i]
continent_name = continent_list[i]
print(country_name)
print(continent_name)
test1  = spark.read.parquet("/mnt/lsde/group08/Country_dictionary/FaceImages200.parquet/continent="+continent_name+"/country="+country_name)
test1.display()

# COMMAND ----------

# display(dbutils.fs.mkdirs("/mnt/lsde/group08/Country_dictionary/country=UA"))
# display(dbutils.fs.mv("/mnt/lsde/group08/Country_dictionary/FaceImages200.parquet/country=UA/_committed_1876527546981111462","/mnt/lsde/group08/Country_dictionary/country=UA"))

# display(dbutils.fs.mv("/mnt/lsde/group08/Country_dictionary/FaceImages200.parquet/_committed_8429951023113965158","/mnt/lsde/group08/Country_dictionary"))

# display(dbutils.fs.rm("/mnt/lsde/group08/Country_dictionary/FaceImages200.parquet/country=UA"))
#dbfs:/mnt/lsde/group08/Country_dictionary/FaceImages200.parquet/country=KI
