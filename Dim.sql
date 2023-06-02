CREATE DATABASE IF NOT EXISTS dim_fact_db;

USE dim_fact_db;

--#--------------------------------------------------------------------------------------------------------#--
--#--------------------------------------------Stream Dimension--------------------------------------------#--
--#--------------------------------------------------------------------------------------------------------#--

CREATE EXTERNAL TABLE IF NOT EXISTS dim_stream(
    streamID INT, 
    streamTitle STRING, 
    streamType STRING
);

USE dim_stream;

SELECT streamID, streamTitle, streamType INTO dim_users FROM <nama_table_stream>;
--------------Atau--------------
CREATE TABLE dim_stream AS
SELECT
    streamID,
    streamTitle,
    streamType
FROM
    <nama_table_stream>;

-------------##--##-----OPSI KEDUA-----##--##-------------
-------------Dengan asumsi data sudah di hdfs-------------
from pyspark.sql import SparkSession, HiveContext

spark = SparkSession.builder \
    .appName("HiveExample") \
    .config("spark.sql.catalogImplementation", "hive") \
    .config("hive.metastore.uris", "thrift://localhost:9083") \
    .enableHiveSupport() \
    .getOrCreate()

createtable = '''
    CREATE EXTERNAL TABLE IF NOT EXISTS dim_stream(
        streamID INT, 
        streamTitle STRING, 
        streamType STRING
    )   
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
    STORED AS TEXTFILE
    LOCATION '<hdfs>'
    TBLPROPERTIES ('skip.header.line.count'='1')
'''
spark.sql(createtable)

--#-------------------------------------------------------------------------------------------------------#--    
--#--------------------------------------------VIDEO DIMENSION--------------------------------------------#--
--#-------------------------------------------------------------------------------------------------------#--

CREATE TABLE IF NOT EXISTS dim_videos(
    video_id INT, 
    video_title STRING, 
    video_description STRING, 
    video_created_at TIMESTAMP, 
    video_published_at TIMESTAMP, 
    video_url TEXT, 
    video_viewable STRING, 
    video_view_count INT, 
    video_language STRING, 
    video_type STRING, 
    video_duration STRING
);

USE dim_videos;

SELECT video_id, video_title, video_description, video_created_at, video_published_at, video_url, video_viewable, video_view_count, video_language, video_type, video_duration
INTO dim_videos FROM <nama_table_video>;
---------Atau---------
CREATE TABLE dim_videos AS
SELECT
    video_id,
    video_title,
    video_description,
    video_created_at,
    video_published_at,
    video_url,
    video_viewable,
    video_view_count,
    video_language,
    video_type,
    video_duration
FROM
    <nama_table_video>
---------##--##-----OPSI KEDUA-----##--##---------
---------Dengan asumsi data sudah di hdfs---------
from pyspark.sql import SparkSession, HiveContext

spark = SparkSession.builder \
    .appName("HiveExample") \
    .config("spark.sql.catalogImplementation", "hive") \
    .config("hive.metastore.uris", "thrift://localhost:9083") \
    .enableHiveSupport() \
    .getOrCreate()

createtable = '''
    CREATE TABLE IF NOT EXISTS dim_videos(
        video_id INT, 
        video_title STRING, 
        video_description STRING, 
        video_created_at TIMESTAMP, 
        video_published_at TIMESTAMP, 
        video_url TEXT, 
        video_viewable STRING, 
        video_view_count INT, 
        video_language STRING, 
        video_type STRING, 
        video_duration STRING
    )
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
    STORED AS TEXTFILE
    LOCATION '<hdfs>'
    TBLPROPERTIES ('skip.header.line.count'='1')
'''
spark.sql(createtable)

--#-------------------------------------------------------------------------------------------------------#--
--#--------------------------------------------Users Dimension--------------------------------------------#--
--#-------------------------------------------------------------------------------------------------------#--

CREATE TABLE IF NOT EXISTS dim_users (
    user_id INT,
    user_login_name STRING, 
    user_display_name STRING, 
    user_type STRING, 
    user_broadcaster_type STRING, 
    user_description STRING, 
    user_email STRING, 
    user_created_at STRING
);

USE dim_users;

SELECT user_id, user_login_name, user_display_name, user_type, user_broadcaster_type, user_description, user_email, user_created_at
INTO dim_users FROM <nama_table_users>
---------Atau---------
CREATE TABLE dim_users AS
SELECT
    user_id,
    user_login_name,
    user_display_name,
    user_type,
    user_broadcaster_type,
    user_description,
    user_email,
    user_created_at
FROM
    <nama_table_users>;

---------##--##-----OPSI KEDUA-----##--##---------
---------Dengan asumsi data sudah di hdfs---------
from pyspark.sql import SparkSession, HiveContext

spark = SparkSession.builder \
    .appName("HiveExample") \
    .config("spark.sql.catalogImplementation", "hive") \
    .config("hive.metastore.uris", "thrift://localhost:9083") \
    .enableHiveSupport() \
    .getOrCreate()

createtable = '''
    CREATE TABLE IF NOT EXISTS dim_users (
        user_id INT,
        user_login_name STRING, 
        user_display_name STRING, 
        user_type STRING, 
        user_broadcaster_type STRING, 
        user_description STRING, 
        user_email STRING, 
        user_created_at STRING
    )
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
    STORED AS TEXTFILE
    LOCATION '<hdfs>'
    TBLPROPERTIES ('skip.header.line.count'='1')
'''
spark.sql(createtable)

--#-------------------------------------------------------------------------------------------------------#--
--#---------------------------------------stream_language Dimension---------------------------------------#--
--#-------------------------------------------------------------------------------------------------------#--

CREATE TABLE IF NOT EXISTS dim_stream_language (
    SK_language INT,
    stream_language_kode STRING,
    stream_language STRING
);

USE dim_stream_language;

INSERT INTO dim_stream_language (SK_language, stream_language_kode, stream_language)
SELECT
    ROW_NUMBER() OVER (ORDER BY stream_language_kode) AS SK_language,
    stream_language_kode,
    CASE
        WHEN stream_language_kode = 'aa' THEN 'Afar'
        WHEN stream_language_kode = 'ab' THEN 'Abkhazian'
        WHEN stream_language_kode = 'ae' THEN 'Avestan'
        WHEN stream_language_kode = 'af' THEN 'Afrikaans'
        WHEN stream_language_kode = 'ak' THEN 'Akan'
        WHEN stream_language_kode = 'am' THEN 'Amharic'
        WHEN stream_language_kode = 'an' THEN 'Aragonese'
        WHEN stream_language_kode = 'ar' THEN 'Arabic'
        WHEN stream_language_kode = 'as' THEN 'Assamese'
        WHEN stream_language_kode = 'av' THEN 'Avaric'
        WHEN stream_language_kode = 'ay' THEN 'Aymara'
        WHEN stream_language_kode = 'az' THEN 'Azerbaijani'
        WHEN stream_language_kode = 'ba' THEN 'Bashkir'
        WHEN stream_language_kode = 'be' THEN 'Belarusian'
        WHEN stream_language_kode = 'bg' THEN 'Bulgarian'
        WHEN stream_language_kode = 'bh' THEN 'Bihari'
        WHEN stream_language_kode = 'bi' THEN 'Bislama'
        WHEN stream_language_kode = 'bm' THEN 'Bambara'
        WHEN stream_language_kode = 'bn' THEN 'Bengali'
        WHEN stream_language_kode = 'bo' THEN 'Tibetan'
        WHEN stream_language_kode = 'br' THEN 'Breton'
        WHEN stream_language_kode = 'bs' THEN 'Bosnian'
        WHEN stream_language_kode = 'ca' THEN 'Catalan'
        WHEN stream_language_kode = 'ce' THEN 'Chechen'
        WHEN stream_language_kode = 'ch' THEN 'Chamorro'
        WHEN stream_language_kode = 'co' THEN 'Corsican'
        WHEN stream_language_kode = 'cs' THEN 'Czech'
        WHEN stream_language_kode = 'cu' THEN 'Church Slavic'
        WHEN stream_language_kode = 'cv' THEN 'Chuvash'
        WHEN stream_language_kode = 'cy' THEN 'Welsh'
        WHEN stream_language_kode = 'da' THEN 'Danish'
        WHEN stream_language_kode = 'de' THEN 'German'
        WHEN stream_language_kode = 'dv' THEN 'Divehi'
        WHEN stream_language_kode = 'dz' THEN 'Dzongkha'
        WHEN stream_language_kode = 'el' THEN 'Greek'
        WHEN stream_language_kode = 'en' THEN 'English'
        WHEN stream_language_kode = 'eo' THEN 'Esperanto'
        WHEN stream_language_kode = 'es' THEN 'Spanish'
        WHEN stream_language_kode = 'et' THEN 'Estonian'
        WHEN stream_language_kode = 'eu' THEN 'Basque'
        WHEN stream_language_kode = 'fa' THEN 'Persian'
        WHEN stream_language_kode = 'ff' THEN 'Fulah'
        WHEN stream_language_kode = 'fi' THEN 'Finnish'
        WHEN stream_language_kode = 'fj' THEN 'Fijian'
        WHEN stream_language_kode = 'fo' THEN 'Faroese'
        WHEN stream_language_kode = 'fr' THEN 'French'
        WHEN stream_language_kode = 'fy' THEN 'Frisian'
        WHEN stream_language_kode = 'ga' THEN 'Irish'
        WHEN stream_language_kode = 'gd' THEN 'Scottish Gaelic'
        WHEN stream_language_kode = 'gl' THEN 'Galician'
        WHEN stream_language_kode = 'gn' THEN 'Guarani'
        WHEN stream_language_kode = 'gu' THEN 'Gujarati'
        WHEN stream_language_kode = 'gv' THEN 'Manx'
        WHEN stream_language_kode = 'ha' THEN 'Hausa'
        WHEN stream_language_kode = 'he' THEN 'Hebrew'
        WHEN stream_language_kode = 'hi' THEN 'Hindi'
        WHEN stream_language_kode = 'ho' THEN 'Hiri Motu'
        WHEN stream_language_kode = 'hr' THEN 'Croatian'
        WHEN stream_language_kode = 'ht' THEN 'Haitian Creole'
        WHEN stream_language_kode = 'hu' THEN 'Hungarian'
        WHEN stream_language_kode = 'hy' THEN 'Armenian'
        WHEN stream_language_kode = 'hz' THEN 'Herero'
        WHEN stream_language_kode = 'ia' THEN 'Interlingua'
        WHEN stream_language_kode = 'id' THEN 'Indonesian'
        WHEN stream_language_kode = 'ie' THEN 'Interlingue'
        WHEN stream_language_kode = 'ig' THEN 'Igbo'
        WHEN stream_language_kode = 'ii' THEN 'Sichuan Yi'
        WHEN stream_language_kode = 'ik' THEN 'Inupiaq'
        WHEN stream_language_kode = 'io' THEN 'Ido'
        WHEN stream_language_kode = 'is' THEN 'Icelandic'
        WHEN stream_language_kode = 'it' THEN 'Italian'
        WHEN stream_language_kode = 'iu' THEN 'Inuktitut'
        WHEN stream_language_kode = 'ja' THEN 'Japanese'
        WHEN stream_language_kode = 'jv' THEN 'Javanese'
        WHEN stream_language_kode = 'ka' THEN 'Georgian'
        WHEN stream_language_kode = 'kg' THEN 'Kongo'
        WHEN stream_language_kode = 'ki' THEN 'Kikuyu'
        WHEN stream_language_kode = 'kj' THEN 'Kuanyama'
        WHEN stream_language_kode = 'kk' THEN 'Kazakh'
        WHEN stream_language_kode = 'kl' THEN 'Kalaallisut'
        WHEN stream_language_kode = 'km' THEN 'Khmer'
        WHEN stream_language_kode = 'kn' THEN 'Kannada'
        WHEN stream_language_kode = 'ko' THEN 'Korean'
        WHEN stream_language_kode = 'kr' THEN 'Kanuri'
        WHEN stream_language_kode = 'ks' THEN 'Kashmiri'
        WHEN stream_language_kode = 'ku' THEN 'Kurdish'
        WHEN stream_language_kode = 'kv' THEN 'Komi'
        WHEN stream_language_kode = 'kw' THEN 'Cornish'
        WHEN stream_language_kode = 'ky' THEN 'Kyrgyz'
        WHEN stream_language_kode = 'la' THEN 'Latin'
        WHEN stream_language_kode = 'lb' THEN 'Luxembourgish'
        WHEN stream_language_kode = 'lg' THEN 'Ganda'
        WHEN stream_language_kode = 'li' THEN 'Limburgish'
        WHEN stream_language_kode = 'ln' THEN 'Lingala'
        WHEN stream_language_kode = 'lo' THEN 'Lao'
        WHEN stream_language_kode = 'lt' THEN 'Lithuanian'
        WHEN stream_language_kode = 'lu' THEN 'Luba-Katanga'
        WHEN stream_language_kode = 'lv' THEN 'Latvian'
        WHEN stream_language_kode = 'mg' THEN 'Malagasy'
        WHEN stream_language_kode = 'mh' THEN 'Marshallese'
        WHEN stream_language_kode = 'mi' THEN 'Maori'
        WHEN stream_language_kode = 'mk' THEN 'Macedonian'
        WHEN stream_language_kode = 'ml' THEN 'Malayalam'
        WHEN stream_language_kode = 'mn' THEN 'Mongolian'
        WHEN stream_language_kode = 'mo' THEN 'Moldavian'
        WHEN stream_language_kode = 'ms' THEN 'Malay'
        WHEN stream_language_kode = 'mt' THEN 'Maltese'
        WHEN stream_language_kode = 'my' THEN 'Burmese'
        WHEN stream_language_kode = 'na' THEN 'Nauru'
        WHEN stream_language_kode = 'nb' THEN 'Norwegian Bokmål'
        WHEN stream_language_kode = 'nd' THEN 'North Ndebele'
        WHEN stream_language_kode = 'ne' THEN 'Nepali'
        WHEN stream_language_kode = 'ng' THEN 'Ndonga'
        WHEN stream_language_kode = 'nl' THEN 'Dutch'
        WHEN stream_language_kode = 'nn' THEN 'Norwegian Nynorsk'
        WHEN stream_language_kode = 'no' THEN 'Norwegian'
        WHEN stream_language_kode = 'nr' THEN 'South Ndebele'
        WHEN stream_language_kode = 'nv' THEN 'Navajo'
        WHEN stream_language_kode = 'ny' THEN 'Chichewa'
        WHEN stream_language_kode = 'oc' THEN 'Occitan'
        WHEN stream_language_kode = 'oj' THEN 'Ojibwe'
        WHEN stream_language_kode = 'om' THEN 'Oromo'
        WHEN stream_language_kode = 'or' THEN 'Oriya'
        WHEN stream_language_kode = 'os' THEN 'Ossetian'
        WHEN stream_language_kode = 'pa' THEN 'Punjabi'
        WHEN stream_language_kode = 'pi' THEN 'Pali'
        WHEN stream_language_kode = 'pl' THEN 'Polish'
        WHEN stream_language_kode = 'ps' THEN 'Pashto'
        WHEN stream_language_kode = 'pt' THEN 'Portuguese'
        WHEN stream_language_kode = 'qu' THEN 'Quechua'
        WHEN stream_language_kode = 'rm' THEN 'Romansh'
        WHEN stream_language_kode = 'rn' THEN 'Rundi'
        WHEN stream_language_kode = 'ro' THEN 'Romanian'
        WHEN stream_language_kode = 'ru' THEN 'Russian'
        WHEN stream_language_kode = 'rw' THEN 'Kinyarwanda'
        WHEN stream_language_kode = 'sa' THEN 'Sanskrit'
        WHEN stream_language_kode = 'sc' THEN 'Sardinian'
        WHEN stream_language_kode = 'sd' THEN 'Sindhi'
        WHEN stream_language_kode = 'se' THEN 'Northern Sami'
        WHEN stream_language_kode = 'sg' THEN 'Sango'
        WHEN stream_language_kode = 'si' THEN 'Sinhala'
        WHEN stream_language_kode = 'sk' THEN 'Slovak'
        WHEN stream_language_kode = 'sl' THEN 'Slovenian'
        WHEN stream_language_kode = 'sm' THEN 'Samoan'
        WHEN stream_language_kode = 'sn' THEN 'Shona'
        WHEN stream_language_kode = 'so' THEN 'Somali'
        WHEN stream_language_kode = 'sq' THEN 'Albanian'
        WHEN stream_language_kode = 'sr' THEN 'Serbian'
        WHEN stream_language_kode = 'ss' THEN 'Swati'
        WHEN stream_language_kode = 'st' THEN 'Southern Sotho'
        WHEN stream_language_kode = 'su' THEN 'Sundanese'
        WHEN stream_language_kode = 'sv' THEN 'Swedish'
        WHEN stream_language_kode = 'sw' THEN 'Swahili'
        WHEN stream_language_kode = 'ta' THEN 'Tamil'
        WHEN stream_language_kode = 'te' THEN 'Telugu'
        WHEN stream_language_kode = 'tg' THEN 'Tajik'
        WHEN stream_language_kode = 'th' THEN 'Thai'
        WHEN stream_language_kode = 'ti' THEN 'Tigrinya'
        WHEN stream_language_kode = 'tk' THEN 'Turkmen'
        WHEN stream_language_kode = 'tl' THEN 'Tagalog'
        WHEN stream_language_kode = 'tn' THEN 'Tswana'
        WHEN stream_language_kode = 'to' THEN 'Tongan'
        WHEN stream_language_kode = 'tr' THEN 'Turkish'
        WHEN stream_language_kode = 'ts' THEN 'Tsonga'
        WHEN stream_language_kode = 'tt' THEN 'Tatar'
        WHEN stream_language_kode = 'tw' THEN 'Twi'
        WHEN stream_language_kode = 'ty' THEN 'Tahitian'
        WHEN stream_language_kode = 'ug' THEN 'Uighur'
        WHEN stream_language_kode = 'uk' THEN 'Ukrainian'
        WHEN stream_language_kode = 'ur' THEN 'Urdu'
        WHEN stream_language_kode = 'uz' THEN 'Uzbek'
        WHEN stream_language_kode = 've' THEN 'Venda'
        WHEN stream_language_kode = 'vi' THEN 'Vietnamese'
        WHEN stream_language_kode = 'vo' THEN 'Volapük'
        WHEN stream_language_kode = 'wa' THEN 'Walloon'
        WHEN stream_language_kode = 'wo' THEN 'Wolof'
        WHEN stream_language_kode = 'xh' THEN 'Xhosa'
        WHEN stream_language_kode = 'yi' THEN 'Yiddish'
        WHEN stream_language_kode = 'yo' THEN 'Yoruba'
        WHEN stream_language_kode = 'za' THEN 'Zhuang'
        WHEN stream_language_kode = 'zh' THEN 'Chinese'
        WHEN stream_language_kode = 'zu' THEN 'Zulu'
        ELSE 'New Country'
    END AS stream_language
FROM
    <nama_table_stream>;

--#------------------------------------------------------------------------------------------------------#--
--#--------------------------------------------Time Dimension--------------------------------------------#--
--#------------------------------------------------------------------------------------------------------#--
--File SQL--
CREATE TABLE time_dimension (
    surrogate_key STRING,
    time TIMESTAMP,
    year INT,
    month INT,
    day INT
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t'
STORED AS TEXTFILE;

--File python--
from datetime import datetime, timedelta

start_date = datetime(2023, 1, 1)
end_date = datetime(2023, 12, 31)

data = []

current_date = start_date
while current_date <= end_date:
    surrogate_key = current_date.strftime("%Y%m%d")
    time = current_date.strftime("%Y-%m-%d")
    year = current_date.year
    month = current_date.month
    day = current_date.day

    data.append((surrogate_key, time, year, month, day))
    current_date += timedelta(days=1)

output_file = "time_dimension.csv"  # Nama file output
delimiter = ","  

with open(output_file, "w") as f:
    for row in data:
        line = delimiter.join(map(str, row)) + "\n"
        f.write(line)

hive_query = f"LOAD DATA INPATH '{output_file}' INTO TABLE time_dimension"

--#------------------------------------------------------------------------------------------------------#--
--#------------------------------------------------------------------------------------------------------#--
--#------------------------------------------------------------------------------------------------------#--


