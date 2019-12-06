import os
import re

from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, monotonically_increasing_id, trim, col, \
    regexp_replace, levenshtein, lower, explode, length, collect_list, array_union, array_intersect, size
from pyspark.ml.feature import Tokenizer, StopWordsRemover, RegexTokenizer, NGram
from pyspark.sql.types import StringType, StructType


def main():
    db_list = ['5694-9szk.Business_Website_or_Other_URL.txt.gz', 'uwyv-629c.StreetName.txt.gz', 'faiq-9dfq.Vehicle_Color.txt.gz',
     'qcdj-rwhu.BUSINESS_NAME2.txt.gz', '6ypq-ih9a.CORE_SUBJECT___MS_CORE_and__09_12_ONLY_.txt.gz',
     'pvqr-7yc4.Vehicle_Color.txt.gz', 'en2c-j6tw.BRONX_CONDOMINIUM_PROPERTY_Building_Classification.txt.gz',
     'uq7m-95z8.interest6.txt.gz', '5ziv-wcy4.WEBSITE.txt.gz', 'ydkf-mpxb.CrossStreetName.txt.gz',
     'w9ak-ipjd.Applicant_Last_Name.txt.gz', 'jz4z-kudi.Respondent_Address__City_.txt.gz',
     'rbx6-tga4.Owner_Street_Address.txt.gz', 'sqmu-2ixd.Agency_Name.txt.gz', 'aiww-p3af.Incident_Zip.txt.gz',
     'mmvm-mvi3.Org_Name.txt.gz', 'h9gi-nx95.VEHICLE_TYPE_CODE_5.txt.gz', 'uh2w-zjsn.Borough.txt.gz',
     'tqtj-sjs8.FromStreetName.txt.gz', 'mqdy-gu73.Color.txt.gz', '7jkp-5w5g.Agency.txt.gz',
     's3zn-tf7c.QUEENS_CONDOMINIUM_PROPERTY_Building_Classification.txt.gz', 'sqcr-6mww.School_Name.txt.gz',
     'vrn4-2abs.SCHOOL_LEVEL_.txt.gz', '2sps-j9st.PERSON_LAST_NAME.txt.gz', '2bmr-jdsv.DBA.txt.gz',
     '4d7f-74pe.Address.txt.gz', 'ji82-xba5.address.txt.gz', 'hy4q-igkk.School_Name.txt.gz', 's9d3-x4fz.EMPCITY.txt.gz',
     '5uac-w243.PREM_TYP_DESC.txt.gz', '64gx-bycn.EMPCITY.txt.gz', 'e9xc-u3ds.CANDMI.txt.gz',
     'h9gi-nx95.VEHICLE_TYPE_CODE_3.txt.gz', 'p937-wjvj.HOUSE_NUMBER.txt.gz', 'dj4e-3xrn.SCHOOL_LEVEL_.txt.gz',
     'qu8g-sxqf.MI.txt.gz', 'mdcw-n682.Middle_Initial.txt.gz', 'pq5i-thsu.DVC_MAKE.txt.gz',
     'ub9e-s7ai.CORE_SUBJECT___MS_CORE_and__09_12_ONLY_.txt.gz', '52dp-yji6.Owner_First_Name.txt.gz',
     'jz4z-kudi.Respondent_Address__Zip_Code_.txt.gz', 'vx8i-nprf.MI.txt.gz', 'k3cd-yu9d.Location_1.txt.gz',
     'p6h4-mpyy.PRINCIPAL_PHONE_NUMBER.txt.gz', 'sybh-s59s.CORE_SUBJECT___MS_CORE_and__9_12_ONLY_.txt.gz',
     'kz72-dump.CORE_SUBJECT___MS_CORE_and__09_12_ONLY_.txt.gz', '7btz-mnc8.Provider_Last_Name.txt.gz',
     'ph7v-u5f3.TOP_VEHICLE_MODELS___5.txt.gz', 'mjux-q9d4.SCHOOL_LEVEL_.txt.gz', 'hjvj-jfc9.Borough.txt.gz',
     'h9gi-nx95.VEHICLE_TYPE_CODE_2.txt.gz', 'easq-ubfe.CITY.txt.gz', 'sv2w-rv3k.BORO.txt.gz',
     'qu8g-sxqf.First_Name.txt.gz', 'ipu4-2q9a.Site_Safety_Mgr_s_First_Name.txt.gz',
     'ipu4-2q9a.Site_Safety_Mgr_s_Last_Name.txt.gz', 'pgtq-ht5f.CORE_SUBJECT___MS_CORE_and__9_12_ONLY_.txt.gz',
     '52dp-yji6.Owner_Last_Name.txt.gz', 's3k6-pzi2.interest4.txt.gz', '4y63-yw9e.SCHOOL_NAME.txt.gz',
     'gez6-674h.CORE_SUBJECT___MS_CORE_and__9_12_ONLY_.txt.gz', 'a9md-ynri.MI.txt.gz',
     'u553-m549.Independent_Website.txt.gz', 'uzcy-9puk.Street_Name.txt.gz', 'dg92-zbpx.VendorAddress.txt.gz',
     'jcih-dj9q.QUEENS_____CONDOMINIUMS_COMPARABLE_PROPERTIES_____Neighborhood.txt.gz', '735p-zed8.CANDMI.txt.gz',
     'vg63-xw6u.CITY.txt.gz', 'aiww-p3af.Cross_Street_1.txt.gz', 'sa5w-dn2t.Agency.txt.gz', 'cspg-yi7g.ADDRESS.txt.gz',
     'crbs-vur7.QUEENS_CONDOMINIUM_PROPERTY_Neighborhood.txt.gz', 'erm2-nwe9.City.txt.gz',
     'nyis-y4yr.Owner_s__Phone__.txt.gz', 'tukx-dsca.Address_1.txt.gz', '9b9u-8989.DBA.txt.gz',
     'e4p3-6ecr.Agency_Name.txt.gz', '5mw2-hzqx.BROOKLYN_CONDOMINIUM_PROPERTY_Neighborhood.txt.gz',
     'kiv2-tbus.Vehicle_Make.txt.gz', 'w6yt-hctp.COMPARABLE_RENTAL_1__Building_Classification.txt.gz',
     'k3cd-yu9d.CANDMI.txt.gz', 'ii2w-6fne.Borough.txt.gz', 'w7w3-xahh.Location.txt.gz',
     'erm2-nwe9.Park_Facility_Name.txt.gz', '5nz7-hh6t.CORE_SUBJECT___MS_CORE_and__09_12_ONLY_.txt.gz',
     '8wbx-tsch.Website.txt.gz', 'xne4-4v8f.SCHOOL_LEVEL_.txt.gz', 'vw9i-7mzq.neighborhood.txt.gz',
     'yayv-apxh.SCHOOL_LEVEL_.txt.gz', 'aiww-p3af.Park_Facility_Name.txt.gz',
     'jz4z-kudi.Violation_Location__City_.txt.gz', 'kiv2-tbus.Vehicle_Body_Type.txt.gz',
     'fzv4-jan3.SCHOOL_LEVEL_.txt.gz', 'w7w3-xahh.Address_ZIP.txt.gz', 'i9pf-sj7c.INTEREST.txt.gz',
     'ci93-uc8s.ZIP.txt.gz', 'jtus-srrj.School_Name.txt.gz', 'a5td-mswe.Vehicle_Color.txt.gz',
     '29bw-z7pj.Location_1.txt.gz', 'vw9i-7mzq.interest4.txt.gz', 'pvqr-7yc4.Vehicle_Make.txt.gz',
     '3rfa-3xsf.Incident_Zip.txt.gz', 'faiq-9dfq.Vehicle_Body_Type.txt.gz', 'pvqr-7yc4.Vehicle_Body_Type.txt.gz',
     'kj4p-ruqc.StreetName.txt.gz', '4pt5-3vv4.Location.txt.gz', 'c284-tqph.Vehicle_Make.txt.gz',
     'pqg4-dm6b.Address1.txt.gz', 'cqc8-am9x.Borough.txt.gz', '6rrm-vxj9.parkname.txt.gz',
     'tg4x-b46p.ZipCode_s_.txt.gz', 'jzt2-2f7h.School_Name.txt.gz', 'ci93-uc8s.Website.txt.gz',
     'm59i-mqex.QUEENS_CONDOMINIUM_PROPERTY_Neighborhood.txt.gz', 'uzcy-9puk.School_Phone_Number.txt.gz',
     'ci93-uc8s.Vendor_DBA.txt.gz', 'cyfw-hfqk.STATEN_ISLAND_CONDOMINIUM_PROPERTY_Neighborhood.txt.gz',
     '3rfa-3xsf.Intersection_Street_2.txt.gz', 'uqxv-h2se.neighborhood.txt.gz', 'w9ak-ipjd.Owner_s_Street_Name.txt.gz',
     'h9gi-nx95.VEHICLE_TYPE_CODE_1.txt.gz', '4twk-9yq2.CrossStreet2.txt.gz', 'fbaw-uq4e.CITY.txt.gz',
     'mdcw-n682.First_Name.txt.gz', 'w7w3-xahh.Address_City.txt.gz', 'i4ni-6qin.PRINCIPAL_PHONE_NUMBER.txt.gz',
     'imfa-v5pv.School_Name.txt.gz', 'sxx4-xhzg.Park_Site_Name.txt.gz', 'vw9i-7mzq.interest1.txt.gz',
     'sqcr-6mww.Cross_Street_1.txt.gz', '6anw-twe4.FirstName.txt.gz', '2bnn-yakx.Vehicle_Body_Type.txt.gz',
     'uzcy-9puk.Park_Facility_Name.txt.gz', 'pvqr-7yc4.Vehicle_Make.txt.gz', 'c284-tqph.Vehicle_Color.txt.gz',
     'm56g-jpua.COMPARABLE_RENTAL___1___Building_Classification.txt.gz', 'tsak-vtv3.Upcoming_Project_Name.txt.gz',
     'tg3t-nh4h.BusinessName.txt.gz', 'cgz5-877h.SCHOOL_LEVEL_.txt.gz',
     'jz4z-kudi.Violation_Location__Zip_Code_.txt.gz', 'us4j-b5zt.Agency.txt.gz', 'vr8p-8shw.DVT_MAKE.txt.gz',
     '3qfc-4tta.BRONX_____CONDOMINIUMS_COMPARABLE_PROPERTIES_____Neighborhood.txt.gz',
     'bawj-6bgn.BRONX_CONDOMINIUM_PROPERTY_Neighborhood.txt.gz', 'ci93-uc8s.fax.txt.gz',
     'ffnc-f3aa.SCHOOL_LEVEL_.txt.gz', 'h9gi-nx95.VEHICLE_TYPE_CODE_4.txt.gz', 'rbx6-tga4.Owner_Street_Address.txt.gz',
     's3k6-pzi2.interest5.txt.gz', '2sps-j9st.PERSON_FIRST_NAME.txt.gz', 'ji82-xba5.street.txt.gz',
     'f7qh-bcr5.CORE_SUBJECT___MS_CORE_and__9_12_ONLY_.txt.gz', '3rfa-3xsf.Street_Name.txt.gz',
     'n84m-kx4j.VEHICLE_MAKE.txt.gz', 'hy4q-igkk.Location.txt.gz', 'sxmw-f24h.Cross_Street_2.txt.gz',
     'yahh-6yjc.School_Type.txt.gz', '72ss-25qh.Agency_ID.txt.gz', 'faiq-9dfq.Vehicle_Body_Type.txt.gz',
     'm56g-jpua.MANHATTAN___COOPERATIVES_COMPARABLE_PROPERTIES___Building_Classification.txt.gz',
     '3rfa-3xsf.School_Name.txt.gz', 'ic3t-wcy2.Applicant_s_First_Name.txt.gz', 'vw9i-7mzq.interest3.txt.gz',
     'i6b5-j7bu.TOSTREETNAME.txt.gz', 'i5ef-jxv3.Agency.txt.gz', '7crd-d9xh.website.txt.gz',
     'mdcw-n682.Last_Name.txt.gz', 'ge8j-uqbf.interest.txt.gz', 'q2ni-ztsb.Street_Address_1.txt.gz',
     '8k4x-9mp5.Last_Name__only_2014_15_.txt.gz', 'wks3-66bn.School_Name.txt.gz', '43nn-pn8j.DBA.txt.gz',
     'qgea-i56i.PREM_TYP_DESC.txt.gz', 'bdjm-n7q4.CrossStreet2.txt.gz', 'nhms-9u6g.Name__Last__First_.txt.gz',
     'bdjm-n7q4.Location.txt.gz', 'x3kb-2vbv.School_Name.txt.gz', 'uzcy-9puk.Location.txt.gz',
     '6anw-twe4.LastName.txt.gz',
     'tyfh-9h2y.BROOKLYN___COOPERATIVES_COMPARABLE_PROPERTIES___Building_Classification.txt.gz',
     '3rfa-3xsf.Cross_Street_2.txt.gz', 'bty7-2jhb.Site_Safety_Mgr_s_Last_Name.txt.gz',
     '9jgj-bmct.Incident_Address_Street_Name.txt.gz', 'pdpg-nn8i.BORO.txt.gz', 'w9ak-ipjd.Owner_s_Business_Name.txt.gz',
     'rb2h-bgai.Website.txt.gz', 'jt7v-77mi.Vehicle_Make.txt.gz', 'as69-ew8f.TruckMake.txt.gz',
     'mrxb-9w9v.BOROUGH___COMMUNITY.txt.gz', 'pvqr-7yc4.Vehicle_Body_Type.txt.gz',
     'dm9a-ab7w.AUTH_REP_LAST_NAME.txt.gz', '9z9b-6hvk.Borough.txt.gz',
     'wv4q-e75v.STATEN_ISLAND_CONDOMINIUM_PROPERTY_Neighborhood.txt.gz', 'kwmq-dbub.CANDMI.txt.gz',
     'dvzp-h4k9.COMPARABLE_RENTAL_____1_____Building_Classification.txt.gz', '6ypq-ih9a.BOROUGH.txt.gz',
     'p2d7-vcsb.ACCOUNT_CITY.txt.gz', '2v9c-2k7f.DBA.txt.gz', 'erm2-nwe9.Landmark.txt.gz',
     'dm9a-ab7w.APPLICANT_FIRST_NAME.txt.gz', '72ss-25qh.Borough.txt.gz', 'qpm9-j523.org_neighborhood.txt.gz',
     '6wcu-cfa3.CORE_COURSE__MS_CORE_and_9_12_ONLY_.txt.gz', 'nfkx-wd79.Address_1.txt.gz', 'jzdn-258f.Agency.txt.gz',
     'kiv2-tbus.Vehicle_Color.txt.gz', 'w9ak-ipjd.Filing_Representative_First_Name.txt.gz',
     'irhv-jqz7.BROOKLYN___COOPERATIVES_COMPARABLE_PROPERTIES___Building_Classification.txt.gz',
     'm3fi-rt3k.Street_Address_1_.txt.gz', 'ipu4-2q9a.Owner_s_House_City.txt.gz', 'qpm9-j523.org_website.txt.gz',
     'qgea-i56i.Lat_Lon.txt.gz', 'jvce-szsb.Website.txt.gz', 'd3ge-anaz.CORE_COURSE__MS_CORE_and_9_12_ONLY_.txt.gz',
     'kiyv-ks3f.phone.txt.gz', 'qe6k-pu9t.Agency.txt.gz', '5e7x-8jy6.School_Name.txt.gz', 'xne4-4v8f.SCHOOL.txt.gz',
     '7btz-mnc8.Provider_First_Name.txt.gz', 'uq7m-95z8.interest1.txt.gz', 'n5mv-nfpy.Location1.txt.gz',
     '8i43-kna8.CORE_SUBJECT.txt.gz', 'eccv-9dzr.Telephone_Number.txt.gz', '4n2j-ut8i.SCHOOL_LEVEL_.txt.gz',
     'dm9a-ab7w.STREET_NAME.txt.gz', '2bnn-yakx.Vehicle_Make.txt.gz', '2bnn-yakx.Vehicle_Color.txt.gz',
     '2bnn-yakx.Vehicle_Body_Type.txt.gz', 'jt7v-77mi.Vehicle_Color.txt.gz', 'bty7-2jhb.Owner_s_House_Zip_Code.txt.gz',
     'cvh6-nmyi.SCHOOL_LEVEL_.txt.gz', '7yds-6i8e.CORE_SUBJECT__MS_CORE_and_9_12_ONLY_.txt.gz',
     'ajxm-kzmj.NeighborhoodName.txt.gz', '3aka-ggej.CORE_SUBJECT___MS_CORE_and__09_12_ONLY_.txt.gz',
     '6bgk-3dad.RESPONDENT_ZIP.txt.gz', 'fbaw-uq4e.Location_1.txt.gz',
     'jxyc-rxiv.MANHATTAN___COOPERATIVES_COMPARABLE_PROPERTIES___Building_Classification.txt.gz',
     'n2s5-fumm.BRONX_CONDOMINIUM_PROPERTY_Building_Classification.txt.gz', 'bjuu-44hx.DVV_MAKE.txt.gz',
     'uzcy-9puk.Street_Name.txt.gz', 's3k6-pzi2.interest1.txt.gz', 'wg9x-4ke6.Principal_phone_number.txt.gz',
     'vhah-kvpj.Borough.txt.gz', 'dm9a-ab7w.AUTH_REP_FIRST_NAME.txt.gz', '3rfa-3xsf.Street_Name.txt.gz',
     'urzf-q2g5.Phone_Number.txt.gz', 'him9-7gri.Agency.txt.gz', '3rfa-3xsf.Cross_Street_2.txt.gz',
     'mu46-p9is.CallerZipCode.txt.gz', 'a5qt-5jpu.STATEN_ISLAND_CONDOMINIUM_PROPERTY_Neighborhood.txt.gz',
     'ytjm-yias.CORE_SUBJECT___MS_CORE_and__09_12_ONLY_.txt.gz', 'sxmw-f24h.Park_Facility_Name.txt.gz',
     'vuae-w6cg.Agency.txt.gz', 'qusa-igsv.BORO.txt.gz', '5tdj-xqd5.Borough.txt.gz', '2bnn-yakx.Vehicle_Make.txt.gz',
     't8hj-ruu2.Business_Phone_Number.txt.gz', 'ajgi-hpq9.CORE_SUBJECT___MS_CORE_and__09_12_ONLY_.txt.gz',
     'jhjm-vsp8.Agency.txt.gz', '4nft-bihw.Property_Address.txt.gz', '6je4-4x7e.SCHOOL_LEVEL_.txt.gz',
     'c284-tqph.Vehicle_Make.txt.gz', 'dpm2-m9mq.owner_zip.txt.gz', 'gk83-aa6y.SCHOOL_NAME.txt.gz',
     't8hj-ruu2.First_Name.txt.gz', 'as69-ew8f.StartCity.txt.gz', 'i8ys-e4pm.CORE_COURSE_9_12_ONLY_.txt.gz',
     'myei-c3fa.Neighborhood_1.txt.gz', 'upwt-zvh3.SCHOOL_LEVEL_.txt.gz', 'aiww-p3af.School_Phone_Number.txt.gz',
     'kiv2-tbus.Vehicle_Make.txt.gz', 'weg5-33pj.SCHOOL_LEVEL_.txt.gz',
     'rmv8-86p4.BROOKLYN_CONDOMINIUM_PROPERTY_Neighborhood.txt.gz']
    #strategy pattern usually you would have a class then extend and apply, but this will do.
    functions = [count_car_make, count_parks, count_website, count_business, count_building_code]
    tokenizer = Tokenizer(inputCol="_c0", outputCol="token_raw")
    remover = StopWordsRemover(inputCol="token_raw", outputCol="token_filtered")
    regex_tokenizer = RegexTokenizer(inputCol="_c0", outputCol="letters", pattern="")
    ngram = NGram(n=3, inputCol="letters", outputCol="ngrams")
    pipeline = [tokenizer, remover, regex_tokenizer, ngram]
    for file in db_list[0:3]:
        processed_path = os.path.join(os.sep, "user", "hm74", "NYCColumns", file)
        df_to_process = spark.read.option("delimiter", "\t").csv(processed_path)
        df_to_process = df_to_process.withColumn("id", monotonically_increasing_id())
        for stage in pipeline:
            df_to_process = stage.transform(df_to_process)
        for function in functions:
            print("processing file {} function {}".format(file, function))
            sem_name, df_to_process, count = function(df_to_process)
            print("sem_name {}, count {}, count left {}".format(sem_name, count, df_to_process.count()))


def website_regex(val):
    # https://stackoverflow.com/questions/7160737/python-how-to-validate-a-url-in-python-malformed-or-not
    regex = re.compile(
        r'^(?:http|ftp)s?://'  # http:// or https://
        r'(?:(?:[A-Z0-9](?:[A-Z0-9-]{0,61}[A-Z0-9])?\.)+(?:[A-Z]{2,6}\.?|[A-Z0-9-]{2,}\.?)|'  # domain...
        r'localhost|'  # localhost...
        r'\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3})'  # ...or ip
        r'(?::\d+)?'  # optional port
        r'(?:/?|[/?]\S+)$', re.IGNORECASE)
    return re.match(regex, val) is not None


def count_website(df_to_process):
    udf_website_regex = udf(website_regex)
    df_to_process2 = df_to_process.withColumn("_c0_trim", regexp_replace(col("_c0"), "\\s+", ""))
    df_processed = df_to_process2.filter(udf_website_regex(df_to_process2._c0_trim) == True)
    df_left = df_to_process2.join(df_processed, ["id", "id"], "leftanti")
    df_left = df_left.drop("_c0_trim")
    return 'Websites', df_left, df_processed.count()


def building_code_regex(val):
    regex = re.compile(r'^[a-zA-Z][0-9].*')
    return re.match(regex, val) is not None


def count_building_code(df_to_process):
    udf_building_code_regex = udf(building_code_regex)
    df_to_process2 = df_to_process.withColumn("_c0_trim", regexp_replace(col("_c0"), "\\s+", ""))
    df_processed = df_to_process2.filter(udf_building_code_regex(df_to_process2._c0_trim) == True)
    df_left = df_to_process2.join(df_processed, ["id", "id"], "leftanti")
    df_left = df_left.drop("_c0_trim")
    return 'Websites', df_left, df_processed.count()


def count_car_make(df_to_process):
    #obtained csv file from
    #https://github.com/arthurkao/vehicle-make-model-data
    df_car_make = spark.read.option("header", "true").option("delimiter", ",").csv('/user/gl758/hd/car_make.csv')
    df_processed = df_to_process.join(df_car_make, levenshtein(lower(df_to_process._c0), lower(df_car_make.make)) < 2)
    df_left = df_to_process.join(df_processed, ["id", "id"], "leftanti")
    return 'Car make', df_left, df_processed.count()


def count_parks(df_to_process):
    df_cross_join = df_to_process.crossJoin(df_pre_park)
    df_score = get_tokens_match_over_diff(df_cross_join)
    df_processed = df_score.filter(df_score.score > .3)
    df_left = df_to_process.join(df_processed, ["id", "id"], "leftanti")
    return 'Parks/Playgrounds', df_left, df_processed.count()


def count_business(df_to_process):
    df_cross_join = df_to_process.crossJoin(df_pre_business)
    df_score = get_tokens_match_over_diff(df_cross_join)
    df_processed = df_score.filter(df_score.score > .3)
    df_left = df_to_process.join(df_processed, ["id", "id"], "leftanti")
    return 'Business Name', df_left, df_processed.count()


def pre_compute_park():
    list_park_names = ["zone", "park", "playground", "plgd", "beach", "east", "rockaway", "parkway", "south", "river",
                       "pond", "malls", "fort", "island", "meadow", "hill", "west", "lake", "coney", "crotona", "north",
                       "kissena", "alley", "point", "ocean", "field", "highbridge", "area", "lot", "landscape",
                       "courts", "bronx", "tot", "grove", "one", "fields", "washington", "broadway", "st.", "inwood",
                       "highland", "waterfront", "road", "dr.", "shore", "eastern", "cunningham", "hook", "riverside",
                       "red", "canarsie", "mall", "woods", "baisley", "center", "prospect", "marine", "soundview",
                       "morningside", "square", "esplanade", "battery", "cedar", "claremont", "upper", "tryon",
                       "corridor", "juniper", "lower", "house", "pedestrian", "lawn", "mary's", "mccarren", "tennis",
                       "pelham", "valley", "van", "greene", "walk", "memorial", "clove", "central", "lincoln",
                       "promenade", "brookville", "lakes", "bay", "nicholas", "orchard", "trail", "cortlandt",
                       "mosholu", "parade", "pool", "silver", "heron", "marcus", "conference", "high"]
    df_park_names_array = spark.createDataFrame(list_park_names, StringType()).select(collect_list("value")).withColumnRenamed(
        "collect_list(value)", "to_match")
    # df_parks = spark.read.option("delimiter", ",").csv('/user/gl758/hd/park_names.csv')
    # df_park_names = df_parks.select('_c4').distinct()
    # df_park_names = df_park_names.withColumnRenamed("_c4", "_c0")
    # df_park_names_array = get_top_n_in_array(df_park_names, 200)
    return df_park_names_array


def pre_compute_business():
    list_business_names = ["inc.", "inc", "corp.", "llc", "corp", "deli", "construction", "grocery", "auto", "new",
                           "food", "contracting", "wireless", "laundromat", "home", "michael", "john", "market",
                           "corporation", "cleaners", "joseph", "group", "parking", "robert", "construction,",
                           "services", "gourmet", "general", "david", "anthony", "shop", "james", "jose", "ltd.",
                           "service", "street", "improvement", "store", "repair", "grocery,", "richard", "laundry",
                           "william", "design", "avenue", "convenience", "jewelry", "mini", "thomas", "center",
                           "daniel", "management", "services,", "york", "star", "express", "ave", "christopher", "park",
                           "cleaners,", "east", "singh,", "restaurant", "dry", "laundromat,", "city", "best", "george",
                           "builders", "frank", "peter", "luis", "nyc", "contracting,", "towing", "gold", "garage",
                           "candy", "group,", "steven", "paul", "enterprises", "juan", "one", "restoration", "jr,",
                           "mobile", "deli,", "mark", "incorporated", "electronics", "grill", "west", "usa", "stop",
                           "meat", "edward", "medical", "carlos", "charles", "mohammed", "mart", "st.", "co.,", "tire",
                           "kevin", "green", "rodriguez,", "renovation", "development", "super", "car", "company",
                           "nicholas", "solutions", "pharmacy", "andrew", "news", "market,", "recovery", "remodeling",
                           "broadway", "sales", "family", "contractors", "collision", "american", "painting", "fruit",
                           "mohammad", "cleaner", "brian", "supply", "l.l.c.", "supermarket", "king", "trading",
                           "smoke", "improvements", "international", "discount", "renovations", "vincent", "lee,",
                           "cafe", "matthew", "enterprises,", "patrick", "island", "tech", "brothers", "kim,",
                           "brooklyn", "stephen", "victor", "ronald", "body", "mohamed", "eric", "lucky", "jason",
                           "kenneth", "ali", "jonathan", "plus", "williams,", "alexander", "world", "associates,",
                           "ltd", "building", "clean", "united", "interiors", "jeffrey", "fresh", "ave.", "automotive",
                           "first", "metro", "ny,", "associates", "gonzalez,", "farm", "wash", "maria", "sons",
                           "smith,", "maintenance", "care", "big", "furniture", "angel", "quality", "computer", "chen,",
                           "louis", "enterprise", "lopez,", "custom"]
    df_park_names_array = spark.createDataFrame(list_business_names, StringType()).select(
        collect_list("value")).withColumnRenamed(
        "collect_list(value)", "to_match")
    # df_business = spark.read.option("header", "true").option("delimiter", "\t").csv('/user/hm74/NYCOpenData/w7w3-xahh.tsv.gz')
    # df_business_names = df_business.select('Business Name').distinct().where(col("Business Name").isNotNull())
    # df_business_names = df_business_names.withColumnRenamed('Business Name', "_c0")
    # df_business_names_array = get_top_n_in_array(df_business_names, 200)
    return df_park_names_array


def get_tokens_match_over_diff(df_to_process):
    df_processed = df_to_process.withColumn("score", size(array_intersect("token_filtered", "to_match"))/size("token_filtered"))
    return df_processed


def get_top_n_in_array(df_lookup, top):
    df_lookup = df_lookup.select('_c0').distinct()
    tokenizer = Tokenizer(inputCol="_c0", outputCol="token_raw")
    remover = StopWordsRemover(inputCol="token_raw", outputCol="token_filtered")
    df_lookup = tokenizer.transform(df_lookup)
    df_lookup = remover.transform(df_lookup)
    df_lookup = df_lookup.select((explode("token_filtered"))).groupby("col").count().sort('count', ascending=False)
    df_lookup = df_lookup.filter(length("col") > 2).limit(top).select(
        collect_list("col")).withColumnRenamed("collect_list(col)", "to_match")
    return df_lookup

# def calc_jaccard_sim():


if __name__ == "__main__":
    spark = SparkSession \
        .builder \
        .appName("big_data_proj_part2") \
        .config("spark.some.config.option", "some-value") \
        .getOrCreate()
    df_pre_park = pre_compute_park()
    df_pre_park = df_pre_park.cache()
    df_pre_business = pre_compute_business()
    df_pre_business = df_pre_business.cache()
    main()