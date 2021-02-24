from elasticsearch import Elasticsearch
from datetime import datetime
from elasticsearch import RequestsHttpConnection
import csv

ID_list = [1510661,300673,1428999,217,49783,1464277,3,86267,1596694,15543,1573811,1367529,1375247,1342811,49785,1366345,1570509,1522673,1355897,1596692,1604120,229,86475,1548553,1621220,1492893,1377941,1573943,1401989,1596470,1344691,1571305,1429759,1500003,1314507,8639,83815,1559577,91073,25,1318365,1457225,8567,234853,93005,1612290,21,1632018,117453,1378253,232613,20801,150842,146463,1325317,22703,73239,22705,353577,156119,107759,43341,53299,1635732,42981,270093,19945,643273,270437,47145,77335,102521,266041,318633,309341,1536617,76479,292469,131935,1604120,1596694,1628944,163551,1500003,1532943,1596470,270121,353557,109493,158037,1536597,302589,513569,1612290,1492893,1596692,83039,208373,110165,1632018,1621220,1531519,1640122,1613154,1570509,300681,1573811,1536619,1547065,1583812,233305,1578967,134057,1601650,1598262,101523,1583119,1464277,1565825,1546371,1583804,1583144,1578163,24281,64625,1547081,1536369,1564589,163933,1533813,1629000,1541639,1562201,1603740,1530509,1583099,1529559,952537,1438411,1471841,1313437]

must_not_list = []
for item in ID_list:
    value_dict = dict()
    value_dict["OrganizationID"] = str(item)
    this_dict = {"match_phrase":value_dict}
    must_not_list.append(this_dict)
    
def get_total_queries_overall(from_date, to_date):
    es = Elasticsearch(
    ["https://elastic-restricted.pipl.pro:9200"],
    http_auth=("Fangwei.Liu@pipl.com","K@S^oyn9$R"),
    connection_class=RequestsHttpConnection,
    use_ssl=True,
    verify_certs=False,
    sniff_on_start=False,
    sniff_on_connection_fail=False,
    sniff_timeout=30,
    sniffer_timeout=30,
    )

    from_date = from_date.strftime("%Y-%m-%d")
    to_date = to_date.strftime("%Y-%m-%d")

    # The limit of users in organization that we will get good results to
    CRM_ELASTIC_USER_ID_TOP_BUCKETS_SIZE = 10000
    # The limit of fields we will get good results in
    CRM_ELASTIC_FIELDS_TOP_BUCKETS_SIZE = 150

    #org_id = 36217

    res = es.search(
        index="search-requests-v1-pro-*",
        **{
            #"size": 0,
            "stored_fields": ["*"],
            "body": {
                "query": {
                    "bool": {
                      "must": [],
                      "filter": [
                        {"match_all": {}},
                        {"match_phrase": {"search_pointer": "false"}},
                        #{"match_phrase": {"api.IsSuggestedSearch": "false"}},
                        {
                          "range": {
                            "@timestamp": {
                                "gte": "{}T00:00:00.000Z".format(from_date),
                                "lte": "{}T00:00:00.000Z".format(to_date),
                              "format": "strict_date_optional_time"
                            }
                          }
                        },
                    {
                      "script": {
                        "script": {
                          "source": "boolean compare(Supplier s, def v) {return s.get() == v;}compare(() -> {   String searchInput = \"\";\n        boolean isFieldAdded = false;\n        boolean isAddressAdded = false;\n        if (doc['pse_stats.QueryNamesLastFirst'].size() != 0) {\n            if (doc['pse_stats.QueryNamesLastFirst'].value > 0) {\n                searchInput += (isFieldAdded) ? \", \" : \"\";\n                if (doc['pse_stats.QueryNamesLastFirst'].value == 1) {\n                    searchInput += \"Name\";\n                    isFieldAdded = true;\n                } else {\n                    searchInput += \"Names\";\n                    isFieldAdded = true;\n                }\n            }\n        }\n        if (doc['pse_stats.QueryLocationsCountry'].size() != 0 || doc['pse_stats.QueryLocationsCountryState'].size() != 0 || doc['pse_stats.QueryLocationsCountryStateCity'].size() != 0)  {\n            if (doc['pse_stats.QueryLocationsCountryStateCity'].size() != 0) {\n                if (doc['pse_stats.QueryLocationsCountryStateCity'].value > 0) {\n                    isAddressAdded = true;\n                    searchInput += (isFieldAdded) ? \", \" : \"\";\n                    if (doc['pse_stats.QueryLocationsCountryStateCity'].value == 1) {\n                        searchInput += \"Address\";\n                        isFieldAdded = true;\n                    }\n                    else {\n                        searchInput += \"Addresses\";\n                        isFieldAdded = true;\n                    }\n                }\n            }\n            if (!isAddressAdded) {\n                if (doc['pse_stats.QueryLocationsCountryState'].size() != 0) {\n                    if (doc['pse_stats.QueryLocationsCountryState'].value > 0) {\n                        isAddressAdded = true;\n                        searchInput += (isFieldAdded) ? \", \" : \"\";\n                        if (doc['pse_stats.QueryLocationsCountryState'].value == 1) {\n                            searchInput += \"Address\";\n                            isFieldAdded = true;\n                        }\n                        else {\n                            searchInput += \"Addresses\";\n                            isFieldAdded = true;\n                        }\n                    }\n                }\n            }\n            if (!isAddressAdded) {\n                if (doc['pse_stats.QueryLocationsCountry'].size() != 0) {\n                    if (doc['pse_stats.QueryLocationsCountry'].value > 0) {\n                        searchInput += (isFieldAdded) ? \", \" : \"\";\n                        if (doc['pse_stats.QueryLocationsCountry'].value == 1) {\n                            searchInput += \"Address\";\n                            isFieldAdded = true;\n                        }\n                        else {\n                            searchInput += \"Addresses\";\n                            isFieldAdded = true;\n                        }\n                    }\n                }\n            }\n        }\n        if (doc['pse_stats.QueryDOBs'].size() != 0) {\n            if (doc['pse_stats.QueryDOBs'].value == 1) {\n                searchInput += (isFieldAdded) ? \", \" : \"\";\n                searchInput += \"DOB\";\n                isFieldAdded = true;\n            }\n        }\n        if (doc['pse_stats.QueryEmails'].size() != 0) {\n            if (doc['pse_stats.QueryEmails'].value > 0) {\n                searchInput += (isFieldAdded) ? \", \" : \"\";\n                if (doc['pse_stats.QueryEmails'].value == 1) {\n                    searchInput += \"Email\";\n                    isFieldAdded = true;\n                } else {\n                    searchInput += \"Emails\";\n                    isFieldAdded = true;\n                }\n            }\n        }\n        if (doc['pse_stats.QueryPhones'].size() != 0) {\n            if (doc['pse_stats.QueryPhones'].value > 0) {\n                searchInput += (isFieldAdded) ? \", \" : \"\";\n                if (doc['pse_stats.QueryPhones'].value == 1) {\n                    searchInput += \"Phone\";\n                    isFieldAdded = true;\n                } else {\n                    searchInput += \"Phones\";\n                    isFieldAdded = true;\n                }\n            }\n        }\n        if (doc['pse_stats.QueryUsernames'].size() != 0) {\n            if (doc['pse_stats.QueryUsernames'].value > 0) {\n                searchInput += (isFieldAdded) ? \", \" : \"\";\n                if (doc['pse_stats.QueryUsernames'].value == 1) {\n                    searchInput += \"Username\";\n                    isFieldAdded = true;\n                }\n                else {\n                    searchInput += \"Usernames\";\n                    isFieldAdded = true;\n                }\n            }\n        }\n        if (doc['pse_stats.QueryUserIDs'].size() != 0) {\n            if (doc['pse_stats.QueryUserIDs'].value > 0) {\n                searchInput += (isFieldAdded) ? \", \" : \"\";\n                if (doc['pse_stats.QueryUserIDs'].value == 1) {\n                    searchInput += \"UserID\";\n                    isFieldAdded = true;\n                } else {\n                    searchInput += \"UserIDs\";\n                    isFieldAdded = true;\n                }\n            }\n        }\n        if (doc['pse_stats.QueryEducations'].size() != 0) {\n            if (doc['pse_stats.QueryEducations'].value > 0) {\n                searchInput += (isFieldAdded) ? \", \" : \"\";\n                if (doc['pse_stats.QueryEducations'].value == 1) {\n                    searchInput += \"Education\";\n                    isFieldAdded = true;\n                } else {\n                    searchInput += \"Educations\";\n                    isFieldAdded = true;\n                }\n            }\n        }\n        if (doc['pse_stats.QueryJobs'].size() != 0) {\n            if (doc['pse_stats.QueryJobs'].value > 0) {\n                searchInput += (isFieldAdded) ? \", \" : \"\";\n                if (doc['pse_stats.QueryJobs'].value == 1) {\n                    searchInput += \"Job\";\n                    isFieldAdded = true;\n                } else {\n                    searchInput += \"Jobs\";\n                    isFieldAdded = true;\n                }\n            }\n        }\n        return searchInput; }, params.value);",
                          "lang": "painless",
                          "params": {
                            "value": "Email"
                          }
                        }
                      }
                    },
                ],
                      "should": [],
                      "must_not": [
                        {
                          "bool": {
                            "should": must_not_list,
                            "minimum_should_match": 1
                          }
                        }
                      ]

                   }
                },
                     
            "aggs": {
            "level": {
              "date_histogram": {
                "field": "@timestamp",
                "calendar_interval": "1M",
                "time_zone": "UTC",
                "min_doc_count": 1
                  }
                }
              },
            }
        }
     )
    if len(res.get("aggregations", {}).get("level", {}).get("buckets", []))==0:
        result=0
    else:
        result=res.get('aggregations').get('level').get('buckets', [])[0].get('doc_count')
    #print result
    return result

def get_match_rate_by_field_overall(from_date,to_date):
    es = Elasticsearch(
    ["https://elastic-restricted.pipl.pro:9200"],
    http_auth=("Fangwei.Liu@pipl.com","K@S^oyn9$R"),
    connection_class=RequestsHttpConnection,
    use_ssl=True,
    verify_certs=False,
    sniff_on_start=False,
    sniff_on_connection_fail=False,
    sniff_timeout=30,
    sniffer_timeout=30,
    )

    from_date = from_date.strftime("%Y-%m-%d")
    to_date = to_date.strftime("%Y-%m-%d")

    # The limit of users in organization that we will get good results to
    CRM_ELASTIC_USER_ID_TOP_BUCKETS_SIZE = 10000
    # The limit of fields we will get good results in
    CRM_ELASTIC_FIELDS_TOP_BUCKETS_SIZE = 150

    #org_id = 36217

    res = es.search(
        index="search-requests-v1-pro-*",
        **{
            "stored_fields": ["*"],
            "body": {
                "query": {
                    "bool": {
                      "must": [],
                      "filter": [
                        {"match_all": {}},
                        {"match_phrase": {"search_pointer": "false"}},
                        #{"match_phrase": {"api.IsSuggestedSearch": "false"}},
                        {
                          "range": {
                            "@timestamp": {
                                "gte": "{}T00:00:00.000Z".format(from_date),
                                "lte": "{}T00:00:00.000Z".format(to_date),
                              "format": "strict_date_optional_time"
                            }
                          }
                        },
                    {
                      "script": {
                        "script": {
                          "source": "boolean compare(Supplier s, def v) {return s.get() == v;}compare(() -> {   String searchInput = \"\";\n        boolean isFieldAdded = false;\n        boolean isAddressAdded = false;\n        if (doc['pse_stats.QueryNamesLastFirst'].size() != 0) {\n            if (doc['pse_stats.QueryNamesLastFirst'].value > 0) {\n                searchInput += (isFieldAdded) ? \", \" : \"\";\n                if (doc['pse_stats.QueryNamesLastFirst'].value == 1) {\n                    searchInput += \"Name\";\n                    isFieldAdded = true;\n                } else {\n                    searchInput += \"Names\";\n                    isFieldAdded = true;\n                }\n            }\n        }\n        if (doc['pse_stats.QueryLocationsCountry'].size() != 0 || doc['pse_stats.QueryLocationsCountryState'].size() != 0 || doc['pse_stats.QueryLocationsCountryStateCity'].size() != 0)  {\n            if (doc['pse_stats.QueryLocationsCountryStateCity'].size() != 0) {\n                if (doc['pse_stats.QueryLocationsCountryStateCity'].value > 0) {\n                    isAddressAdded = true;\n                    searchInput += (isFieldAdded) ? \", \" : \"\";\n                    if (doc['pse_stats.QueryLocationsCountryStateCity'].value == 1) {\n                        searchInput += \"Address\";\n                        isFieldAdded = true;\n                    }\n                    else {\n                        searchInput += \"Addresses\";\n                        isFieldAdded = true;\n                    }\n                }\n            }\n            if (!isAddressAdded) {\n                if (doc['pse_stats.QueryLocationsCountryState'].size() != 0) {\n                    if (doc['pse_stats.QueryLocationsCountryState'].value > 0) {\n                        isAddressAdded = true;\n                        searchInput += (isFieldAdded) ? \", \" : \"\";\n                        if (doc['pse_stats.QueryLocationsCountryState'].value == 1) {\n                            searchInput += \"Address\";\n                            isFieldAdded = true;\n                        }\n                        else {\n                            searchInput += \"Addresses\";\n                            isFieldAdded = true;\n                        }\n                    }\n                }\n            }\n            if (!isAddressAdded) {\n                if (doc['pse_stats.QueryLocationsCountry'].size() != 0) {\n                    if (doc['pse_stats.QueryLocationsCountry'].value > 0) {\n                        searchInput += (isFieldAdded) ? \", \" : \"\";\n                        if (doc['pse_stats.QueryLocationsCountry'].value == 1) {\n                            searchInput += \"Address\";\n                            isFieldAdded = true;\n                        }\n                        else {\n                            searchInput += \"Addresses\";\n                            isFieldAdded = true;\n                        }\n                    }\n                }\n            }\n        }\n        if (doc['pse_stats.QueryDOBs'].size() != 0) {\n            if (doc['pse_stats.QueryDOBs'].value == 1) {\n                searchInput += (isFieldAdded) ? \", \" : \"\";\n                searchInput += \"DOB\";\n                isFieldAdded = true;\n            }\n        }\n        if (doc['pse_stats.QueryEmails'].size() != 0) {\n            if (doc['pse_stats.QueryEmails'].value > 0) {\n                searchInput += (isFieldAdded) ? \", \" : \"\";\n                if (doc['pse_stats.QueryEmails'].value == 1) {\n                    searchInput += \"Email\";\n                    isFieldAdded = true;\n                } else {\n                    searchInput += \"Emails\";\n                    isFieldAdded = true;\n                }\n            }\n        }\n        if (doc['pse_stats.QueryPhones'].size() != 0) {\n            if (doc['pse_stats.QueryPhones'].value > 0) {\n                searchInput += (isFieldAdded) ? \", \" : \"\";\n                if (doc['pse_stats.QueryPhones'].value == 1) {\n                    searchInput += \"Phone\";\n                    isFieldAdded = true;\n                } else {\n                    searchInput += \"Phones\";\n                    isFieldAdded = true;\n                }\n            }\n        }\n        if (doc['pse_stats.QueryUsernames'].size() != 0) {\n            if (doc['pse_stats.QueryUsernames'].value > 0) {\n                searchInput += (isFieldAdded) ? \", \" : \"\";\n                if (doc['pse_stats.QueryUsernames'].value == 1) {\n                    searchInput += \"Username\";\n                    isFieldAdded = true;\n                }\n                else {\n                    searchInput += \"Usernames\";\n                    isFieldAdded = true;\n                }\n            }\n        }\n        if (doc['pse_stats.QueryUserIDs'].size() != 0) {\n            if (doc['pse_stats.QueryUserIDs'].value > 0) {\n                searchInput += (isFieldAdded) ? \", \" : \"\";\n                if (doc['pse_stats.QueryUserIDs'].value == 1) {\n                    searchInput += \"UserID\";\n                    isFieldAdded = true;\n                } else {\n                    searchInput += \"UserIDs\";\n                    isFieldAdded = true;\n                }\n            }\n        }\n        if (doc['pse_stats.QueryEducations'].size() != 0) {\n            if (doc['pse_stats.QueryEducations'].value > 0) {\n                searchInput += (isFieldAdded) ? \", \" : \"\";\n                if (doc['pse_stats.QueryEducations'].value == 1) {\n                    searchInput += \"Education\";\n                    isFieldAdded = true;\n                } else {\n                    searchInput += \"Educations\";\n                    isFieldAdded = true;\n                }\n            }\n        }\n        if (doc['pse_stats.QueryJobs'].size() != 0) {\n            if (doc['pse_stats.QueryJobs'].value > 0) {\n                searchInput += (isFieldAdded) ? \", \" : \"\";\n                if (doc['pse_stats.QueryJobs'].value == 1) {\n                    searchInput += \"Job\";\n                    isFieldAdded = true;\n                } else {\n                    searchInput += \"Jobs\";\n                    isFieldAdded = true;\n                }\n            }\n        }\n        return searchInput; }, params.value);",
                          "lang": "painless",
                          "params": {
                            "value": "Email"
                          }
                        }
                      }
                    },
                ],
                      "should": [],
                      "must_not": [
                        {
                          "bool": {
                            "should": must_not_list,
                            "minimum_should_match": 1
                          }
                        }
                      ]

                   }
                },
                 
                "aggs": {
                    "9": {
                      "range": {
                        "field": "pse_stats.MatchingProfiles",
                        "ranges": [
                          {
                            "from": 0,
                            "to": 1
                          },
                          {
                            "from": 1,
                            "to": 2
                          },
                          {
                            "from": 2
                          }
                        ],
                        "keyed": "true"
                      }
                    }
                },

            }
        }
        
    )
    #print res
    return res.get("aggregations", {}).get("9", {}).get("buckets", [])



#x = [[None for _ in range(27)] for _ in range(500)]
x = [[None for _ in range(50)] for _ in range(10000)]
x[0][0] = 'From'
x[0][1] = datetime.now().replace(year=2019,month=6,day=1)
x[0][2] = 'To'
x[0][3] = datetime.now().replace(year=2020,month=5,day=31)
x[0][4] = 'Match_type'
x[0][5] = 'Match Rate'
x[0][6] = 'Count per match_type'
x[0][7] = 'filters'
x[0][8] = 'MatchRequirement Ratio'
x[0][9] = 'Count'
x[0][10] = 'MatchRequirements'
x[0][11] = 'Top 5 MatchRequirement'
x[0][12] = 'Count per MatchRequirements'
x[0][13] = 'APIMinimumMatch'
x[0][14] = 'MinimumMatch breakdown'
x[0][15] = 'Count per MinimumMatch'

x[0][16] = 'Fields'
x[0][17] = 'Fill Rate'
x[0][18] = 'match_type.' 
x[0][19] = 'filters'
x[0][20] = 'Count'
x[0][21] = 'filters'
x[0][22] = 'Count'
x[0][23] = 'filters'
x[0][24] = 'Count'
x[0][25] = 'filters'
x[0][26] = 'Count'


N = 12
space = 2

def datamonthbymonth(from_year,to_year,from_month,to_month,from_day,to_day,savefilename):
    x[0][1] = datetime.now().replace(year=from_year,month=from_month,day=from_day)
    x[0][3] = datetime.now().replace(year=to_year,month=to_month,day=to_day)
    with open("/opt/eval/ds_evaluation/elasticsearch/CountryVsPhone_overall.csv") as csvfile:
        spamreader = csv.reader(csvfile, delimiter=',', quotechar='|')
        kk = 2
        for row in spamreader:
            country = row[0]
	    if country!='OverAll':
            	pass
            else:
                print(country)
                total = get_total_queries_overall(datetime.now().replace(year=from_year, month=from_month, day=from_day), datetime.now().replace(year=to_year, month=to_month, day=to_day))
                #print country,country_code
                x[(kk-2)*N+1][0] = country
                x[(kk-2)*N+1][1] = 'All'
                x[(kk-2)*N+1][3] = total
               
                res = get_match_rate_by_field_overall(datetime.now().replace(year=from_year, month=from_month, day=from_day), datetime.now().replace(year=to_year, month=to_month, day=to_day))
                for key in res.keys():
                    if key=='1.0-2.0':
                        a = res[key]['doc_count']
                        x[(kk-2)*N+2][6] = a
                        x[(kk-2)*N+2][4] = "SingleMatch"
                    elif key=='0.0-1.0':
                        b = res[key]['doc_count']
                        x[(kk-2)*N+1][6] = b
                        x[(kk-2)*N+1][4] = "NoMatch"
                    elif key=='2.0-*':
                        c = res[key]['doc_count']
                        x[(kk-2)*N+3][6] = c
                        x[(kk-2)*N+3][4] = "MultipleMatch"
		
                total = a+b+c
                if total == 0:
                    x[(kk-2)*N+2][5] = '0'
                    x[(kk-2)*N+1][5] = '0'
                    x[(kk-2)*N+3][5] = '0'

                else:
                    x[(kk-2)*N+2][5] = str(round(a*1.0/total*100,1))+'%'
                    x[(kk-2)*N+1][5] = str(round(b*1.0/total*100,1))+'%'
                    x[(kk-2)*N+3][5] = str(round(c*1.0/total*100,1))+'%'



            kk = kk + 1

    with open('/opt/eval/ds_evaluation/PRO_analysis_monthly_email'+str(savefilename)+'.csv', "wb") as f:
        writer = csv.writer(f)
        writer.writerows(x)

months = [12,1,2,3,4,5,6,7,8,9,10,11,12,1,2]
years = [2019,2020,2020,2020,2020,2020,2020,2020,2020,2020,2020,2020,2020,2021,2021]
day = 1
savefilename = 1
for ii in range(len(months)-1):
    from_year = years[ii]
    from_month = months[ii]
    to_month = months[ii+1]
    to_year = years[ii+1]
    to_day = 1
    from_day = 1
    datamonthbymonth(from_year,to_year,from_month,to_month,from_day,to_day,savefilename)
    print(savefilename)
    savefilename = savefilename + 1





