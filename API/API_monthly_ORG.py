from elasticsearch import Elasticsearch
from datetime import datetime
from elasticsearch import RequestsHttpConnection
import csv

ID_list = [1464277,1476991,73239,1387111,43341,22703,22705,42981,20801,1596694,64625,232613,300673,52313,270093,86475,340213,150842,52315,1583184,156119,266041,1536927,1583099,43341,351,77335,300681,15543,1,1344691,1464277,217,49785,1401989,1570509,49783,3,8639,1596694,1355897,1510661,1596692,1323575,952537,1367513,1336767]
must_not_list = []
for item in ID_list:
    value_dict = dict()
    value_dict["OrganizationID"] = str(item)
    this_dict = {"match_phrase":value_dict}
    must_not_list.append(this_dict)
    
def get_top_orgids(from_date,to_date,X,Y):
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

    res = es.search(
        index="search-requests-v1-api-*",
        **{
            "stored_fields": ["*"],
            "body": {
                 "query": {
                    "bool": {
                      "must": [],
                      "filter": [
                        {"match_all": {}},
                        {"match_phrase": {"search_pointer": "false"}},
                        {"match_phrase": {"api.IsSuggestedSearch": "false"}},
                        {
                          "bool": {
                            "should": [
                              {"match": {"phones_country_codes.country_code": X}},
                              {"match": {"addresses.country.keyword": Y}}
                            ]
                          }
                        },
                        {
                          "range": {
                            "@timestamp": {
                                "gte": "{}T00:00:00.000Z".format(from_date),
                                "lte": "{}T00:00:00.000Z".format(to_date),
                              "format": "strict_date_optional_time"
                            }
                          }
                        }
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
          "terms": {
            "field": "OrganizationID",
            "order": {
              "_count": "desc"
            },
            "size": 10
          }
        }
      }

            },
    "_source": {"excludes": []},   
        }
        
    )
    org_raw=res.get("aggregations", {}).get("level", {}).get("buckets", [])
    org=[]
    for item in org_raw:
        org.append(item.get("key"))
    print org
    
    return org


def get_total_queries(OrgId,from_date, to_date,X,Y):
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


    res = es.search(
        index="search-requests-v1-api-*",
        **{
            #"size": 0,
            "stored_fields": ["*"],
            "body": {
                "query": {
                    "bool": {
                        "must": [
                             {
                                  "match_phrase": {
                                    "OrganizationID": {
                                      "query": OrgId
                                    }
                                  }
                                },
                            {
                                "range": {
                                    "@timestamp": {
                                        "gte": "{}T00:00:00.000Z".format(from_date),
                                        "lt": "{}T00:00:00.000Z".format(to_date),
                                        "format": "strict_date_time",
                                    }
                                }
                            },

                            
                                {"bool": {
                                    "should": [
                                        {"match": {"phones_country_codes.country_code": X}},
                                        {"match": {"addresses.country.keyword": Y}},
                                    ]
                                    }
                                },
                        ],
                    "must_not": {"term": {"search_pointer": "true"}},
                    "must_not": {"term": {"api.IsSuggestedSearch": "true"}},
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
                            }

            },
    "_source": {"excludes": []},   
        }
        
    )
    print res
    if len(res.get("aggregations", {}).get("level", {}).get("buckets", []))==0:
        result=0
    else:
        result=res.get('aggregations').get('level').get('buckets', [])[0].get('doc_count')
    #print result
    return result


def get_top_search_field(OrgId,from_date, to_date,X,Y):
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


    res = es.search(
        index="search-requests-v1-api-*",
        **{
            "size": 0,
            "body": {
                "query": {
                    "bool": {
                        "must": [
                             {
                                  "match_phrase": {
                                    "OrganizationID": {
                                      "query": OrgId
                                    }
                                  }
                                },
                            {
                                "range": {
                                    "@timestamp": {
                                        "gte": "{}T00:00:00.000Z".format(from_date),
                                        "lt": "{}T00:00:00.000Z".format(to_date),
                                        "format": "strict_date_time",
                                    }
                                }
                            },

                                {"bool": {
                                    "should": [
                                        {"match": {"phones_country_codes.country_code": X}},
                                        {"match": {"addresses.country.keyword": Y}},
                                    ]
                                    }
                                },
                        ],
                                    
                    "must_not": {"term": {"search_pointer": "true"}},
                    "must_not": {"term": {"api.IsSuggestedSearch": "true"}},
               }
            },
                 
                     "aggs": {
                            "level": {
                                "terms":{
                                    "script":{
                                        "source":
					    "  String searchInput = \"\";\n        boolean isFieldAdded = false;\n        boolean isAddressAdded = false;\n        if (doc['pse_stats.QueryNamesLastFirst'].size() != 0) {\n            if (doc['pse_stats.QueryNamesLastFirst'].value > 0) {\n                searchInput += (isFieldAdded) ? \", \" : \"\";\n                if (doc['pse_stats.QueryNamesLastFirst'].value == 1) {\n                    searchInput += \"Name\";\n                    isFieldAdded = true;\n                } else {\n                    searchInput += \"Names\";\n                    isFieldAdded = true;\n                }\n            }\n        }\n        if (doc['pse_stats.QueryLocationsCountry'].size() != 0 || doc['pse_stats.QueryLocationsCountryState'].size() != 0 || doc['pse_stats.QueryLocationsCountryStateCity'].size() != 0)  {\n            if (doc['pse_stats.QueryLocationsCountryStateCity'].size() != 0) {\n                if (doc['pse_stats.QueryLocationsCountryStateCity'].value > 0) {\n                    isAddressAdded = true;\n                    searchInput += (isFieldAdded) ? \", \" : \"\";\n                    if (doc['pse_stats.QueryLocationsCountryStateCity'].value == 1) {\n                        searchInput += \"Address\";\n                        isFieldAdded = true;\n                    }\n                    else {\n                        searchInput += \"Addresses\";\n                        isFieldAdded = true;\n                    }\n                }\n            }\n            if (!isAddressAdded) {\n                if (doc['pse_stats.QueryLocationsCountryState'].size() != 0) {\n                    if (doc['pse_stats.QueryLocationsCountryState'].value > 0) {\n                        isAddressAdded = true;\n                        searchInput += (isFieldAdded) ? \", \" : \"\";\n                        if (doc['pse_stats.QueryLocationsCountryState'].value == 1) {\n                            searchInput += \"Address\";\n                            isFieldAdded = true;\n                        }\n                        else {\n                            searchInput += \"Addresses\";\n                            isFieldAdded = true;\n                        }\n                    }\n                }\n            }\n            if (!isAddressAdded) {\n                if (doc['pse_stats.QueryLocationsCountry'].size() != 0) {\n                    if (doc['pse_stats.QueryLocationsCountry'].value > 0) {\n                        searchInput += (isFieldAdded) ? \", \" : \"\";\n                        if (doc['pse_stats.QueryLocationsCountry'].value == 1) {\n                            searchInput += \"Address\";\n                            isFieldAdded = true;\n                        }\n                        else {\n                            searchInput += \"Addresses\";\n                            isFieldAdded = true;\n                        }\n                    }\n                }\n            }\n        }\n        if (doc['pse_stats.QueryDOBs'].size() != 0) {\n            if (doc['pse_stats.QueryDOBs'].value == 1) {\n                searchInput += (isFieldAdded) ? \", \" : \"\";\n                searchInput += \"DOB\";\n                isFieldAdded = true;\n            }\n        }\n        if (doc['pse_stats.QueryEmails'].size() != 0) {\n            if (doc['pse_stats.QueryEmails'].value > 0) {\n                searchInput += (isFieldAdded) ? \", \" : \"\";\n                if (doc['pse_stats.QueryEmails'].value == 1) {\n                    searchInput += \"Email\";\n                    isFieldAdded = true;\n                } else {\n                    searchInput += \"Emails\";\n                    isFieldAdded = true;\n                }\n            }\n        }\n        if (doc['pse_stats.QueryPhones'].size() != 0) {\n            if (doc['pse_stats.QueryPhones'].value > 0) {\n                searchInput += (isFieldAdded) ? \", \" : \"\";\n                if (doc['pse_stats.QueryPhones'].value == 1) {\n                    searchInput += \"Phone\";\n                    isFieldAdded = true;\n                } else {\n                    searchInput += \"Phones\";\n                    isFieldAdded = true;\n                }\n            }\n        }\n        if (doc['pse_stats.QueryUsernames'].size() != 0) {\n            if (doc['pse_stats.QueryUsernames'].value > 0) {\n                searchInput += (isFieldAdded) ? \", \" : \"\";\n                if (doc['pse_stats.QueryUsernames'].value == 1) {\n                    searchInput += \"Username\";\n                    isFieldAdded = true;\n                }\n                else {\n                    searchInput += \"Usernames\";\n                    isFieldAdded = true;\n                }\n            }\n        }\n        if (doc['pse_stats.QueryUserIDs'].size() != 0) {\n            if (doc['pse_stats.QueryUserIDs'].value > 0) {\n                searchInput += (isFieldAdded) ? \", \" : \"\";\n                if (doc['pse_stats.QueryUserIDs'].value == 1) {\n                    searchInput += \"UserID\";\n                    isFieldAdded = true;\n                } else {\n                    searchInput += \"UserIDs\";\n                    isFieldAdded = true;\n                }\n            }\n        }\n        if (doc['pse_stats.QueryEducations'].size() != 0) {\n            if (doc['pse_stats.QueryEducations'].value > 0) {\n                searchInput += (isFieldAdded) ? \", \" : \"\";\n                if (doc['pse_stats.QueryEducations'].value == 1) {\n                    searchInput += \"Education\";\n                    isFieldAdded = true;\n                } else {\n                    searchInput += \"Educations\";\n                    isFieldAdded = true;\n                }\n            }\n        }\n        if (doc['pse_stats.QueryJobs'].size() != 0) {\n            if (doc['pse_stats.QueryJobs'].value > 0) {\n                searchInput += (isFieldAdded) ? \", \" : \"\";\n                if (doc['pse_stats.QueryJobs'].value == 1) {\n                    searchInput += \"Job\";\n                    isFieldAdded = true;\n                } else {\n                    searchInput += \"Jobs\";\n                    isFieldAdded = true;\n                }\n            }\n        }\n        return searchInput;",
                                        "lang": "painless"
                                        },
                                    "size": 5,
                                    "order": {"_count": "desc"},
                                    "valueType": "string"
                                    }
                                }
                            }

            },
        }
        
    )

    return res.get("aggregations", {}).get("level", {}).get("buckets", [])


def get_match_rate(OrgId,from_date, to_date,X,Y):
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


    res = es.search(
        index="search-requests-v1-api-*",
        **{
            "size": 0,
            "body": {
                "query": {
                    "bool": {
                        "must": [
                             {
                                  "match_phrase": {
                                    "OrganizationID": {
                                      "query": OrgId
                                    }
                                  }
                                },
                            {
                                "range": {
                                    "@timestamp": {
                                        "gte": "{}T00:00:00.000Z".format(from_date),
                                        "lt": "{}T00:00:00.000Z".format(to_date),
                                        "format": "strict_date_time",
                                    }
                                }
                            },

                                {"bool": {
                                    "should": [
                                        {"match": {"phones_country_codes.country_code": X}},
                                        {"match": {"addresses.country.keyword": Y}},
                                    ]
                                    }
                                },
                        ],
                    "must_not": {"term": {"search_pointer": "true"}},
                    "must_not": {"term": {"api.IsSuggestedSearch": "true"}},
                            
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

            },
        }
        
    )
    return res.get("aggregations", {}).get("9", {}).get("buckets", [])


def get_FillRate(OrgId,from_date, to_date,X,Y,fields):
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

    org_id = 1821

    res = es.search(
        index="search-requests-v1-api-*",
        **{
            "size": 0,
            "body": {
                "query": {
                    "bool": {
                        "must": [
                            {
                                  "match_phrase": {
                                    "OrganizationID": {
                                      "query": OrgId
                                    }
                                  }
                                },
                            {
                                "range": {
                                    "@timestamp": {
                                        "gte": "{}T00:00:00.000Z".format(from_date),
                                        "lt": "{}T00:00:00.000Z".format(to_date),
                                        "format": "strict_date_time",
                                    }
                                }
                            },
            
                            
                                {"bool": {
                                    "should": [
                                        {"match": {"phones_country_codes.country_code": X}},
                                        {"match": {"addresses.country.keyword": Y}},
                                    ]
                                    }
                                },
                        ],
                        
                    "must_not": {"term": {"search_pointer": "true"}},
                    "must_not": {"term": {"api.IsSuggestedSearch": "true"}},


               }
            },
                 
  "aggs": {
    "7": {
      "filters": {
          "filters": {
              "with" + " " + str(fields): {
                "exists": {
                  #"field": "api.available_data.premium.phones"
                  "field": "api.available_data.premium." + str(fields)
                }
              },
              "without" + " " + str(fields): {
                "bool": {
                  "must_not": {
                    "exists": {
                      "field": "api.available_data.premium." + str(fields)
                    }
                  }
                }
              }
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
      }
    }
  },
  },
        }
        
    )
    #print res
    return res


#x = [[None for _ in range(27)] for _ in range(500)]
x = [[None for _ in range(50)] for _ in range(50000)]
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

    with open("/opt/eval/ds_evaluation/elasticsearch/CountryVsPhone_20.csv") as csvfile:
        spamreader = csv.reader(csvfile, delimiter=',', quotechar='|')
        kk = 2
        for row in spamreader:
            country = row[0]
            country_code = row[1]
            ids=get_top_orgids(datetime.now().replace(year=from_year, month=from_month, day=from_day), datetime.now().replace(year=to_year, month=to_month, day=to_day),int(country_code),country)
	    #orgids=[7963,539,85843,16249,1594600,53853,20781,73239,1344191,22225]
            orgids=ids
            #print ids
            for OrgId in orgids:
                total = get_total_queries(int(OrgId),datetime.now().replace(year=from_year, month=from_month, day=from_day), datetime.now().replace(year=to_year, month=to_month, day=to_day),int(country_code),country)
                print (country,OrgId)
                x[(kk-2)*N+1][0] = str(OrgId)+ "#"+ country 
                x[(kk-2)*N+1][1] = 'All'
                x[(kk-2)*N+1][3] = total
                if total!=0:
                
                    res=get_top_search_field(int(OrgId),datetime.now().replace(year=from_year, month=from_month, day=from_day), datetime.now().replace(year=to_year, month=to_month, day=to_day),int(country_code),country)
                    sumP = 0
                    sumT = 0
                    for ii in range(len(res)):
                        x[(kk-2)*N+2+ii][1] = res[ii]['key']
                        x[(kk-2)*N+2+ii][3] = res[ii]['doc_count']
                        x[(kk-2)*N+2+ii][2] = str(res[ii]['doc_count']*1.0/total*100)+'%'
                        sumP = sumP + res[ii]['doc_count']*1.0/total*100
                        sumT = sumT + float(x[(kk-2)*N+2+ii][3])
                    x[(kk-2)*N+2+10][2] = str(sumP)+'%'
                    x[(kk-2)*N+2+10][3] = sumT
                    res = get_match_rate(int(OrgId),datetime.now().replace(year=from_year, month=from_month, day=from_day), datetime.now().replace(year=to_year, month=to_month, day=to_day),int(country_code),country)
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
                    
                    total = a + b + c
                    x[(kk-2)*N+2][5] = str(round(a*1.0/total*100,1))+'%'
                    x[(kk-2)*N+1][5] = str(round(b*1.0/total*100,1))+'%'
                    x[(kk-2)*N+3][5] = str(round(c*1.0/total*100,1))+'%'
                    fields=["phones", "emails", "social_profiles", "jobs","educations","addresses","dobs"]

                    x[(kk-2)*N+1][18] = 'NoMatch'
                    x[(kk-2)*N+2][18] = 'NoMatch'
                    x[(kk-2)*N+3][18] = 'PerfectMatch'
                    x[(kk-2)*N+4][18] = 'PerfectMatch'
                    x[(kk-2)*N+5][18] = 'PossibleMatch'
                    x[(kk-2)*N+6][18] = 'PossibleMatch'  

                    for ii in range(len(fields)):
                        item = fields[ii]
                        x[(kk-2)*N+ii+1][16] = item
                        res_dummy = get_FillRate(int(OrgId), datetime.now().replace(year=from_year, month=from_month, day=from_day), datetime.now().replace(year=to_year, month=to_month, day=to_day), int(country_code),country, item)
                        #print(res.keys())
                        res = res_dummy['aggregations']['7']['buckets']
                        try:
                            for key in res.keys():

                                if key =='with '+str(item):
                                    x[(kk-2)*N+3][20+ii*2] = res[key]['9']['buckets']['1.0-2.0']['doc_count']
                                    x[(kk-2)*N+3][19+ii*2] = 'with '+item
                                    x[(kk-2)*N+1][20+ii*2] = res[key]['9']['buckets']['0.0-1.0']['doc_count']
                                    x[(kk-2)*N+1][19+ii*2] = 'with '+item
                                    x[(kk-2)*N+5][20+ii*2] = res[key]['9']['buckets']['2.0-*']['doc_count']
                                    x[(kk-2)*N+5][19+ii*2] = 'with '+item
                                else:
                                    x[(kk-2)*N+4][20+ii*2] = res[key]['9']['buckets']['1.0-2.0']['doc_count']
                                    x[(kk-2)*N+4][19+ii*2] = 'without '+item
                                    x[(kk-2)*N+2][20+ii*2] = res[key]['9']['buckets']['0.0-1.0']['doc_count']
                                    x[(kk-2)*N+2][19+ii*2] = 'without '+item
                                    x[(kk-2)*N+6][20+ii*2] = res[key]['9']['buckets']['2.0-*']['doc_count']
                                    x[(kk-2)*N+6][19+ii*2] = 'without '+item
                            x[(kk-2)*N+ii+1][17] = str(round(x[(kk-2)*N+3][20+ii*2]*1.0/(x[(kk-2)*N+3][20+ii*2]+x[(kk-2)*N+4][20+ii*2])*100,1))+'%'
                        except ZeroDivisionError:
                            x[(kk-2)*N+ii+1][17] =0

                else:
                    x[(kk-2)*N+1][0] = str(OrgId)+ "#"+ country 
                    x[(kk-2)*N+1][1] = 'All'
                    x[(kk-2)*N+1][3] = total
                    
                kk = kk + 1   

    with open('/opt/eval/ds_evaluation/API_monthly_ORG_'+str(savefilename)+'.csv', "wb") as f:
        writer = csv.writer(f)
        writer.writerows(x)
        
##months = [12,1,2,3,4]
##years = [2019,2020,2020,2020,2020]

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































