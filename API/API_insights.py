from elasticsearch import Elasticsearch
from datetime import datetime
import csv
import pandas as pd
from elasticsearch import RequestsHttpConnection

ID_list = [1464277,1476991,73239,1387111,43341,22703,22705,42981,20801,1596694,64625,232613,300673,52313,270093,86475,340213,150842,52315,1583184,156119,266041,1536927,1583099,43341,351,77335,300681,15543,1,1344691,1464277,217,49785,1401989,1570509,49783,3,8639,1596694,1355897,1510661,1596692,1323575,952537,1367513,1336767]
#ID_list = [1464277,1476991,73239]

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
                        {"match_phrase": {"match_type.keyword": {"query": "PerfectMatch"}}},
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
                  "date_histogram": {
                    "field": "@timestamp",
                    "calendar_interval": "1w",
                    "time_zone": "UTC",
                    "min_doc_count": 1
                            }
                        }
                    },
                }
            }  
        )
    print res.get("aggregations", {}).get("level", {}).get("buckets", [])[0].get('doc_count')
    return res.get("aggregations", {}).get("level", {}).get("buckets", [])[0].get('doc_count')

def get_fill_rate_subfields_overall(from_date, to_date,item):
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
        index="search-requests-v1-api-*",
        **{
            "size": 0,
            "body": {
                "query": {
                    "bool": {
                        "must": [],
                        "filter": [
                        {"match_all": {}},
                        {"match_phrase": {"search_pointer": "false"}},
                        {"match_phrase": {"api.IsSuggestedSearch": "false"}},
                        {"match_phrase": {"match_type.keyword": {"query": "PerfectMatch"}}},
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
            "2": {
              "range": {
                "field": "insights.response.fill.summary."+str(item),
                "ranges": [
                  {
                    "from": 0,
                    "to": 1
                  },
                  {
                    "from": 1,
                    "to": 1000
                  }
                ],
                "keyed": "true"
              }
            }
          },
        }
    }
    
)              
    #print (res.get('aggregations').get('2').get('buckets', []))
    return res.get('aggregations').get('2').get('buckets', [])

def get_addresses_subfields_overall(from_date, to_date,item):
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
        index="search-requests-v1-api-*",
        **{
            "size": 0,
            "body": {
                "query": {
                    "bool": {
                        "must": [],
                        "filter": [
                        {"match_all": {}},
                        {"match_phrase": {"search_pointer": "false"}},
                        {"match_phrase": {"api.IsSuggestedSearch": "false"}},
                        {"match_phrase": {"match_type.keyword": {"query": "PerfectMatch"}}},
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
            "2": {
              "range": {
                "field": "insights.response.fill.addresses.summary."+str(item),
                "ranges": [
                  {
                    "from": 0,
                    "to": 1
                  },
                  {
                    "from": 1,
                    "to": 1000
                  }
                ],
                "keyed": "true"
              }
            }
          },
        }
    }
    
)              

    return res.get('aggregations').get('2').get('buckets', [])


def get_social_overall(from_date, to_date,item):
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
        index="search-requests-v1-api-*",
        **{
            "size": 0,
            "body": {
                "query": {
                    "bool": {
                        "must": [],
                        "filter": [
                        {"match_all": {}},
                        {"match_phrase": {"search_pointer": "false"}},
                        {"match_phrase": {"api.IsSuggestedSearch": "false"}},
                        {"match_phrase": {"match_type.keyword": {"query": "PerfectMatch"}}},
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
            "2": {
              "range": {
                "field": "insights.response.fill.social_profiles.summary."+str(item),
                "ranges": [
                  {
                    "from": 1,
                    "to": 2
                  },
                  {
                    "from": 2,
                    "to": 1000
                  }
                ],
                "keyed": "true"
              }
            }
          },
        },
    }
    
)              
    
    return res.get('aggregations').get('2').get('buckets', [])

def get_total_queries(from_date, to_date,X,Y):
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
                        {"match_phrase": {"match_type.keyword": {"query": "PerfectMatch"}}},
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
                  "date_histogram": {
                    "field": "@timestamp",
                    "calendar_interval": "1w",
                    "time_zone": "UTC",
                    "min_doc_count": 1
                      }
                    }
                },

            }
        }
    )
    #print res
    if len(res.get("aggregations", {}).get("level", {}).get("buckets", []))==0:
        result=0
    else:
        result=res.get('aggregations').get('level').get('buckets', [])[0].get('doc_count')
    #print result
    return result

def get_fill_rate_subfields(from_date, to_date,X,Y,item):
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
                        {"match_phrase": {"match_type.keyword": {"query": "PerfectMatch"}}},
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
    "2": {
      "range": {
        "field": "insights.response.fill.summary."+str(item),
        "ranges": [
          {
            "from": 0,
            "to": 1
          },
          {
            "from": 1,
            "to": 1000
          }
        ],
        "keyed": "true"
      }
    }
  },

        },
    }
    
)              
    #print (res.get('aggregations').get('2').get('buckets', []))
    return res.get('aggregations').get('2').get('buckets', [])

def get_addresses_subfields(from_date, to_date,X,Y, item):
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
                        {"match_phrase": {"match_type.keyword": {"query": "PerfectMatch"}}},
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
    "2": {
      "range": {
        "field": "insights.response.fill.addresses.summary."+str(item),
        "ranges": [
          {
            "from": 0,
            "to": 1
          },
          {
            "from": 1,
            "to": 1000
          }
        ],
        "keyed": "true"
      }
    }
  },

        },
    }
    
)              

    return res.get('aggregations').get('2').get('buckets', [])

def get_social(from_date, to_date,X,Y,item):
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
                        {"match_phrase": {"match_type.keyword": {"query": "PerfectMatch"}}},
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
    "2": {
      "range": {
        "field": "insights.response.fill.social_profiles.summary."+str(item),
        "ranges": [
          {
            "from": 1,
            "to": 2
          },
          {
            "from": 2,
            "to": 1000
          }
        ],
        "keyed": "true"
      }
    }
  },

        },
    }
    
)              
    
    return res.get('aggregations').get('2').get('buckets', [])
    
    
def datamonthbymonth(from_year,to_year,from_month,to_month,from_day,to_day,savefilename):
    result=['Country','Total','with phones','without phones','phones%','with emails','without emails','emails%','with jobs','without jobs','jobs%','with educations','without educations','educations%','with dobs','without dobs','dobs%','with addresses','without addresses','addresses%','country','state','city','street','house','1-CPF','multi-CPF','CPF%','1-badoo','multi-badoo','badoo%','1-bebo','multi-bebo','bebo%','1-ebay','multi-ebay','ebay%','1-facebook','multi-facebook','facebook%','1-flickr','multi-flickr','flickr%','1-flixster','multi-flixster','flixster%','1-foursquare','multi-foursquare','foursquare%','1-friendster','multi-friendster','friendster%','1-google','multi-google','google%','1-gravatar','multi-gravatar','gravatar%','1-hi5','multi-hi5','hi5%','1-instagram','multi-instagram','instagram%','1-linkedin','multi-linkedin','linkedin%','1-meetup','multi-meetup','meetup%','1-myyearbook','multi-myyearbook','myyearbook%','1-netlog','multi-netlog','netlog%','1-ning','multi-ning','ning%','1-pinterest','multi-pinterest','pinterest%','1-sonico','multi-sonico','sonico%','1-soundcloud','multi-soundcloud','soundcloud%','1-tagged','multi-tagged','tagged%','1-twitter','multi-twitter','twitter%','1-vkontakte','multi-vkontakte','vkontakte%','1-youtube','multi-youtube','youtube%']
    with open('/opt/eval/ds_evaluation/API_insights_SALES'+str(savefilename)+'.csv', 'wb') as f:
        writer = csv.writer(f)
        writer.writerow(result)
        with open("/opt/eval/ds_evaluation/elasticsearch/CountryVsPhone.csv") as csvfile:
            spamreader = csv.reader(csvfile, delimiter=',', quotechar='|')
            kk = 2
            for row in spamreader:
                res=[]
                country = row[0]
                if country != "OverAll":
                    country_code = row[1]
                    print(country)
                    #print(country_code)

                    res.append(country)
                    total_query = get_total_queries(datetime.now().replace(year=from_year, month=from_month, day=from_day), datetime.now().replace(year=to_year, month=to_month, day=to_day), int(country_code),country)
                    
                    res.append(total_query)
                    
                    fields = ['phones','emails','jobs','educations','dobs','addresses']
                    for field in fields:
                        res_field = get_fill_rate_subfields(datetime.now().replace(year=from_year, month=from_month, day=from_day), datetime.now().replace(year=to_year, month=to_month, day=to_day),int(country_code),country,field)
                        #print(sub,res_sub[u'0.0-1.0'][u'doc_count'],res_sub[u'1.0-1000.0'][u'doc_count'])
                        res.append(res_field[u'0.0-1.0'][u'doc_count'])
                        res.append(res_field[u'1.0-1000.0'][u'doc_count'])
                        if total_query == 0:
                            res.append('0')
                        else: 
                            res.append(str(res_field[u'1.0-1000.0'][u'doc_count']*1.0/total_query*100)+'%')
                          

                    subfields = ['country','state','city','street','house']
                    for sub in subfields:
                        res_sub = get_addresses_subfields(datetime.now().replace(year=from_year, month=from_month, day=from_day), datetime.now().replace(year=to_year, month=to_month, day=to_day),int(country_code),country,sub)
                        #print(sub,res_sub[u'0.0-1.0'][u'doc_count'],res_sub[u'1.0-1000.0'][u'doc_count'])
                        result.append(res_sub[u'0.0-1.0'][u'doc_count'])
                        result.append(res_sub[u'1.0-1000.0'][u'doc_count'])
                        if total_query == 0:
                            res.append('0')
                        else: 
                            res.append(str(res_sub[u'1.0-1000.0'][u'doc_count']*1.0/total_query*100)+'%')
                        
                    social=['CPF','badoo','bebo','ebay','facebook','flickr','flixster','foursquare','friendster','google','gravatar','hi5','instagram','linkedin','meetup','myyearbook','netlog','ning','pinterest','sonico','soundcloud','tagged','twitter','vkontakte','youtube']
                    for social_page in social:
                        res_social = get_social(datetime.now().replace(year=from_year, month=from_month, day=from_day), datetime.now().replace(year=to_year, month=to_month, day=to_day), int(country_code),country,social_page)
                        
                        #print(social_page,res_social[u'1.0-2.0'][u'doc_count'],res_social[u'2.0-1000.0'][u'doc_count'])
                        res.append(res_social[u'1.0-2.0'][u'doc_count'])
                        res.append(res_social[u'2.0-1000.0'][u'doc_count'])
                        social_count = res_social[u'1.0-2.0'][u'doc_count']+res_social[u'2.0-1000.0'][u'doc_count']
                        if total_query == 0:
                            res.append('0')
                        else:
                            res.append(str(social_count*1.0/total_query*100)+'%')
                else:
                    res.append('OverAll')
                    total_query = get_total_queries_overall(datetime.now().replace(year=from_year, month=from_month, day=from_day), datetime.now().replace(year=to_year, month=to_month, day=to_day))
                    res.append(total_query)
                    
                    fields = ['phones','emails','jobs','educations','dobs','addresses']
                    for field in fields:
                        res_field = get_fill_rate_subfields_overall(datetime.now().replace(year=from_year, month=from_month, day=from_day), datetime.now().replace(year=to_year, month=to_month, day=to_day),field)
                        #print(sub,res_sub[u'0.0-1.0'][u'doc_count'],res_sub[u'1.0-1000.0'][u'doc_count'])
                        res.append(res_field[u'0.0-1.0'][u'doc_count'])
                        res.append(res_field[u'1.0-1000.0'][u'doc_count'])
                        if total_query == 0:
                            res.append('0')
                        else: 
                            res.append(str(res_field[u'1.0-1000.0'][u'doc_count']*1.0/total_query*100)+'%')

                    subfields = ['country','state','city','street','house']
                    for sub in subfields:
                        res_sub = get_addresses_subfields_overall(datetime.now().replace(year=from_year, month=from_month, day=from_day), datetime.now().replace(year=to_year, month=to_month, day=to_day),sub)
                        #print(sub,res_sub[u'0.0-1.0'][u'doc_count'],res_sub[u'1.0-1000.0'][u'doc_count'])
                        result.append(res_sub[u'0.0-1.0'][u'doc_count'])
                        result.append(res_sub[u'1.0-1000.0'][u'doc_count'])
                        if total_query == 0:
                            res.append('0')
                        else: 
                            res.append(str(res_sub[u'1.0-1000.0'][u'doc_count']*1.0/total_query*100)+'%')
                           
                    social=['CPF','badoo','bebo','ebay','facebook','flickr','flixster','foursquare','friendster','google','gravatar','hi5','instagram','linkedin','meetup','myyearbook','netlog','ning','pinterest','sonico','soundcloud','tagged','twitter','vkontakte','youtube']
                    for social_page in social:
                        res_social = get_social_overall(datetime.now().replace(year=from_year, month=from_month, day=from_day), datetime.now().replace(year=to_year, month=to_month, day=to_day),social_page)
                        
                        #print(social_page,res_social[u'1.0-2.0'][u'doc_count'],res_social[u'2.0-1000.0'][u'doc_count'])
                        res.append(res_social[u'1.0-2.0'][u'doc_count'])
                        res.append(res_social[u'2.0-1000.0'][u'doc_count'])
                        social_count = res_social[u'1.0-2.0'][u'doc_count']+res_social[u'2.0-1000.0'][u'doc_count']
                        if total_query == 0:
                            res.append('0')
                        else:
                            res.append(str(social_count*1.0/total_query*100)+'%')
                kk = kk + 1 

                writer.writerow(res)


    df = pd.read_csv('/opt/eval/ds_evaluation/API_insights_SALES'+str(savefilename)+'.csv',usecols=['Country','Total','phones%','emails%','jobs%','educations%','dobs%','addresses%','country','state','city','street','house','CPF%','badoo%','bebo%','ebay%','facebook%','flickr%','flixster%','foursquare%','friendster%','google%','gravatar%','hi5%','instagram%','linkedin%','meetup%','myyearbook%','netlog%','ning%','pinterest%','sonico%','soundcloud%','tagged%','twitter%','vkontakte%','youtube%'])
    
    df.to_csv('/opt/eval/ds_evaluation/API_insights_SALES'+str(savefilename)+'.csv',columns=['Country','Total','phones%','emails%','jobs%','educations%','dobs%','addresses%','country','state','city','street','house','facebook%','linkedin%','twitter%','pinterest%','instagram%','google%','youtube%','CPF%','badoo%','bebo%','ebay%','flickr%','flixster%','foursquare%','friendster%','gravatar%','hi5%','meetup%','myyearbook%','netlog%','ning%','sonico%','soundcloud%','tagged%','vkontakte%'],index=False)

days = [25,1]
months = [1,2]
years = [2021,2021]

#days = [1,8,15,22,29,6,13,20,27,3,10,17,24,31,7,14,21,28,5,12,19,26,2,9,16,23,30,7,14,21,28,4,11,18,25]
#months = [6,6,6,6,6,7,7,7,7,8,8,8,8,8,9,9,9,9,10,10,10,10,11,11,11,11,11,12,12,12,12,1,1,1,1]
#years = [2020,2020,2020,2020,2020,2020,2020,2020,2020,2020,2020,2020,2020,2020,2020,2020,2020,2020,2020,2020,2020,2020,2020,2020,2020,2020,2020,2020,2020,2020,2020,2021,2021,2021,2021]



savefilename = 1
for ii in range(len(months)-1):
    from_year = years[ii]
    from_month = months[ii]
    from_day = days[ii]
    
    to_month = months[ii+1]
    to_year = years[ii+1]
    to_day = days[ii+1]

    

    datamonthbymonth(from_year,to_year,from_month,to_month,from_day,to_day,savefilename)
    print(savefilename)
    savefilename = savefilename + 1

