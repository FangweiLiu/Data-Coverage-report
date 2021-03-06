from elasticsearch import Elasticsearch
from datetime import datetime
import csv
import pandas as pd
from elasticsearch import RequestsHttpConnection

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
            "stored_fields": ["*"],
            "body": {
                "query": {
                    "bool": {
                        "must": [],
                        "filter": [
                        {"match_all": {}},
                        {"match_phrase": {"search_pointer": "false"}},
                        #{"match_phrase": {"api.IsSuggestedSearch": "false"}},
                        {"match_phrase": {"match_type.keyword": {"query": "PerfectMatch"}}},
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
        index="search-requests-v1-pro-*",
        **{
            "size": 0,
            "body": {
                "query": {
                    "bool": {
                        "must": [],
                        "filter": [
                        {"match_all": {}},
                        {"match_phrase": {"search_pointer": "false"}},
                        #{"match_phrase": {"api.IsSuggestedSearch": "false"}},
                        {"match_phrase": {"match_type.keyword": {"query": "PerfectMatch"}}},
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
        index="search-requests-v1-pro-*",
        **{
            "size": 0,
            "body": {
                "query": {
                    "bool": {
                        "must": [],
                        "filter": [
                        {"match_all": {}},
                        {"match_phrase": {"search_pointer": "false"}},
                        #{"match_phrase": {"api.IsSuggestedSearch": "false"}},
                        {"match_phrase": {"match_type.keyword": {"query": "PerfectMatch"}}},
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
        index="search-requests-v1-pro-*",
        **{
            "size": 0,
            "body": {
                "query": {
                    "bool": {
                        "must": [],
                        "filter": [
                        {"match_all": {}},
                        {"match_phrase": {"search_pointer": "false"}},
                        #{"match_phrase": {"api.IsSuggestedSearch": "false"}},
                        {"match_phrase": {"match_type.keyword": {"query": "PerfectMatch"}}},
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
    with open('/opt/eval/ds_evaluation/PRO_insights_SALES'+str(savefilename)+'.csv', 'wb') as f:
        writer = csv.writer(f)
        writer.writerow(result)
        with open("/opt/eval/ds_evaluation/elasticsearch/CountryVsPhone.csv") as csvfile:
            spamreader = csv.reader(csvfile, delimiter=',', quotechar='|')
            kk = 2
            for row in spamreader:
                country = row[0]
                res=[]
                if country != "OverAll":
                    pass
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


    df = pd.read_csv('/opt/eval/ds_evaluation/PRO_insights_SALES'+str(savefilename)+'.csv',usecols=['Country','Total','phones%','emails%','jobs%','educations%','dobs%','addresses%','country','state','city','street','house','CPF%','badoo%','bebo%','ebay%','facebook%','flickr%','flixster%','foursquare%','friendster%','google%','gravatar%','hi5%','instagram%','linkedin%','meetup%','myyearbook%','netlog%','ning%','pinterest%','sonico%','soundcloud%','tagged%','twitter%','vkontakte%','youtube%'])
    
    df.to_csv('/opt/eval/ds_evaluation/PRO_insights_SALES_'+str(savefilename)+'.csv',columns=['Country','Total','phones%','emails%','jobs%','educations%','dobs%','addresses%','country','state','city','street','house','facebook%','linkedin%','twitter%','pinterest%','instagram%','google%','youtube%','CPF%','badoo%','bebo%','ebay%','flickr%','flixster%','foursquare%','friendster%','gravatar%','hi5%','meetup%','myyearbook%','netlog%','ning%','sonico%','soundcloud%','tagged%','vkontakte%'],index=False)

##days = [25,1]
##months = [1,2]
##years = [2021,2021]

days = [3,10,17,24,31,7,14,21,28,5,12,19,26,2,9,16,23,30,7,14,21,28,4,11,18,25]
months = [8,8,8,8,8,9,9,9,9,10,10,10,10,11,11,11,11,11,12,12,12,12,1,1,1,1]
years = [2020,2020,2020,2020,2020,2020,2020,2020,2020,2020,2020,2020,2020,2020,2020,2020,2020,2020,2020,2020,2020,2020,2021,2021,2021,2021]



savefilename = 1
for ii in range(len(months)-1):
    from_year = years[ii]
    from_month = months[ii]
    from_day = days[ii]
    
    to_month = months[ii+1]
    to_year = years[ii+1]
    to_day = days[ii+1]
    
    datamonthbymonth(from_year,to_year,from_month,to_month,from_day,to_day,savefilename)
    print from_year,to_year,from_month,to_month,from_day,to_day
    print(savefilename)
    savefilename = savefilename + 1

