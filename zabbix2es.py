import os,time
import traceback
from function import invoke
from elasticsearch import Elasticsearch
from elastic_transport import RequestsHttpNode
import asyncio
from elasticsearch import AsyncElasticsearch
from elasticsearch.helpers import async_bulk

async def gendata(response,i):
    for item in response["result"]:
        item["field_type"] = str(i)
#        print(item)
        yield {
            "_op_type": "create",
            "_index": "zabbix-history",
            "pipeline": "zabbix",
            "_source": item
        }

async def main():
    server = "http://10.146.0.25"
    for i in range(5):
        now = int(time.time())
        request = {
            "jsonrpc": "2.0",
                "method": "history.get",
                "params": {
                    "history": i,
#                    "limit": 150000,
                    "time_from": now-60,
#                     "time_till": now-14400,
#                    "hostids": ["10561"]
                    "hostids": ["10560","10084","10561"]
                },
            "id": 2,
            "auth": "<token>"
        }
        es = AsyncElasticsearch("https://kspm.es.asia-northeast1.gcp.cloud.es.io:9243",basic_auth=("elastic","<token>"))
    #    print(es.info())
        try:
            response = {}
            if invoke(server, request, response):
                print(len(response["result"]))
                await async_bulk(es,gendata(response,i))
                await es.close()

            else:
                print("{}: error: message={}, data={}, code={}".format(
                    os.path.basename(__file__),
                    response["error"]["message"],
                    response["error"]["data"],
                    response["error"]["code"])
                )
    
        except Exception as e:
            print("{}: exception: {}".format(
                os.path.basename(__file__),
                traceback.format_exc())
            )
 
if __name__ == "__main__":
    loop = asyncio.get_event_loop()
    loop.run_until_complete(main())
