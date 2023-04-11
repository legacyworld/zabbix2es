import json
import urllib.request
 
def invoke(server, parameter, content):
    result = False
    url = server + "/api_jsonrpc.php"
#    print(url)
    request = urllib.request.Request(
        url = url,
        data = json.dumps(parameter).encode(),
        headers = {"Content-Type": "application/json-rpc"}
    )
 
    try:
        with urllib.request.urlopen(request) as response:
            dictionary = json.loads(response.read())
            if "result" in dictionary:
                content["result"] = dictionary["result"]
                result = True
 
            else:
                content["error"] = dictionary["error"]
 
    except:
        raise
 
    return result