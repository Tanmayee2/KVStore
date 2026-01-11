import sys
import requests

def usage():
    print("GET : python client.py <address> <key>")
    print("PUT : python client.py <address> <key> <value>")
    print("Example: python client.py http://127.0.0.1:5000 name Tanmayee")
    sys.exit(1)

if len(sys.argv) < 3:
    usage()

addr = sys.argv[1]
key  = sys.argv[2]

if len(sys.argv) == 3:
    # GET
    r = requests.get(
        url=addr + "/request",
        json={"payload": {"key": key}},
        timeout=2,
    )
    print(r.json())

elif len(sys.argv) == 4:
    # PUT
    value = sys.argv[3]
    r = requests.put(
        url=addr + "/request",
        json={"payload": {"key": key, "value": value}},
        timeout=2,
    )
    print(r.json())

else:
    usage()