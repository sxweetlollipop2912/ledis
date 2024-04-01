import requests

URL = "http://127.0.0.1:8080/"

while True:
    query = input("ledis> ")
    try:
        response = requests.post(URL, data=query)

        if response.status_code == 200:
            print(response.text)
        else:
            print(f"Error {response.status_code}: {response.text}")
    except requests.exceptions.ConnectionError:
        print(f"Connection error")
