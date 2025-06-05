
from demo1 import  list_projects
from requests.auth import HTTPBasicAuth

BASE_URL = "https://aeroadithiyan3.atlassian.net"
EMAIL = "arunkumarp.ece.sec@gmail.com"
API_TOKEN = "ATATT3xFfGF0FgATwGbxxDIycyFbW3bzb31fdlevxUZCvdHerFV6FwfQ7FcRXh51KA2s5l_QKMfKRsr3-ALFf5m7Kic-qCb5k2ndUktRgvsPYMQTLp9dzZktfbg1o644mAW16dKtbMhIo8wrbNkZJuoemocIkxIcOiF-lsvfA5GNRkCaj_2orBo=1D399A12"
auth = HTTPBasicAuth(EMAIL, API_TOKEN)
headers = {
    "Accept": "application/json"
}

result=list_projects(JIRA_BASE_URL=BASE_URL,auth=auth,headers=headers)

print(result)