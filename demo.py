
from demo1 import  list_projects
from requests.auth import HTTPBasicAuth
auth = HTTPBasicAuth(EMAIL, API_TOKEN)
headers = {
    "Accept": "application/json"
}

result=list_projects(JIRA_BASE_URL=BASE_URL,auth=auth,headers=headers)

print(result)
