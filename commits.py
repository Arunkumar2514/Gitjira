import requests
import psycopg2
from datetime import datetime
from typing import List, Dict
from datetime import datetime, timezone
import time
import configparser
from pathlib import Path

# Configuration

def load_config():
    #Loading configuration from .github_analyzer_config file
    config = configparser.ConfigParser()
    project_config = Path('.github_analyzer_config')
    if project_config.exists():
        config.read(project_config)
        return config

    raise FileNotFoundError("No .github_analyzer_config found in project root or home directory")
try:
        config = load_config()
        github_config = {
                'token': config.get('GITHUB', 'TOKEN'),
                'repo': config.get('GITHUB', 'REPO'),
                'owner': config.get('GITHUB', 'OWNER')
            }
        jira_config = {
                'base_url': config.get('JIRA', 'URL'),
                'email': config.get('JIRA', 'EMAIL'),
                'token': config.get('JIRA', 'TOKEN')
            }
        
        DB_CONFIG = {
            'dbname': config.get('DATABASE', 'NAME'),
            'user': config.get('DATABASE', 'USER'),
            'password': config.get('DATABASE', 'PASSWORD'),
            'host': config.get('DATABASE', 'HOST'),
            'port': config.get('DATABASE', 'PORT')
        }
except Exception as e:
        print(f"Config error: {e}")
        exit(1)
JIRA_API_URL = f"{jira_config['base_url']}/rest/api/3"
JIRA_EMAIL = jira_config["email"]
JIRA_API_TOKEN = jira_config["token"]
GITHUB_API_URL = f"https://api.github.com/repos/{github_config['owner']}/{github_config['repo']}"
GITHUB_TOKEN=github_config["token"]


def fetch_jira_issues(project: str = "GJA", max_results: int = 1000) -> List[str]:
    """Fetch all Jira issue keys for a project (e.g., GJA-123)."""
    url = f"{JIRA_API_URL}/search"
    headers = {
        "Accept": "application/json",
        "Content-Type": "application/json"
    }
    auth = (JIRA_EMAIL, JIRA_API_TOKEN)
    query = {
        "jql": f'project = "{project}" ORDER BY created DESC',  
        "maxResults": max_results,
        "fields": ["key"]
    }
    
    try:
        response = requests.post(url, headers=headers, auth=auth, json=query)
        response.raise_for_status()
        return [issue["key"] for issue in response.json()["issues"]]
    except Exception as e:
        print(f"Error fetching Jira issues: {e}")
        print(f"Response: {response.text}")  # Debug: Print full error
        return []

def fetch_jira_transitions(jira_key: str) -> List[Dict]:
    """Fetch stage transitions (e.g., TO DO → IN PROGRESS) for a Jira issue."""
    url = f"{JIRA_API_URL}/issue/{jira_key}/changelog"
    headers = {"Accept": "application/json"}
    auth = (JIRA_EMAIL, JIRA_API_TOKEN)
    
    try:
        response = requests.get(url, headers=headers, auth=auth)
        response.raise_for_status()
        transitions = []
        for history in response.json()["values"]:
            for item in history["items"]:
                if item["field"] == "status":
                    transitions.append({
                        "to_stage": item["toString"],
                        "transition_date": datetime.strptime(
                            history["created"], "%Y-%m-%dT%H:%M:%S.%f%z"
                        )
                    })
        return transitions
    except Exception as e:
        print(f"Error fetching transitions for {jira_key}: {e}")
        return []
    
def get_all_branches() -> List[str]:
    """Fetch all branches in the repository."""
    headers = {
        "Authorization": f"token {GITHUB_TOKEN}",
        "Accept": "application/vnd.github.v3+json"
    }
    branches = []
    page = 1
    
    while True:
        try:
            url = f"{GITHUB_API_URL}/branches?per_page=100&page={page}"
            response = requests.get(url, headers=headers)
            
            # Handle rate limiting
            if response.status_code == 403:
                reset_time = int(response.headers.get('X-RateLimit-Reset', time.time() + 60))
                sleep_time = max(reset_time - time.time(), 0) + 5
                print(f"Rate limit exceeded. Sleeping for {sleep_time} seconds...")
                time.sleep(sleep_time)
                continue
            
            response.raise_for_status()
            data = response.json()
            if not data:
                break
                
            branches.extend(branch["name"] for branch in data)
            if len(data) < 100:
                break
            page += 1
            
        except Exception as e:
            print(f"Error fetching branches: {e}")
            break
    
    return branches

def get_commits_for_jira_key(jira_key: str) -> List[Dict]:
    """Fetch commits linked to a Jira key from all branches."""
    headers = {
        "Authorization": f"token {GITHUB_TOKEN}",
        "Accept": "application/vnd.github.v3+json"
    }
    commits = []
    branches = get_all_branches()
    
    for branch in branches:
        page = 1
        while True:
            try:
                url = f"{GITHUB_API_URL}/commits"
                params = {
                    "per_page": 100,
                    "page": page,
                    "sha": branch,
                    "commit_message": jira_key
                }
                response = requests.get(url, headers=headers, params=params)
                
                # Handle rate limiting
                if response.status_code == 403:
                    reset_time = int(response.headers.get('X-RateLimit-Reset', time.time() + 60))
                    sleep_time = max(reset_time - time.time(), 0) + 5
                    print(f"Rate limit exceeded. Sleeping for {sleep_time} seconds...")
                    time.sleep(sleep_time)
                    continue
                
                response.raise_for_status()
                data = response.json()
                if not data:
                    break
                
                for commit in data:
                    if jira_key.lower() in commit["commit"]["message"].lower():
                        commits.append({
                            "sha": commit["sha"],
                            "date": datetime.strptime(
                                commit["commit"]["author"]["date"],
                                "%Y-%m-%dT%H:%M:%SZ"
                            ).replace(tzinfo=timezone.utc)
                        })
                
                if "next" not in response.links:
                    break
                page += 1
                
            except Exception as e:
                print(f"Error fetching commits from branch {branch}: {e}")
                break
    
    return commits

def map_commits_to_stages(jira_key: str) -> List[Dict]:
    """Map commits to stages based on transition timestamps."""
    transitions = fetch_jira_transitions(jira_key)
    commits = get_commits_for_jira_key(jira_key)
    
    if not commits:
        return []
    
    transitions.sort(key=lambda x: x["transition_date"])
    commits.sort(key=lambda x: x["date"])
    
    stage_commits = []
    current_stage = "TO DO"
    
    for commit in commits:
        commit_date = commit["date"]
        for transition in transitions:
            transition_date = transition["transition_date"]
            if commit_date >= transition_date:
                current_stage = transition["to_stage"]
        
        stage_commits.append({
            "jira_key": jira_key,
            "commit_sha": commit["sha"],
            "commit_date": commit_date,
            "stage": current_stage
        })
    
    return stage_commits

def save_to_postgres(stage_commits: List[Dict]):
    """Save stage-commit mappings to PostgreSQL."""
    conn = psycopg2.connect(**DB_CONFIG)
    cursor = conn.cursor()
    
    for entry in stage_commits:
        cursor.execute("""
            INSERT INTO jira_commit_stages 
            (jira_key, commit_sha, commit_date, stage)
            VALUES (%s, %s, %s, %s)
            ON CONFLICT (jira_key, commit_sha) DO NOTHING
        """, (
            entry["jira_key"],
            entry["commit_sha"],
            entry["commit_date"],
            entry["stage"]
        ))
    
    conn.commit()
    cursor.close()
    conn.close()

def main():
    """Orchestrate the workflow."""
    jira_keys = fetch_jira_issues("GJA")
    print(f"Found {len(jira_keys)} Jira issues to process.")
    
    for key in jira_keys:
        print(f"Processing {key}...")
        stage_commits = map_commits_to_stages(key)
        if stage_commits:
            save_to_postgres(stage_commits)
    
    print("Data saved to PostgreSQL!")

if __name__ == "__main__":
    main()