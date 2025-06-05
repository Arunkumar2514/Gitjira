import requests
from typing import List, Dict, Optional, Any
import re
import asyncio
import configparser
from pathlib import Path
import psycopg2
from requests.auth import HTTPBasicAuth



def load_config():
    config = configparser.ConfigParser()
    project_config = Path('.github_analyzer_config')
    if project_config.exists():
        config.read(project_config)
        return config

    raise FileNotFoundError("No .github_analyzer_config found in project root or home directory")
try:
        config = load_config()
        GITHUB_TOKEN = config.get('GITHUB', 'TOKEN')
        DB_CONFIG = {
            'dbname': config.get('DATABASE', 'NAME'),
            'user': config.get('DATABASE', 'USER'),
            'password': config.get('DATABASE', 'PASSWORD'),
            'host': config.get('DATABASE', 'HOST'),
            'port': config.get('DATABASE', 'PORT')
        }
        jira_config = {
                'base_url': config.get('JIRA', 'URL'),
                'email': config.get('JIRA', 'EMAIL'),
                'token': config.get('JIRA', 'TOKEN')
            }
except Exception as e:
        print(f"Config error: {e}")
        exit(1)


BASE_URL = 'https://api.github.com'
HEADERS = {
    "Authorization": f"token {GITHUB_TOKEN}",
    "Accept": "application/vnd.github.v3+json"
}
JIRA_EMAIL = jira_config.get('email')
JIRA_API_TOKEN = jira_config.get('token')
JIRA_BASE_URL = jira_config.get('base_url')
TEAM_FIELD_ID = "customfield_10001"
VENDOR_FIELD_ID = "customfield_10058"
SPRINT_FIELD_ID = "customfield_10020"

auth = HTTPBasicAuth(JIRA_EMAIL, JIRA_API_TOKEN)
headers = {"Accept": "application/json"}

def create_db_connection():
        try:
            return psycopg2.connect(**DB_CONFIG)
        except psycopg2.Error as e:
            print(f"Database connection failed: {e}")
            return None

def extract_jira_issue_keys(message: str) -> List[str]:
        """Extract JIRA issue keys from commit message (e.g., PROJ-123)"""
        if not message:
            return []
        # Match patterns like PROJ-123, PROJECT-456, etc.
        return re.findall(r'([A-Z][A-Z0-9]*-[0-9]+)', message.upper())
    
class JiraProcessor:
    def __init__(self, db_conn=None):
        self.db_conn = db_conn or create_db_connection()
        self.github_jira_mapping = {}
        self.issue_cache = {}
        self._load_login_mappings()  # Load existing mappings at initialization

    def _load_login_mappings(self):
        """Load existing login mappings from database into memory"""
        with self.db_conn.cursor() as cursor:
            cursor.execute("SELECT login, username FROM user_login_mappings")
            for login, username in cursor:
                self.github_jira_mapping[login.lower()] = username


    def populate_commit_contributors_from_commits(self):
        """Populate commit_contributors table with GitHub logins and map to JIRA users"""
        with self.db_conn.cursor() as cursor:
            # First insert new logins that don't exist yet
            cursor.execute("""
                INSERT INTO commit_contributors (login)
                SELECT DISTINCT TRIM(author)
                FROM commit_history
                WHERE author IS NOT NULL
                AND LOWER(TRIM(author)) NOT IN (
                    SELECT LOWER(TRIM(login)) FROM commit_contributors
                )
                RETURNING login
            """)
            new_logins = [row[0] for row in cursor.fetchall()]
            
            # For new logins, try to find mappings
            for login in new_logins:
                normalized_login = login.lower()
                if normalized_login in self.github_jira_mapping:
                    team_member = self.github_jira_mapping[normalized_login]
                    cursor.execute("""
                        UPDATE commit_contributors
                        SET team_member = %s
                        WHERE login = %s
                    """, (team_member, login))
            
            self.db_conn.commit()

    def _update_login_mappings(self, cursor):
        """Update the login mappings table with new mappings"""
        for login, team_member in self.github_jira_mapping.items():
            if not login or not team_member:
                continue
                
            cursor.execute("""
                INSERT INTO user_login_mappings (login, username)
                VALUES (%s, %s)
                ON CONFLICT (login) DO UPDATE
                SET username = EXCLUDED.username
            """, (login.lower(), team_member))
            
            # Also update commit_contributors for this login
            cursor.execute("""
                UPDATE commit_contributors
                SET team_member = %s
                WHERE login = %s
                AND (team_member IS NULL OR team_member != %s)
            """, (team_member, login, team_member))

    async def process_commit_jira_links(self):
        """Process commits that reference JIRA issues to find user mappings"""
        with self.db_conn.cursor() as cursor:
            cursor.execute("""
                SELECT ch.sha, ch.commit_message, cc.login, cc.team_member
                FROM commit_history ch
                JOIN commit_contributors cc ON ch.author = cc.login
                WHERE ch.commit_message ~ '[A-Z][A-Z0-9]*-[0-9]+'
                  AND (cc.team_member IS NULL OR cc.team_member = 'Unknown')
            """)

            commits = cursor.fetchall()
            for sha, message, login, _ in commits:
                # First check if we already have a mapping for this login
                normalized_login = login.lower()
                if normalized_login in self.github_jira_mapping:
                    team_member = self.github_jira_mapping[normalized_login]
                    cursor.execute("""
                        UPDATE commit_contributors
                        SET team_member = %s
                        WHERE login = %s
                    """, (team_member, login))
                    continue
                
                # If no mapping exists, try to find one via JIRA issues
                for issue_key in extract_jira_issue_keys(message):
                    issue = await self._fetch_jira_issue(issue_key)
                    if not issue:
                        continue

                    assignee = issue.get("fields", {}).get("assignee")
                    if not assignee:
                        continue

                    assignee_name = assignee.get("displayName")
                    assignee_email = assignee.get("emailAddress")
                    team = self._get_field_value(issue["fields"], TEAM_FIELD_ID)
                    vendor = self._get_field_value(issue["fields"], VENDOR_FIELD_ID)

                    if assignee_name:
                        # Store the new mapping
                        self.github_jira_mapping[normalized_login] = assignee_name
                        cursor.execute("""
                            UPDATE commit_contributors
                            SET team_member = %s,
                                team = COALESCE(%s, team),
                                vendor = COALESCE(%s, vendor)
                            WHERE login = %s
                        """, (assignee_name, team, vendor, login))
                        break

            # Update all mappings at once
            self._update_login_mappings(cursor)
            self.db_conn.commit()
    
    async def _fetch_all_jira_issues(self) -> List[Dict[str, Any]]:
        all_issues = []
        start_at = 0
        max_results = 100

        while True:
            try:
                response = requests.get(
                    f"{JIRA_BASE_URL}/rest/api/3/search",
                    headers=headers,
                    auth=auth,
                    params={
                        "maxResults": max_results,
                        "startAt": start_at,
                        "fields": f"{TEAM_FIELD_ID},{VENDOR_FIELD_ID},{SPRINT_FIELD_ID},status,assignee,reporter"
                    },
                    timeout=30
                )
                response.raise_for_status()
                data = response.json()

                issues = data.get("issues", [])
                if not issues:
                    break

                all_issues.extend(issues)
                start_at += max_results

                if start_at >= data.get("total", 0):
                    break

            except requests.RequestException as e:
                print(f"Failed fetching JIRA issues: {e}")
                break

        return all_issues

    async def _fetch_jira_issue(self, issue_key: str) -> Optional[Dict[str, Any]]:
        if issue_key in self.issue_cache:
            return self.issue_cache[issue_key]

        try:
            response = requests.get(
                f"{JIRA_BASE_URL}/rest/api/3/issue/{issue_key}",
                headers=headers,
                auth=auth,
                timeout=30
            )
            response.raise_for_status()
            issue = response.json()
            self.issue_cache[issue_key] = issue
            return issue

        except requests.HTTPError as e:
            if response.status_code == 404:
                print(f"JIRA issue {issue_key} not found (404). Skipping.")
            else:
                print(f"Error fetching JIRA issue {issue_key}: {e}")
            self.issue_cache[issue_key] = None  # Prevent retry
            return None

    def _get_field_value(self, fields: Dict, field_id: str) -> Optional[str]:
        if not fields or field_id not in fields:
            return None

        value = fields[field_id]
        if isinstance(value, dict):
            return value.get("value") or value.get("name")
        elif isinstance(value, list):
            return ", ".join(
                item.get("value") or item.get("name") or str(item)
                for item in value if isinstance(item, dict)
            )
        return str(value) if value else None
    
    def extract_status(self,issue):
        try:
            return issue["fields"]["status"]["name"]
        except (KeyError, TypeError) as e:
            print(f"⚠️ Status field missing in issue {issue.get('key')}: {e}")
            return "Unknown"
    
    async def process_jira_issues(self) -> List[Dict[str, Any]]:
        all_issues = await self._fetch_all_jira_issues()
        if not all_issues:
            print("No JIRA issues found to process")
            return []

        with self.db_conn.cursor() as cursor:
            # Fetch existing entries for comparison
            cursor.execute("""
                SELECT jira_key, status, commit_sha FROM jira_issue_commits
            """)
            existing_entries = cursor.fetchall()
            existing_map = {(row[0], row[2]): row[1] for row in existing_entries}  # {(jira_key, commit_sha): status}

            # Get all existing Jira keys in the database
            cursor.execute("SELECT DISTINCT jira_key FROM jira_issue_commits")
            existing_jira_keys = {row[0] for row in cursor.fetchall()}

            # Get existing commit associations from commit_jira
            cursor.execute("SELECT jira_key, sha FROM commit_jira")
            commit_jira_pairs = cursor.fetchall()
            jira_commits = {}
            for jira_key, sha in commit_jira_pairs:
                if jira_key not in jira_commits:
                    jira_commits[jira_key] = set()
                jira_commits[jira_key].add(sha)

            inserts = 0
            updates = 0

            for issue in all_issues:
                fields = issue.get("fields", {})
                key = issue.get("key")
                sprints = fields.get(SPRINT_FIELD_ID, [])
                name = sprints[0].get("name") if isinstance(sprints, list) and sprints else None
                status = self.extract_status(issue)
                commits = jira_commits.get(key, [None])  # None represents cases with no commit

                # Check if this is a new Jira key (regardless of commits)
                is_new_jira_key = key not in existing_jira_keys

                for commit_sha in commits:
                    entry_key = (key, commit_sha)
                    existing_status = existing_map.get(entry_key)

                    # Determine if we need to insert/update
                    if is_new_jira_key:
                        # Case 1: New Jira key - insert all its commits
                        should_insert = True
                    elif existing_status is None:
                        # Case 2: New commit for existing Jira key
                        should_insert = True
                    elif existing_status != status:
                        # Case 3: Status changed for existing Jira key/commit
                        should_insert = True
                    else:
                        # No change needed
                        continue
                    if should_insert:
                        try:
                            cursor.execute("""
                                INSERT INTO jira_issue_commits (sprint, jira_key, status, commit_sha)
                                VALUES (%s, %s, %s, %s)
                                ON CONFLICT (commit_sha) DO UPDATE
                                SET status = EXCLUDED.status
                                WHERE jira_issue_commits.status != EXCLUDED.status
                            """, (name, key, status, commit_sha))
                            
                            if existing_status is None:
                                inserts += 1
                            else:
                                updates += 1
                        except psycopg2.Error as e:
                            print(f"Error inserting/updating {key} with commit {commit_sha}: {e}")
                            self.db_conn.rollback()

            self.db_conn.commit()
            print(f"✅ Processed JIRA issues: {inserts} inserts, {updates} updates.")
        return all_issues

async def async_main():
    """Async entry point"""
    print("\nEnriching with JIRA data...")
    jira_processor = JiraProcessor()
    try:
        jira_processor.populate_commit_contributors_from_commits()
        
        print("\nProcessing JIRA issues...")
        await jira_processor.process_jira_issues()
        
        print("\nMapping commits to JIRA issues...")
        await jira_processor.process_commit_jira_links()
        
        print("✅ JIRA data processing complete")
    except Exception as e:
        print(f"❌ Error processing JIRA data: {e}")
                
def main():
    """Sync wrapper for async main"""
    asyncio.run(async_main())
if __name__ == "__main__":
    main()