import requests
from datetime import datetime
from typing import List, Dict, Optional
import logging
import time
import sys
from logging import StreamHandler
import psycopg2
from psycopg2.extras import execute_batch
import time
import traceback
import configparser
from pathlib import Path

class SafeStreamHandler(StreamHandler):
    #A stream handler which is used to handle Unicode characters on Windows
    def emit(self, record):
        try:
            msg = self.format(record)
            if sys.stdout.encoding.lower() in ('cp1252', 'ansi'):
                msg = msg.encode('ascii', 'replace').decode('ascii')
            self.stream.write(msg + self.terminator)
            self.flush()
        except Exception:
            self.handleError(record)
# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        SafeStreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

def load_config() -> configparser.ConfigParser:
    #Load configuration from .github_analyzer_config file
    config = configparser.ConfigParser()
    config_path = Path('.github_analyzer_config')
    
    if config_path.exists():
            config.read(config_path)
            return config
    
    raise FileNotFoundError(
        "No .github_analyzer_config found in:\n" +
        "\n".join(f" - {p}" for p in config_path)
    )

class JiraGitHubIntegrator:
    def __init__(self):
        try:
            # Load configuration
            config = load_config()
            
            # GitHub configuration
            self.github_config = {
                'token': config.get('GITHUB', 'TOKEN'),
                'repo': config.get('GITHUB', 'REPO'),
                'owner': config.get('GITHUB', 'OWNER')
            }
            
            # Jira configuration
            self.jira_config = {
                'base_url': config.get('JIRA', 'URL'),
                'email': config.get('JIRA', 'EMAIL'),
                'token': config.get('JIRA', 'TOKEN')
            }
            
            # Database configuration (optional)
            self.db_config = None
            if config.has_section('DATABASE'):
                self.db_config = {
                    'database': config.get('DATABASE', 'NAME'),
                    'user': config.get('DATABASE', 'USER'),
                    'password': config.get('DATABASE', 'PASSWORD'),
                    'host': config.get('DATABASE', 'HOST'),
                    'port': config.get('DATABASE', 'PORT', fallback='5432')
                }
            self.jira_config['base_url'] = self.jira_config['base_url'].rstrip('/')
        
            self.jira_auth = (self.jira_config['email'], self.jira_config['token'])
            self.github_headers = {
            "Authorization": f"token {self.github_config['token']}",
            "Accept": "application/vnd.github+json",
            "User-Agent": "Jira-GitHub-Integrator"
        }
            
            # Initialize database if configured
            self.db_conn = None
            if self.db_config:
                self._init_db()
                
        except Exception as e:
            logger.error(f"Configuration error: {e}")
            raise
    
    def _init_db(self):
        #Initialize PostgreSQL database connection
        if not self.db_config:
            logger.warning("No database configuration provided")
            return False
        
        max_retries = 3
        for attempt in range(max_retries):
            try:
                # Close existing connection if it exists
                if hasattr(self, 'db_conn') and self.db_conn:
                    self.db_conn.close()
                    
                # Create new connection
                self.db_conn = psycopg2.connect(
                    host=self.db_config['host'],
                    database=self.db_config['database'],
                    user=self.db_config['user'],
                    password=self.db_config['password'],
                    connect_timeout=5
                )
                
                # Create a new cursor for verification
                cursor = self.db_conn.cursor()
                cursor.execute("SELECT 1")
                result = cursor.fetchone()
                cursor.close()
                
                #if result and result[0] == 1:
                    #logger.info("✅ Database connection successful")
                    
            except psycopg2.OperationalError as e:
                logger.warning(f"Attempt {attempt + 1} failed: {str(e)}")
                time.sleep(2)
            except Exception as e:
                logger.error(f"Unexpected error during connection: {e}")
                if hasattr(self, 'db_conn') and self.db_conn:
                    self.db_conn.close()
                time.sleep(2)
                logger.error("❌ Failed to establish database connection after retries")
        return False

    def verify_connections(self) -> bool:
        #Verify all connections (Jira, GitHub, and DB if configured)
        jira_ok = self._verify_jira_connection()
        github_ok = self._verify_github_connection()
        
        # database connection verification
        db_ok = True
        if self.db_config:
            if not self.db_conn or self.db_conn.closed != 0:
                logger.warning("Database connection not active, attempting to reconnect...")
                db_ok = self._init_db()
            else:
                try:
                    cursor = self.db_conn.cursor()
                    cursor.execute("SELECT 1")
                    cursor.close()
                    db_ok = True
                except:
                    db_ok = False
        
        if not db_ok and self.db_config:
            logger.error("Database connection verification failed")
        
        return jira_ok and github_ok and (True if not self.db_config else db_ok)

    def _verify_jira_connection(self) -> bool:
        #Verify Jira API connection
        try:
            url = f"{self.jira_config['base_url']}/rest/api/3/myself"
            response = requests.get(url, auth=self.jira_auth, timeout=10)
            if response.status_code == 200:
                logger.info("Jira connection successful")
                return True
            logger.error(f"Jira connection failed: {response.status_code} - {response.text}")
            return False
        except Exception as e:
            logger.error(f"Jira connection error: {e}")
            return False

    def _verify_github_connection(self) -> bool:
        #Verify GitHub API connection
        try:
            url = f"https://api.github.com/repos/{self.github_config['owner']}/{self.github_config['repo']}"
            response = requests.get(url, headers=self.github_headers, timeout=10)
            if response.status_code == 200:
                logger.info("GitHub connection successful")
                return True
            logger.error(f"GitHub connection failed: {response.status_code} - {response.text}")
            return False
        except Exception as e:
            logger.error(f"GitHub connection error: {e}")
            return False
    
    def _fetch_paginated_jira_data(self, endpoint: str, params: Dict) -> List[Dict]:
        #Helper method to handle Jira API pagination
        results = []
        start_at = 0
        max_results = 100
        
        while True:
            try:
                current_params = params.copy()
                current_params.update({
                    "startAt": start_at,
                    "maxResults": max_results
                })
                
                response = requests.get(
                    f"{self.jira_config['base_url']}{endpoint}",
                    auth=self.jira_auth,
                    params=current_params
                )
                response.raise_for_status()
                data = response.json()
                
                if 'issues' in data:
                    results.extend(data['issues'])
                
                start_at += len(data.get('issues', []))
                
                if start_at >= data.get('total', 0):
                    break
                    
            except requests.exceptions.RequestException as e:
                logger.error(f"Jira API error: {e}")
                break
                
        return results
        
    def get_jira_issues(self, project: str, start_date: str, end_date: str) -> List[Dict]:
        #Retrieve Jira issues between specified dates
        try:
            formatted_start = datetime.strptime(start_date, "%Y-%m-%d").strftime("%Y-%m-%d")
            formatted_end = datetime.strptime(end_date, "%Y-%m-%d").strftime("%Y-%m-%d")
        except ValueError as e:
            logger.error(f"Invalid date format: {e}")
            return []

        # Try multiple JQL variants
        # jql_variants = [
        #     f"project = {project} AND statusCategory = Done AND status changed to Done after {formatted_start} AND status changed to Done before {formatted_end}",
        #     f"project = {project} AND status = Done AND updated >= {formatted_start} AND updated <= {formatted_end}",
        #     f"project = {project} AND updated >= {formatted_start} AND updated <= {formatted_end}"
        # ]

       
        issues = self._fetch_paginated_jira_data(
                "/rest/api/3/search",
                params={"expand": "changelog"}
            )
        if issues:
            return issues
        
        logger.warning("No issues found with any JQL variant")
        return []
    
    def get_github_commits_for_jira_key(self, jira_key: str) -> List[Dict]:
        """Get GitHub commits from all branches referencing a specific Jira key"""
        all_commits = []
        seen_shas = set()

        try:
            # Get all branches
            branches_url = f"https://api.github.com/repos/{self.github_config['owner']}/{self.github_config['repo']}/branches"
            branches_response = requests.get(branches_url, headers=self.github_headers)
            branches_response.raise_for_status()
            branches = [b['name'] for b in branches_response.json()]
        except Exception as e:
            logger.error(f"Failed to fetch branches: {e}")
            return []

        for branch in branches:
            page = 1
            while True:
                try:
                    commits_url = f"https://api.github.com/repos/{self.github_config['owner']}/{self.github_config['repo']}/commits"
                    params = {
                        "sha": branch,
                        "per_page": 100,
                        "page": page
                    }
                    response = requests.get(commits_url, headers=self.github_headers, params=params)
                    response.raise_for_status()
                    commits = response.json()

                    if not commits:
                        break

                    for commit in commits:
                        sha = commit.get('sha')
                        if sha in seen_shas:
                            continue
                            
                        message = commit.get('commit', {}).get('message', '')
                        if jira_key.lower() in message.lower():
                            all_commits.append(commit)
                            seen_shas.add(sha)

                        author_name = None
                        if commit.get('author') and commit['author'].get('login'):
                            author_name = commit['author']['login']
                        elif commit.get('commit', {}).get('author', {}).get('name'):
                            author_name = commit['commit']['author']['name']
                    if len(commits) < 100:
                        break
                        
                    page += 1

                except requests.exceptions.RequestException as e:
                    logger.error(f"Error fetching commits from {branch}: {e}")
                    break

        logger.info(f"Found {len(all_commits)} commits referencing {jira_key}")
        return all_commits
    
    def safe_get(self,dct: Dict, keys: str, default=None):
        
        try:
            for key in keys.split('.'):
                dct = dct.get(key, {}) if isinstance(dct, dict) else {}
            return dct if dct != {} else default
        except Exception:
            return default
    
    
    def _get_jira_issue_details(self, jira_key: str) -> Optional[Dict]:
        #Fetch Jira issue details with comprehensive error handling
        try:
            # First get the field schema to identify custom fields
            fields_url = f"{self.jira_config['base_url']}/rest/api/3/field"
            fields_response = requests.get(fields_url, auth=self.jira_auth, timeout=10)
            fields_response.raise_for_status()
            all_fields = fields_response.json()

            # Identify custom fields by name patterns
            points_field_id = next(
                (f['id'] for f in all_fields 
                if 'point' in f['name'].lower() or 'story' in f['name'].lower()),
                None
            )

            # Get issue details with expanded fields
            issue_url = f"{self.jira_config['base_url']}/rest/api/3/issue/{jira_key}"
            params = {
                'expand': 'names,renderedFields',
                'fields': f'reporter,priority,issuetype,project,resolution,status,created,updated{"," + points_field_id if points_field_id else ""}'
            }
            
            issue_response = requests.get(issue_url, auth=self.jira_auth, params=params, timeout=15)
            issue_response.raise_for_status()
            issue_data = issue_response.json()

            if not issue_data or not isinstance(issue_data.get('fields'), dict):
                logger.error(f"Invalid Jira response structure for {jira_key}")
                return None

            fields = issue_data['fields']

            # Safely extract reporter information
            reporter = 'Unknown'
            try:
                reporter_data = fields.get('reporter') or {}
                reporter = reporter_data.get('displayName', 
                                        reporter_data.get('name', 'Unknown'))
            except Exception as e:
                logger.warning(f"Couldn't extract reporter for {jira_key}: {str(e)}")

            # Safely extract story points
            points = 0
            if points_field_id:
                try:
                    points = float(fields.get(points_field_id, 0)) if fields.get(points_field_id) else 0
                except (TypeError, ValueError) as e:
                    logger.warning(f"Couldn't parse points for {jira_key}: {str(e)}")

            return {
                'priority': self.safe_get(fields, 'priority.name', 'Normal'),
                'created': self.safe_get(fields, 'created'),
                'reporter': reporter,
                'issuetype': self.safe_get(fields, 'issuetype.name', 'Story'),
                'project': self.safe_get(fields, 'project.key', 'GJA'),
                'resolution': self.safe_get(fields, 'resolution.name', 'Unresolved'),
                'points': points,
                'status_change_date': self.safe_get(fields, 'updated', self.safe_get(fields, 'created'))
                
            }

        except requests.exceptions.RequestException as e:
            logger.error(f"Jira API request failed for {jira_key}: {str(e)}")
        except Exception as e:
            logger.error(f"Unexpected error processing {jira_key}: {str(e)}")
        
        return None
            
    
    def store_jira_parent_relationships(self, jira_key: str) -> int:
        #Store parent relationships in the database
        try:
            # Verify database connection
            if not self.db_conn or self.db_conn.closed != 0:
                if not self._init_db():
                    logger.error("Cannot store relationships - no database connection")
                    return 0

            # Fetch issue details including parent from Jira API
            url = f"{self.jira_config['base_url']}/rest/api/3/issue/{jira_key}?fields=parent,issuelinks"
            response = requests.get(url, auth=self.jira_auth, timeout=15)
            response.raise_for_status()
            issue_data = response.json()

            if not issue_data or not isinstance(issue_data.get('fields'), dict):
                logger.warning(f"No valid data found for {jira_key}")
                return 0

            fields = issue_data['fields']
            parent_records = []

            # Check for direct parent (Epic relationship)
            if fields.get('parent'):
                parent = fields['parent']
                parent_records.append((
                    jira_key,
                    parent.get('key'),
                    parent.get('fields', {}).get('summary'),
                    parent.get('fields', {}).get('issuetype', {}).get('name')
                ))

            # Check for issue links (for other relationship types)
            for link in fields.get('issuelinks', []):
                if link.get('outwardIssue'):
                    linked_issue = link['outwardIssue']
                    if any(t in link.get('type', {}).get('name', '').lower() 
                        for t in ['parent', 'epic', 'child']):
                        parent_records.append((
                            jira_key,
                            linked_issue.get('key'),
                            linked_issue.get('fields', {}).get('summary'),
                            linked_issue.get('fields', {}).get('issuetype', {}).get('name')
                        ))
                elif link.get('inwardIssue'):
                    linked_issue = link['inwardIssue']
                    if any(t in link.get('type', {}).get('name', '').lower() 
                        for t in ['parent', 'epic', 'child']):
                        parent_records.append((
                            linked_issue.get('key'),
                            jira_key,
                            None,  # Don't store summary for children
                            None   # Don't store type for children
                        ))

            if not parent_records:
                logger.debug(f"No parent relationships found for {jira_key}")
                return 0

            # Insert records
            with self.db_conn.cursor() as cursor:
                cursor.executemany("""
                    INSERT INTO jira_parent (
                        jira_key, parent_key, parent_summary, parent_type
                    ) VALUES (%s, %s, %s, %s)
                    ON CONFLICT (jira_key, parent_key) DO UPDATE SET
                        parent_summary = EXCLUDED.parent_summary,
                        parent_type = EXCLUDED.parent_type
                """, parent_records)

                inserted_count = cursor.rowcount
                self.db_conn.commit()
                logger.info(f"Stored {inserted_count} parent relationships for {jira_key}")
                return inserted_count

        except requests.exceptions.RequestException as e:
            logger.error(f"Jira API error for {jira_key} parent: {str(e)}")
        except Exception as e:
            if self.db_conn:
                self.db_conn.rollback()
            logger.error(f"Database error storing {jira_key} parent: {str(e)}")
        return 0
    
    def store_jira_issue_links(self, jira_key: str) -> int:
        #Store Jira issue links in the database
        try:
            # First get the issue type for this issue
            issue_type = ''
            try:
                issue_url = f"{self.jira_config['base_url']}/rest/api/3/issue/{jira_key}?fields=issuetype"
                issue_response = requests.get(issue_url, auth=self.jira_auth, timeout=10)
                issue_response.raise_for_status()
                issue_data = issue_response.json()
                issue_type = self.safe_get(issue_data, 'fields.issuetype.name', '') or ''
            except Exception as e:
                logger.warning(f"Couldn't fetch issue type for {jira_key}: {str(e)}")

            # Fetch issue links from Jira API
            url = f"{self.jira_config['base_url']}/rest/api/3/issue/{jira_key}"
            params = {
                'fields': 'issuelinks',
                'expand': 'fields.issuelinks'
            }
            
            response = requests.get(url, auth=self.jira_auth, params=params, timeout=15)
            response.raise_for_status()
            issue_data = response.json()

            link_records = []

            if issue_data and isinstance(issue_data.get('fields'), dict):
                fields = issue_data['fields']
                
                for link in fields.get('issuelinks', []):
                    try:
                        # Handle outward links (this issue links to another)
                        if link.get('outwardIssue'):
                            linked_issue = link['outwardIssue']
                            link_type = link.get('type', {}).get('outward', 'relates to') or ''
                            link_records.append((
                                jira_key,
                                link_type.lower(),
                                linked_issue.get('key', ''),
                                self.safe_get(linked_issue, 'fields.status.name', '') or '',
                                self.safe_get(linked_issue, 'fields.priority.name', '') or '',
                                self.safe_get(linked_issue, 'fields.issuetype.name', '') or ''
                            ))

                        # Handle inward links (another issue links to this one)
                        if link.get('inwardIssue'):
                            linked_issue = link['inwardIssue']
                            link_type = link.get('type', {}).get('inward', 'relates to') or ''
                            link_records.append((
                                linked_issue.get('key', ''),
                                link_type.lower(),
                                jira_key,
                                '',  # link_status
                                '',  # link_priority
                                ''   # issue_type
                            ))

                    except Exception as e:
                        logger.warning(f"Error processing link for {jira_key}: {str(e)}")
                        continue

            # Insert into the database
            with self.db_conn.cursor() as cursor:
                # First delete any existing links for this issue
                cursor.execute("""
                    DELETE FROM jira_issue_link WHERE jira_key = %s
                """, (jira_key,))
                
                if not link_records:
                    link_records.append((
                        jira_key,
                        '',  # link_type
                        '',  # link_key
                        '',  # link_status
                        '',  # link_priority
                        issue_type  # issue_type
                    ))
                
                # Now insert all current links
                execute_batch(cursor, """
                    INSERT INTO jira_issue_link (
                        jira_key, link_type, link_key, 
                        link_status, link_priority, issue_type
                    ) VALUES (%s, %s, %s, %s, %s, %s)
                """, link_records)

                inserted_count = len(link_records)
                self.db_conn.commit()
                logger.info(f"Stored {inserted_count} issue links for {jira_key}")
                return inserted_count

        except requests.exceptions.RequestException as e:
            logger.error(f"Jira API error for {jira_key} issue links: {str(e)}")
        except Exception as e:
            if self.db_conn:
                self.db_conn.rollback()
            logger.error(f"Database error storing {jira_key} issue links: {str(e)}")
            logger.error(f"Traceback: {traceback.format_exc()}")
        return 0

    def store_commit_jira_mappings(self, jira_key: str, commits: List[Dict]) -> int:
        #Store the mappings of commits to Jira issues in the database
        try:
            # data for all tables
            commit_records = []
            for commit in commits:
                if not isinstance(commit, dict):
                    continue

                sha = commit.get('sha')
                
                if not sha:
                    continue

                commit_records.append((jira_key, sha))
                     
            if not commit_records:
                logger.warning(f"No valid commits for {jira_key}")
                return 0
            
            with self.db_conn.cursor() as cursor:
                if commit_records:
                    cursor.executemany("""
                        INSERT INTO commit_jira (jira_key, sha)
                        VALUES (%s, %s)
                        ON CONFLICT (sha) DO NOTHING
                    """, commit_records)

                # calling parent relationships
                self.store_jira_parent_relationships(jira_key)
                # calling issue links
                links_stored = self.store_jira_issue_links(jira_key)
                if links_stored == 0:
                    logger.warning(f"No issue links were stored for {jira_key} - this might be normal if no links exist")
                self.db_conn.commit()
                logger.info(f"Stored {len(commit_records)} commits and relationships for {jira_key}")
                return len(commit_records)

        except Exception as e:
            if self.db_conn:
                self.db_conn.rollback()
            logger.error(f"Failed to store data for {jira_key}: {str(e)}")
            return 0

    def store_single_jira_history(self, jira_key: str):
        url = f"{self.jira_config['base_url']}/rest/api/3/issue/{jira_key}/changelog"
        try:
            response = requests.get(url, auth=self.jira_auth, timeout=15)
            response.raise_for_status()
            changelog_data = response.json()
            
            history_records = []
            if changelog_data and isinstance(changelog_data.get('values'), list):
                for history in changelog_data['values']:
                    created = history.get('created')
                    if not created:
                        continue

                    for item in history.get('items', []):
                        field = item.get('field')
                        history_records.append((
                            jira_key,
                            created,
                            field,
                            item.get('fromString'),
                            item.get('toString')
                        ))

            if history_records:
                with self.db_conn.cursor() as cursor:
                    cursor.executemany("""
                        INSERT INTO jira_history (
                            jira_key, change_date, field, 
                            from_value, current_stage
                        ) VALUES (%s, %s, %s, %s, %s)
                        ON CONFLICT (jira_key, change_date, field) DO NOTHING
                    """, history_records)
                    self.db_conn.commit()
        except Exception as e:
            logger.error(f"Failed to store history for {jira_key}: {str(e)}")
            if self.db_conn:
                self.db_conn.rollback()
            
    def store_jira_details_only(self, jira_key: str) -> int:
        try:
            # Get Jira issue details
            jira_issue = self._get_jira_issue_details(jira_key)
            if not jira_issue:
                logger.error(f"Failed to get details for {jira_key}")
                return 0

            # Try to get description
            issue_description = ""
            try:
                issue_url = f"{self.jira_config['base_url']}/rest/api/3/issue/{jira_key}?fields=description"
                response = requests.get(issue_url, auth=self.jira_auth, timeout=10)
                response.raise_for_status()
                issue_data = response.json()
                description_content = issue_data.get('fields', {}).get('description')
                if isinstance(description_content, dict):
                    issue_description = description_content.get('content', [{}])[0].get('content', [{}])[0].get('text', '')
                elif isinstance(description_content, str):
                    issue_description = description_content
            except Exception as e:
                logger.warning(f"Couldn't fetch description for {jira_key}: {str(e)}")

            detail_record = (
                jira_key,
                jira_issue['priority'],
                jira_issue['created'],
                f"{self.jira_config['base_url']}/browse/{jira_key}",
                issue_description[:500],
                jira_issue['reporter'],
                jira_issue['issuetype'],
                jira_issue['project'],
                jira_issue['resolution'],
                jira_issue['points'],
                jira_issue['status_change_date']
            )

            with self.db_conn.cursor() as cursor:
                try:
                    cursor.execute("""
                        INSERT INTO jira_detail (
                            jira_key, priority, created_date, url, 
                            summary, reporter, issue_type, project,
                            resolution, points, status_change_date
                        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                        ON CONFLICT (jira_key) DO UPDATE SET
                            priority = EXCLUDED.priority,
                            url = EXCLUDED.url,
                            summary = EXCLUDED.summary,
                            reporter = EXCLUDED.reporter,
                            issue_type = EXCLUDED.issue_type,
                            resolution = EXCLUDED.resolution,
                            points = EXCLUDED.points,
                            status_change_date = EXCLUDED.status_change_date
                    """, detail_record)
                except Exception as e:
                    logger.error(f"Failed to insert jira_detail for {jira_key}: {e}")
                    return 0

            return 1

        except Exception as e:
            logger.exception(f"Unexpected error while storing Jira details for {jira_key}: {e}")
            return 0

    
    def get_changed_files(self, commit_sha: str) -> List[Dict]:
       
        url = f"https://api.github.com/repos/{self.github_config['repo']}/commits/{commit_sha}"
        
        try:
            response = requests.get(url, headers=self.github_headers)
            response.raise_for_status()
            commit_data = response.json()
            
            return [
                {
                    'filename': file.get('filename'),
                    'additions': file.get('additions'),
                    'deletions': file.get('deletions'),
                    'changes': file.get('changes'),
                    'status': file.get('status')
                }
                for file in commit_data.get('files', [])
            ]
            
        except requests.exceptions.RequestException as e:
            logger.error(f"Error fetching commit {commit_sha[:7]}: {e}")
            return []

if __name__ == "__main__":
    try:
        integrator = JiraGitHubIntegrator()
        
        if not integrator.verify_connections():
            logger.error("Failed to establish connections")
            exit(1)
            
        project = "GJA"  
        start_date = "2024-01-01"
        end_date = datetime.now().strftime("%Y-%m-%d")
        
        #calling the get_jira_issues function
        issues = integrator.get_jira_issues(project, start_date, end_date)
        logger.info(f"Found {len(issues)} Jira issues")
        
        for issue in issues:
            jira_key = issue.get('key')
            if not jira_key:
                continue
            integrator.store_single_jira_history(jira_key)  
            commits = integrator.get_github_commits_for_jira_key(jira_key)
            integrator.store_jira_details_only(jira_key)
            if commits and integrator.db_config:
                stored = integrator.store_commit_jira_mappings(jira_key, commits)
                logger.info(f"Stored {stored} commits for {jira_key}")        
    except Exception as e:
        logger.error(f"Initialization failed: {e}")
        exit(1)