import os
import json
import re
import stat
import asyncio
import aiohttp
import psycopg2
import subprocess
import shutil
import time
from typing import Optional
import configparser
from pathlib import Path
import requests
from collections import defaultdict

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
        GITHUB_TOKEN = config.get('GITHUB', 'TOKEN')
        github_repo=config.get('GITHUB', 'REPO')
        github_owner=config.get('GITHUB', 'OWNER')
        
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

    #print("Loaded config:", {section: dict(config[section]) for section in config.sections()})

# GitHub API setup
BASE_URL = 'https://api.github.com'
HEADERS = {
    "Authorization": f"token {GITHUB_TOKEN}",
    "Accept": "application/vnd.github.v3+json"
}

class RateLimiter:
    # Rate limiting to avoid hitting GitHub API limits
    def __init__(self):
            self.last_request = 0
            self.min_delay = 1.0  # seconds between requests

    async def wait(self):
            elapsed = time.time() - self.last_request
            if elapsed < self.min_delay:
                await asyncio.sleep(self.min_delay - elapsed)
            self.last_request = time.time()

rate_limiter = RateLimiter()

def create_db_connection():
    #Creating a connection to PostgreSQL database
        try:
            return psycopg2.connect(**DB_CONFIG)
        except psycopg2.Error as e:
            print(f"Database connection failed: {e}")
            return None

def get_all_branches(owner, repo, token=None):
    # Fetching all branches for a given repository
    branches = []
    page = 1
    headers = {
        "Accept": "application/vnd.github.v3+json",
        "Authorization": f"token {token}" if token else None
    }
    
    while True: 
        try:
            url = f"https://api.github.com/repos/{owner}/{repo}/branches?per_page=100&page={page}"
            print(f"Fetching branches from {url}...")
            response = requests.get(url, headers=headers)
            
            # Check for rate limiting
            if response.status_code == 403 and 'X-RateLimit-Remaining' in response.headers:
                if int(response.headers['X-RateLimit-Remaining']) == 0:
                    reset_time = int(response.headers['X-RateLimit-Reset'])
                    sleep_time = max(reset_time - time.time(), 0) + 5
                    print(f"Rate limit exceeded. Sleeping for {sleep_time} seconds...")
                    time.sleep(sleep_time)
                    continue
            
            response.raise_for_status()
            
            data = response.json()
            if not data:
                break
                
            branches.extend(branch['name'] for branch in data)
            if len(data) < 100:
                break
            page += 1
            
        except requests.exceptions.HTTPError as e:
            if response.status_code == 404:
                print(f"repository {owner}/{repo} not found or inaccessible")
            else:
                print(f"Error fetching branches: {e}")
            break
        except Exception as e:
            print(f"Unexpected error: {e}")
            break
    
    return branches

async def get_repository_info(session: aiohttp.ClientSession, owner: str, repo: str) -> dict:
    # Get repository information
    repo_data = await get_github_data(session, f"/repos/{owner}/{repo}")
    if repo_data:
        return {
            'default_branch': repo_data.get('default_branch', 'main'),
            'html_url': repo_data.get('html_url', f"https://github.com/{owner}/{repo}")
        }
    return {'default_branch': 'main', 'html_url': f"https://github.com/{owner}/{repo}"}

async def get_github_data(session: aiohttp.ClientSession, endpoint: str, params: Optional[dict] = None) -> Optional[dict]:
    # Fetching data from GitHub API with rate limiting and error handling
    url = f"{BASE_URL}{endpoint}"
    max_retries = 3
    retry_delay = 5
    
    for attempt in range(max_retries):
        try:
            async with session.get(url, headers=HEADERS, params=params) as response:
                # Handle rate limits
                remaining = int(response.headers.get('X-RateLimit-Remaining', 0))
                if remaining <= 1:
                    reset_time = int(response.headers.get('X-RateLimit-Reset', time.time() + 3600))
                    sleep_time = max(reset_time - time.time(), 0) + 10
                    print(f"⚠️ Rate limit reached. Sleeping for {sleep_time} seconds...")
                    await asyncio.sleep(sleep_time)
                    continue  # Retry after sleep
                
                if response.status == 200:
                    return await response.json()
                elif response.status == 404:
                    print(f"❌ 404 Not Found: {url}")
                    return None
                else:
                    print(f"⚠️ Retry {attempt + 1}: HTTP {response.status} for {url}")
                    await asyncio.sleep(retry_delay)
        except Exception as e:
            print(f"⚠️ Retry {attempt + 1}: Error fetching {url}: {str(e)}")
            await asyncio.sleep(retry_delay)
    
    print(f"❌ Failed after {max_retries} retries for {url}")
    return None
   
async def fetch_and_store_commits(owner: str, repo: str, branch: str = "main") -> bool:
    # Fetch and store commits for a given repository and branch
    async with aiohttp.ClientSession() as session:
        repo_info = await get_repository_info(session, owner, repo)
        if not repo_info:
            print(f"Failed to get repository info for {owner}/{repo}")
            return False
            
        repo_full_name = f"{owner}/{repo}"
        repo_url = repo_info['html_url']
        
        conn = create_db_connection()
        if not conn:
            return False
        
        try:
            page = 1
            per_page = 100
            processed_commits = 0
            
            while True:
                # Get paginated commits
                commits_data = await get_github_data(
                    session,
                    f"/repos/{owner}/{repo}/commits",
                    params={
                        'sha': branch,
                        'per_page': per_page,
                        'page': page
                    }
                )
                
                if not commits_data or not isinstance(commits_data, list):
                    print(f"Invalid commits data received for page {page}")
                    break
                
                # Process commits in current page
                for commit_data in commits_data:
                    if not isinstance(commit_data, dict) or 'sha' not in commit_data:
                        print(f"Skipping invalid commit data: {commit_data}")
                        continue
                        
                    try:
                        # Get full commit details
                        full_commit = await get_github_data(
                            session,
                            f"/repos/{owner}/{repo}/commits/{commit_data['sha']}"
                        )
                        
                        if not full_commit:
                            print(f"Failed to fetch details for commit {commit_data['sha']}")
                            continue
                            
                        if store_commit_data(conn, repo_full_name, repo_url, branch, full_commit):
                            processed_commits += 1
                        else:
                            print(f"Failed to store commit {commit_data['sha']}")
                            
                    except Exception as e:
                        print(f"Error processing commit {commit_data.get('sha', 'unknown')}: {str(e)}")
                        continue
                
                # Pagination control
                if len(commits_data) < per_page:
                    break
                    
                page += 1
                await asyncio.sleep(1)  # Rate limiting
            
            print(f"✅ Successfully processed {processed_commits} commits for {repo_full_name} branch {branch}")
            return True
            
        except Exception as e:
            print(f"Error processing {repo_full_name}: {str(e)}")
            return False
        finally:
            conn.close()


def store_commit_data(conn, repo_full_name, repo_url, branch, commit_data):
    #Store commit data in PostgreSQL across multiple related tables
    if not conn:
        return False

    try:
        with conn.cursor() as cursor:
            cursor.execute("BEGIN")
            # First, extract all the data we'll need
            sha = commit_data['sha']
            if not sha:
                print("⚠️ Commit missing SHA, skipping")
                return False
            commit_message = commit_data['commit']['message']
            words = re.findall(r'\b[a-z0-9]+\b', commit_message.lower())
            author_name = None
            if commit_data.get('author') and commit_data['author'].get('login'):
                author_name = commit_data['author']['login']
            elif commit_data.get('commit', {}).get('author', {}).get('name'):
                author_name = commit_data['commit']['author']['name']
            
            stats = commit_data.get('stats', {})
            files = commit_data.get('files', [])
            # Extract directories and file extensions from files
            directories = set()
            extension_counts = defaultdict(int)
            
            for file in files:
                if not isinstance(file, dict):
                    continue
                    
                filename = file.get('filename', '')
                if '.' in filename:
                    _, extension = filename.rsplit('.', 1)
                    extension = '.' + extension.lower()
                elif '/' in filename:
                    directory = filename.rsplit('/', 1)[0]
                    directories.add(directory)  
                else:
                    extension = 'others'  # No extension
                #print(f"Processing file: {filename} with extension: {extension}")            
                extension_counts[extension] += 1
                    
            # 1. Insert into main commit_history table
            cursor.execute("""
                INSERT INTO commit_history (
                    sha, url, branch, repository, repository_url,
                    author, commit_date, commit_message,
                    file_count, lines_added, lines_removed
                ) VALUES (
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
                ) ON CONFLICT (sha) DO UPDATE SET
                    branch = EXCLUDED.branch,
                    repository = EXCLUDED.repository,
                    repository_url = EXCLUDED.repository_url,
                    author = EXCLUDED.author,
                    commit_date = EXCLUDED.commit_date,
                    commit_message = EXCLUDED.commit_message,
                    file_count = EXCLUDED.file_count,
                    lines_added = EXCLUDED.lines_added,
                    lines_removed = EXCLUDED.lines_removed
            """, (
                commit_data['sha'],
                commit_data['html_url'],
                branch,
                repo_full_name,
                repo_url,
                author_name,
                commit_data['commit']['committer']['date'],
                commit_message,
                len(files),
                stats.get('additions', 0),
                stats.get('deletions', 0)
            ))

            # 2. Insert into commit_branch_relationship
            cursor.execute("""
                INSERT INTO commit_branch_relationship (sha, branch)
                VALUES (%s, %s)
                ON CONFLICT (sha, branch) DO NOTHING
            """, (sha, branch))

            # 3. Insert into commit_directory (all unique directories)
            for directory in directories:
                cursor.execute("""
                    INSERT INTO commit_directory (sha, directory)
                    VALUES (%s, %s)
                    ON CONFLICT (sha, directory) DO NOTHING
                """, (sha, directory))

            # 4. Insert into commit_file_types
            for ext, count in extension_counts.items():
                cursor.execute("""
                        INSERT INTO commit_file_types (sha,file_extension,count)
                        VALUES (%s, %s, %s)
                        ON CONFLICT (sha, file_extension) DO UPDATE SET
                        count = EXCLUDED.count
                    """, (sha, ext, count))

            # 5. Insert into commit_message
            for word in words:  # Using set() to store each word only once per commit
                    cursor.execute("""
                        INSERT INTO commit_message_words (sha, word)
                        VALUES (%s, %s)
                        ON CONFLICT (sha, word) DO NOTHING
                    """, (sha, word))
            
            # 6. Process commit tags
            tags = extract_tags(commit_data['commit']['message'])
            if not tags:  
                tags = ['No tag found']

            for tag in tags:
                    cursor.execute("""
                        INSERT INTO commit_tags (sha, tags)
                        VALUES (%s, %s)
                        ON CONFLICT (sha, tags) DO NOTHING
                    """, (sha, str(tag).lower()))

            for file in files:
                if not isinstance(file, dict):
                    continue
                    
                filename = file.get('filename')
                if not filename:  # Skip if no filename
                    continue
                try:
                    line_inserts = file.get('additions', 0)
                    line_deletes = file.get('deletions', 0)

                    # Get language from filename first
                    language = get_language_from_filename(filename)
                    
                    # Skip SCC analysis for non-code files
                    if not is_code_file(filename):
                        complexity_data = {
                            'Lines': 0,
                            'Code': 0,
                            'Comment': 0,
                            'Complexity': 0,
                            'Language': language
                        }
                    else:
                        complexity_data = get_complexity_with_scc(repo_full_name, sha, filename)
                        # Ensure language is set even if SCC fails
                        complexity_data['Language'] = language

                    # Insert into commit_files
                    cursor.execute("""
                        INSERT INTO commit_files (
                            sha, filename, line_inserts, line_deletes,
                            total_lines, total_code_lines, total_comment_lines, complexity
                        ) VALUES (
                            %s, %s, %s, %s, %s, %s, %s, %s
                        ) ON CONFLICT (sha, filename) DO UPDATE SET
                            line_inserts = EXCLUDED.line_inserts,
                            line_deletes = EXCLUDED.line_deletes,
                            total_lines = EXCLUDED.total_lines,
                            total_code_lines = EXCLUDED.total_code_lines,
                            total_comment_lines = EXCLUDED.total_comment_lines,
                            complexity = EXCLUDED.complexity
                    """, (
                        sha,
                        filename,
                        line_inserts,
                        line_deletes,
                        complexity_data.get('Lines', 0),
                        complexity_data.get('Code', 0),
                        complexity_data.get('Comment', 0),
                        complexity_data.get('Complexity', 0)
                    ))

                    # Only store complexity for code files
                    if is_code_file(filename):
                        cursor.execute("""
                            INSERT INTO code_complexity_measurement (
                                filename, language, total_code_lines, 
                                total_comments, complexity
                            ) VALUES (
                                %s, %s, %s, %s, %s
                            ) ON CONFLICT (filename) DO UPDATE SET
                                language = EXCLUDED.language,
                                total_code_lines = EXCLUDED.total_code_lines,
                                total_comments = EXCLUDED.total_comments,
                                complexity = EXCLUDED.complexity
                        """, (
                            filename,
                            complexity_data.get('Language', language),
                            complexity_data.get('Code', 0),
                            complexity_data.get('Comment', 0),
                            complexity_data.get('Complexity', 0)
                        ))
                        
                except Exception as e:
                    print(f"Error processing file {filename} in commit {sha}: {str(e)}")
                    continue

        conn.commit()
        return True
    except Exception as e:
            print(f"❌ Failed to store commit {sha}: {e}")
            return False   

def get_language_from_filename(filename: str) -> str:
    # Get the language from the filename
    if not filename or not isinstance(filename, str):
        return 'Unknown'

    language_mapping = {
        '.py': 'Python',
        '.js': 'JavaScript',
        '.ts': 'TypeScript',
        '.java': 'Java',
        '.go': 'Go',
        '.rb': 'Ruby',
        '.c': 'C',
        '.h': 'C Header',
        '.cpp': 'C++',
        '.hpp': 'C++ Header',
        '.cs': 'C#',
        '.php': 'PHP',
        '.swift': 'Swift',
        '.kt': 'Kotlin',
        '.scala': 'Scala',
        '.rs': 'Rust',
        '.sh': 'Shell Script',
        '.pl': 'Perl',
        '.r': 'R',
        '.sql': 'SQL',
        '.html': 'HTML',
        '.css': 'CSS',
        '.json': 'JSON',
        '.yml': 'YAML',
        '.yaml': 'YAML',
        '.xml': 'XML',
        '.md': 'Markdown',
        '.txt': 'Text',
        '.ps1': 'PowerShell',
        '.soql': 'SOQL',
        '.sosl': 'SOSL',
        '.apex': 'Apex',
        '.cls': 'Apex Class',
        '.trigger': 'Apex Trigger',
        '.page': 'Visualforce Page',
        '.component': 'Visualforce Component',
        '.dockerfile': 'Dockerfile',
        '.tf': 'Terraform',
        '.ini': 'INI',
        '.properties': 'Properties',
        '.gitignore': 'Git Ignore',
        '.forceignore': 'Salesforce Ignore',
        '.sfdx-project': 'Salesforce Config',
        '.eslintrc': 'ESLint Config',
        '.babelrc': 'Babel Config',
        '.prettierrc': 'Prettier Config',
        '.stylelintrc': 'Stylelint Config',
        '.editorconfig': 'EditorConfig',
        '.npmignore': 'NPM Ignore',
        '.dockerignore': 'Docker Ignore',
        '.gitattributes': 'Git Attributes',
        '.tpl': 'Terraform Plan',
        '.npmrc': 'NPM Config',
        '.bin': 'Binary',
        '.exe': 'Executable',
        '.tsx': 'TypeScript JSX',
        '.cjs': 'CommonJS',
        '.email': 'Email',
        'prettierignore': 'Prettier',
        '.prettierignore': 'Prettier',
    }
    
    special_files = {
        'dockerfile': 'Docker',
        'makefile': 'Make',
        'gitignore': 'Git',
        'forceignore': 'Salesforce',
        '.forceignore': 'Salesforce',
        '.gitignore': 'Git',
        'prettierignore': 'Prettier',
        '.prettierignore': 'Prettier',
        'prettierrc': 'Prettier',
        '.prettierrc': 'Prettier',
        '.eslintrc': 'ESLint',
        '.babelrc': 'Babel',
        '.stylelintrc': 'Stylelint',
        '.editorconfig': 'EditorConfig',
        '.npmignore': 'NPM',
        '.dockerignore': 'Docker',
        '.gitattributes': 'Git',
        '.npmrc': 'NPM',
        'husky/pre-commit': 'Husky',
        '.husky/pre-commit': 'Husky',
        '.github/workflows': 'GitHub Actions'
    }
    
    basename = os.path.basename(filename).lower()
    full_path = filename.lower()

    # 1. Check for special files by exact name match
    if basename in special_files:
        return special_files[basename]
    
    # 2. Check for special files with leading dot
    if f".{basename}" in special_files:
        return special_files[f".{basename}"]
    
    # 3. Check for special paths (like husky/pre-commit)
    for special_path in special_files:
        if special_path in full_path:
            return special_files[special_path]

    # 4. Standard extension processing
    _, ext = os.path.splitext(filename.lower())
    if ext in language_mapping:
        return language_mapping[ext]

    return None    

def is_code_file(filename):
    #Checking if file should be analyzed with SCC
    code_extensions = ['.py', '.js', '.ts', '.java', '.go', '.rb', '.php', 
                    '.cpp', '.c', '.h', '.cs', '.swift', '.kt', '.scala',
                    '.rs', '.sh', '.sql', '.html', '.css', '.json', '.yml']
    return any(filename.endswith(ext) for ext in code_extensions)    

def remove_readonly(path):
    #Removing readonly attributes from files and directories
    for root, dirs, files in os.walk(path):
        for name in files + dirs:
            try:
                filepath = os.path.join(root, name)
                mode = os.stat(filepath).st_mode
                os.chmod(filepath, mode | stat.S_IWRITE)
            except Exception as e:
                print(f"Couldn't change permissions for {filepath}: {e}")
               
def handle_remove_readonly(func, path):
    #Handling readonly files and directories
    try:
        os.chmod(path, stat.S_IWRITE)
        func(path)
    except Exception as e:
        print(f"Could not remove {path}: {e}")
        try:
            os.unlink(path)
        except Exception:
            pass

def get_complexity_with_scc(repo_full_name, sha, filename):
    #Analyzing the  file complexity using scc tool
    if not filename or not isinstance(filename, str):
        return {'Language': 'Unknown'}
    temp_dir = os.path.join(os.getenv('TEMP', '/tmp'), f"repo_{sha[:7]}")
    language = get_language_from_filename(filename)
    try:
        if os.path.exists(temp_dir):
            remove_readonly(temp_dir)
            shutil.rmtree(temp_dir, onexc=handle_remove_readonly)
        
        for _ in range(3):  # Retry up to 3 times
            try:
                subprocess.run([
                    'git', 'clone', 
                    f'https://github.com/{repo_full_name}.git',
                    temp_dir
                ], check=True, capture_output=True, text=True, timeout=60)
                break
            except subprocess.TimeoutExpired:
                print(f"Retrying clone for {filename}...")
                continue
        subprocess.run([
            'git', '-C', temp_dir, 'checkout', sha
        ], check=True, capture_output=True, text=True)
        
        # Run scc on the specific file
        file_path = os.path.join(temp_dir, filename)
        if not os.path.exists(file_path):
            return {'Language':language}
            
        result = subprocess.run([
            'scc', '--format', 'json', file_path
        ], capture_output=True, text=True, check=True)
        
        # Parse scc output
        data = json.loads(result.stdout)
        if data:
            data[0]['Language'] = language  
            return data[0]
        return {'Language': language}
        
    except subprocess.CalledProcessError as e:
        print(f"SCC analysis failed for {filename}: {e.stderr}")
        return {'Language': language}   
    except json.JSONDecodeError as e:
        print(f"Invalid JSON from SCC for {filename}")
        return {'Language': language}   
    except Exception as e:
        print(f"Error in complexity analysis: {e}")
        return {'Language': language}
    finally:
        # Clean up temp directory
        if os.path.exists(temp_dir):
                remove_readonly(temp_dir)
                for _ in range(3): 
                    try:
                        shutil.rmtree(temp_dir, onexc=handle_remove_readonly)
                        break
                    except Exception as e:
                        print(f"Retrying cleanup of {temp_dir}: {e}")
                        time.sleep(1)

def extract_tags(message):
    #Extracting tags from commit message
    if not message:
        return ['No tag found']
    tags = re.findall(r'#(\w+)|tags?:?\s*(\d+)', message)
    cleaned_tags = []
    for tag_pair in tags:
        cleaned_tags.extend(t for t in tag_pair if t)
    
    return cleaned_tags if cleaned_tags else ['No tag found']
   
async def async_main():
    # Main function to fetch and store commits
    # repository details
    owner = github_owner
    repo = github_repo
    branches = get_all_branches(owner, repo, token=GITHUB_TOKEN)
    
    #branches="build/QA" if you want to test with a specific branch
    
    print(f"Branches in {owner}/{repo}:")
    # Print each branch on a new line
    for branch in branches:
        print(f"- {branch}")
   
    async with aiohttp.ClientSession() as session:
        for branch in branches:
            print(f"\nFetching commit history for {owner}/{repo} (branch: {branch})...")
            print()
            success = await fetch_and_store_commits(owner, repo, branch)
            if not success:
                print(f"❌ Failed to process commits for {owner}/{repo} branch {branch}")
def main():
    # Main entry point
    asyncio.run(async_main())
if __name__ == "__main__":
    # Running the main function
    main()               
