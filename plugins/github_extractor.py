import requests
import json
import os
import time

class GitHubExtractor:
    def __init__(self, owner, repo, token):
        self.owner = owner
        self.repo = repo
        self.headers = {
            "Authorization": f"token {token}",
            "Accept": "application/vnd.github.v3+json"
        }
        self.base_url = f"https://api.github.com/repos/{owner}/{repo}"

    def _make_request(self, endpoint, params=None):
        """Helper method to handle API requests and errors."""
        url = f"{self.base_url}/{endpoint}"
        try:
            response = requests.get(url, headers=self.headers, params=params)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            print(f"Error fetching {endpoint}: {e}")
            return None

    def get_reviews(self, pr_number):
        """Fetches reviews for a PR."""
        return self._make_request(f"pulls/{pr_number}/reviews")

    def get_status_checks(self, commit_sha):
        """Fetches status checks for a specific commit."""
        return self._make_request(f"commits/{commit_sha}/status")

    def get_commits(self, pr_number):
        """Fetches the list of commits for a PR."""
        return self._make_request(f"pulls/{pr_number}/commits")

    def fetch_full_data(self, limit=50):
        """Fetches PRs and enriches them with reviews, status checks, and commits."""
        print(f"Fetching PRs for {self.owner}/{self.repo}...")
        prs_raw = self._make_request("pulls", params={"state": "closed", "per_page": limit})
        
        if not prs_raw:
            print("No PRs found.")
            return []

        full_data = []
        for i, pr in enumerate(prs_raw):
            pr_number = pr.get("number")
            head_sha = pr.get("head", {}).get("sha")
            
            print(f"Enriching PR #{pr_number} ({i+1}/{len(prs_raw)})...")
            
            # Enrich data with Reviews, Status, AND Commits
            reviews = self.get_reviews(pr_number)
            status = self.get_status_checks(head_sha)
            commits = self.get_commits(pr_number)
            
            enriched_pr = {
                "pr_info": pr,
                "reviews": reviews,
                "status_checks": status,
                "commits": commits  # Added as per requirements
            }
            full_data.append(enriched_pr)
            
            # Rate limit protection
            time.sleep(0.5) 

        return full_data

    def save_to_json(self, data, filename):
        """Saves data to the data volume (handles both Docker and Local paths)."""
        if os.getenv("AIRFLOW_HOME"):
            base_dir = "/opt/airflow/data"
        else:
            base_dir = os.path.join(os.getcwd(), "data")
            
        path = os.path.join(base_dir, filename)
        os.makedirs(os.path.dirname(path), exist_ok=True)
        
        with open(path, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=4)
        print(f"Saved {len(data)} records to {path}")