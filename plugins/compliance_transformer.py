import json
import pandas as pd
import os

class ComplianceTransformer:
    def __init__(self, input_path: str, output_path: str):
        self.input_path = input_path
        self.output_path = output_path

    def transform(self):
        """
        Reads JSON, applies compliance logic, saves to Parquet, and prints summary stats.
        """
        print(f"Starting transformation logic on {self.input_path}...")
        
        if not os.path.exists(self.input_path):
            print(f"Error: Input file {self.input_path} not found.")
            return

        with open(self.input_path, "r", encoding="utf-8") as f:
            raw_data = json.load(f)

        transformed_data = []

        for item in raw_data:
            pr_info = item.get("pr_info", {})
            reviews = item.get("reviews") or []
            status_checks = item.get("status_checks") or {}

            # --- Business Logic ---
            # Rule 1: At least one APPROVED review
            has_approval = any(r.get("state") == "APPROVED" for r in reviews)
            
            # Rule 2: All status checks passed
            checks_passed = status_checks.get("state") == "success"
            
            # Final Compliance
            is_compliant = has_approval and checks_passed

            # Extract repository name safely
            repo_name = pr_info.get("base", {}).get("repo", {}).get("name", "unknown")

            row = {
                "pr_number": pr_info.get("number"),
                "pr_title": pr_info.get("title"),
                "author": pr_info.get("user", {}).get("login"),
                "repository": repo_name,  # Added requirement
                "merged_at": pr_info.get("merged_at"),
                "code_review_passed": has_approval,
                "status_checks_passed": checks_passed,
                "is_compliant": is_compliant
            }
            transformed_data.append(row)

        # Create DataFrame
        df = pd.DataFrame(transformed_data)
        
        # Save to Parquet
        try:
            os.makedirs(os.path.dirname(self.output_path), exist_ok=True)
            df.to_parquet(self.output_path, index=False)
            print(f"Successfully saved compliance report to {self.output_path}")
        except Exception as e:
            print(f"Error saving Parquet: {e}")

        # --- Summary Statistics (Requirement) ---
        self._print_summary_stats(df)

    def _print_summary_stats(self, df):
        """Calculates and logs summary statistics as required."""
        total_prs = len(df)
        if total_prs == 0:
            print("No data to summarize.")
            return

        compliant_count = df["is_compliant"].sum()
        compliance_rate = (compliant_count / total_prs) * 100
        violations = total_prs - compliant_count
        
        # Group violations by repository
        violations_by_repo = df[~df["is_compliant"]].groupby("repository").size().to_dict()

        print("\n" + "="*40)
        print("COMPLIANCE SUMMARY REPORT")
        print("="*40)
        print(f"Total PRs Processed:    {total_prs}")
        print(f"Compliant PRs:          {compliant_count}")
        print(f"Compliance Rate:        {compliance_rate:.2f}%")
        print(f"Total Violations:       {violations}")
        print("-" * 20)
        print("Violations by Repository:")
        for repo, count in violations_by_repo.items():
            print(f"  - {repo}: {count}")
        print("="*40 + "\n")