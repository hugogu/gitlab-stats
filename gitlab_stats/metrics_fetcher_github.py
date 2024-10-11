from github import Github, GithubException

from models import Commit
from metrics_fetcher import MetricsFetcher

class GitHubMetricsFetcher(MetricsFetcher):
    def __init__(self, access_key):
        self.client = Github(access_key)

    def fetch_projects(self):
        return self.client.get_user().get_repos()

    def fetch_commits(self, project, since, all_branches):
        try:
            if all_branches:
                branches = project.get_branches()
            else:
                branches = [project.get_branch('master')]
        except GithubException as e:
            print(f"Failed to fetch branches for project {project.name}: {str(e)}")
            return []

        commits = []
        for branch in branches:
            commits.extend(project.get_commits(sha=branch.name, since=since))

        return [Commit.from_github_commit(commit) for commit in commits]