import gitlab

from attr_util import set_attributes_for_collection
from models import Commit
from metrics_fetcher import MetricsFetcher

class GitLabMetricsFetcher(MetricsFetcher):
    def __init__(self, url, access_key):
        self.client = gitlab.Gitlab(url, private_token=access_key)

    def fetch_projects(self):
        projects = self.client.projects.list(all=True)
        set_attributes_for_collection(projects, full_name=lambda project: project.name_with_namespace.lower())
        return projects

    def fetch_commits(self, project, since, all_branches):
        if all_branches:
            branches = project.branches.list(all=True)
        else:
            branches = [project.branches.get('master')]

        commits = [commit
                   for branch in branches
                   for commit in project.commits.list(all=True, with_stats=True, since=since, ref_name=branch.name)
                   ]
        return [Commit.from_gitlab_commit(commit) for commit in commits]
