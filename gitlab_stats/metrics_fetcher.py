from abc import ABC, abstractmethod

class MetricsFetcher(ABC):
    @abstractmethod
    def fetch_projects(self):
        pass

    @abstractmethod
    def fetch_commits(self, project, since, all_branches):
        pass
