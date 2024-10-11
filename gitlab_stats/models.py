from datetime import datetime

class Project:
    def __init__(self, id, name, full_name, description, url, default_branch):
        self.id = id
        self.name = name
        self.full_name = full_name
        self.description = description
        self.url = url
        self.default_branch = default_branch

    @classmethod
    def from_github_project(cls, github_project):
        return cls(
            id=github_project.id,
            name=github_project.name,
            full_name=github_project.full_name,
            description=github_project.description,
            url=github_project.html_url,
            default_branch=github_project.default_branch
        )

    @classmethod
    def from_gitlab_project(cls, gitlab_project):
        return cls(
            id=gitlab_project.id,
            name=gitlab_project.name,
            full_name=gitlab_project.path_with_namespace,
            description=gitlab_project.description,
            url=gitlab_project.web_url,
            default_branch=gitlab_project.default_branch
        )


from datetime import datetime

class Commit:
    def __init__(self, sha, author, date, message, parents, stats):
        self.sha = sha
        self.author = author
        self.date = date
        self.message = message
        self.parents = parents
        self.stats = stats

    @classmethod
    def from_github_commit(cls, github_commit):
        return cls(
            sha=github_commit.sha,
            author=github_commit.commit.author.name,
            date=github_commit.commit.author.date,
            message=github_commit.commit.message,
            parents=[parent.sha for parent in github_commit.parents],
            stats=github_commit.stats.__dict__
        )

    @classmethod
    def from_gitlab_commit(cls, gitlab_commit):
        return cls(
            sha=gitlab_commit.id,
            author=gitlab_commit.author_name,
            date=datetime.strptime(gitlab_commit.created_at, '%Y-%m-%dT%H:%M:%S.%f%z'),
            message=gitlab_commit.message,
            parents=gitlab_commit.parent_ids,
            stats=gitlab_commit.stats
        )
