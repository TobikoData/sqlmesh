from sqlmesh.integrations.github.cicd.config import GithubCICDBotConfig
from sqlmesh.integrations.gitlab.cicd.config import GitlabCICDBotConfig

CICDBotConfig = GithubCICDBotConfig | GitlabCICDBotConfig
