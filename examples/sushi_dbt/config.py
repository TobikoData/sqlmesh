from sqlmesh.dbt.profile import Profile

config = Profile.load().to_sqlmesh()
