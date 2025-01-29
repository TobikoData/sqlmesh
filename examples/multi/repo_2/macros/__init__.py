from sqlmesh import macro


@macro()
def two(context):
    return 2


@macro()
def dup(context):
    return "'repo_2'"
