from sqlmesh import macro


@macro()
def one(context):
    return 1


@macro()
def dup(context):
    return "'repo_1'"
