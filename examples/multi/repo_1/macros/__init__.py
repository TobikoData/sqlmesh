from sqlmesh import macro


@macro()
def one(context):
    return 1
