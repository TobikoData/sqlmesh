from sqlmesh import macro


@macro()
def noop(context):
    return "SELECT 1"
