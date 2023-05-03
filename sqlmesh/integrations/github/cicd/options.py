import click

token = click.option(
    "--token",
    type=str,
    help="The Github Token to be used. Pass in `${{ secrets.GITHUB_TOKEN }}` if you want to use the one created by Github actions",
)

merge = click.option(
    "--merge",
    is_flag=True,
    help="Merge the PR after successfully deploying to production",
)

delete = click.option(
    "--delete",
    is_flag=True,
    help="Delete the PR environment after successfully deploying to production",
)
