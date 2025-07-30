UPSTREAM_PICKLE_FILE_SIGNATURES = {
    "a3a8d27b822072fa5c67c0651cb3c934"  # distributed with dateparser==1.2.2
}


def fix_dateparser() -> None:
    # work around the following upstream issues in dateparser==1.2.2 (which all have the same root cause):
    # - https://github.com/scrapinghub/dateparser/issues/1270
    # - https://github.com/scrapinghub/dateparser/issues/1281
    # - https://github.com/scrapinghub/dateparser/issues/1282
    #
    # This hack can be removed if this issue is fixed upstream.
    # If you're removing this hack, make sure to update pyproject.toml to blacklist version 1.2.2

    import importlib, hashlib
    from pathlib import Path

    tz_cache = None
    spec = importlib.util.find_spec("dateparser")
    if spec and spec.origin:
        # spec.origin will be something like:
        # "/path/to/venv/lib/python3.9/site-packages/dateparser/__init__.py"
        tz_cache = Path(spec.origin).parent / "data" / "dateparser_tz_cache.pkl"
        if tz_cache.exists():
            # if the tz_cache file matches the signature of the buggy upstream one, delete it
            # deleting it forces it to be correctly re-generated for the local environment when dateparser is imported
            signature = hashlib.md5(tz_cache.read_bytes()).hexdigest()
            if signature in UPSTREAM_PICKLE_FILE_SIGNATURES:
                try:
                    tz_cache.unlink()
                except Exception as e:
                    print(f"WARNING: Unable to delete upstream dateparser cache: {str(e)}")

    # Test that it actually worked
    import dateparser

    if dateparser.parse("1 minute ago") is None:
        hint_filename = (
            str(tz_cache)
            if tz_cache is not None
            else "site-packages/dateparser/data/dateparser_tz_cache.pkl"
        )
        print(
            "WARNING: Buggy dateparser detected; some date expressions may fail to parse.\n"
            f"Please either delete the file '{hint_filename}' manually or use dateparser<=1.2.1"
        )
