import re

# Captures one of the following patterns: "{{", "{#", "{%" and "{%-".
# Q: this will also flag text that contains "{{" inside a string as Jinja. Is this a problem?
JINJA_RE = re.compile("{({|#|(%(-)?))")
