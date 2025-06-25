{% if foo == 'bar' %}
    baz
    {% if baz == 'bing' %}
        bong
    {% else %}
        qux
    {% endif %}
{% elif a == fn(b) %}
    {% if c == 'f' and fn1(a, c, 'foo') == 'test' %}
        output1
    {% elif z is defined %}
        output2
    {% endif %}
    output
{% endif %}