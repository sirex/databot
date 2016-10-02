import urllib.parse


def check_domains(url, domains=None):
    if not domains:
        return True
    domains = [domains] if isinstance(domains, str) else domains
    for domain in domains:
        if url.netloc == domain:
            return True
    return False


def check_query(qry, query):
    qry = dict(qry)
    for key in query:
        if key not in qry:
            return False
    return True


def url(url, domains=None, query=None):
    query = [query] if isinstance(query, str) else query or []

    url = urllib.parse.urlparse(url)
    qry = urllib.parse.parse_qsl(url.query, keep_blank_values=True)
    if query:
        qry = [(k, v) for k, v in qry if k in query]
    qry = sorted(qry)

    checks = [
        check_domains(url, domains),
        check_query(qry, query),
    ]
    if not all(checks):
        return None

    url = url._replace(query=urllib.parse.urlencode(qry), fragment='')
    return urllib.parse.urlunparse(url)
