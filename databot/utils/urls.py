import urllib.parse


def check_domains(url, domains=None):
    if not domains:
        return True
    domains = [domains] if isinstance(domains, str) else domains
    for domain in domains:
        if url.netloc == domain:
            return True
    return False


def url(url, domains=None, query=None):
    query = [query] if isinstance(query, str) else query or []

    url = urllib.parse.urlparse(url)
    qry = urllib.parse.parse_qsl(url.query)
    if query:
        qry = [(k, v) for k, v in qry if k in query]
    qry = sorted(qry)

    checks = [
        check_domains(url, domains),
    ]
    if not all(checks):
        return None

    url = url._replace(query=urllib.parse.urlencode(qry), fragment='')
    return urllib.parse.urlunparse(url)
