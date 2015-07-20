from __future__ import absolute_import
import regex as re

from dossier.fc import StringCounter
from urlparse import urlparse


path_prefixes = r'''user|users|Users|home|data/media|var/users|u01''' + \
                r'''|Documents and Settings|WINNT\\Profiles'''

username_re = re.compile(
    r'^((?P<drive>[A-Za-z]):)?(/|\\)(%s)(/|\\)(?P<username>[^/\\$%%]+)' % path_prefixes
)

def usernames(urls):
    '''Take an iterable of `urls` of normalized URL or file paths and
    attempt to extract usernames.  Returns a list.

    '''
    usernames = StringCounter()
    for url, count in urls.items():
        uparse = urlparse(url)
        path = uparse.path
        hostname = uparse.hostname
        m = username_re.match(path)
        if m:
            usernames[m.group('username')] += count
        elif hostname in ['twitter.com', 'www.facebook.com']:
            usernames[path.lstrip('/')] += count
    return usernames
