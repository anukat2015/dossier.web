from __future__ import absolute_import
import pytest

from dossier.fc import StringCounter

from dossier.extraction import usernames

example_usernames_from_paths = [
     (r'http://www.example.com/user/folder3/index.html?source=dummy', 'folder3', 3),
     (r'http://www.example.com/user/myaccount', 'myaccount', 2),
     (r'http://www.different.com/folder3', None, 4),
     (r'http://www.different.com/user/myaccount', 'myaccount', 7),
     (r'http://www.also.com/user', None, 23),
     (r'http://www.also2.com/user/user', 'user', 1),
     (r'http://frob.com/user/my_account/media/Dresses/hi.jpg', 'my_account', 1),
     (r'https://www.facebook.com/my_account', 'my_account', 1),
     (r'https://twitter.com/my_account', 'my_account', 1),
     (r'C:\WINNT\Profiles\myaccount%MyUserProfile%', 'myaccount', 3), # Microsoft Windows NT
     (r'C:\WINNT\Profiles\myaccount', 'myaccount', 3), # Microsoft Windows NT
     (r'd:\WINNT\Profiles\myaccount', 'myaccount', 3), # Microsoft Windows NT
     (r'X:\Documents and Settings\myaccount', 'myaccount', 8), # Microsoft Windows 2000, XP and 2003
     (r'C:\Users\myaccount', 'myaccount', 3), # Microsoft Windows Vista, 7 and 8
     (r'C:\Users\myaccount\dog', 'myaccount', 3), # Microsoft Windows Vista, 7 and 8
     (r'C:\Users\whg\Desktop\Plug\FastGui(LYT)\Shell\Release\Shell.pdb', 'whg', 2),
     (r'C:\Documents and Settings\whg\\Plug\FastGui(LYT)\Shell\Release\Shell.pdb', 'whg', 3),
     (r'C:\Users\whg\Desktop\Plug\FastGui(LYT)\Shell\Release\Shell.pdb', 'whg', 3),
     (r'/home/myaccount$HOME', 'myaccount', 5), # Unix-Based
     (r'/var/users/myaccount', 'myaccount', 3), # Unix-Derived
     (r'/u01/myaccount', 'myaccount', 3), # Unix-Derived
     (r'/user/myaccount', 'myaccount', 3), # Unix-Derived
     (r'/users/myaccount', 'myaccount', 3), # Unix-Derived
     (r'/var/users/myaccount', 'myaccount', 3), # Unix-Derived
     (r'/home/myaccount', 'myaccount', 3), # Linux / BSD (FHS)
     (r'/Users/my_account$HOME', 'my_account', 5), # Mac OS X
     (r'/Users/my_account', 'my_account', 5), # Mac OS X
     (r'/data/media/myaccount', 'myaccount', 5) # Android
     ]

@pytest.mark.parametrize(
    ('url_or_path', 'username', 'count'),
    example_usernames_from_paths
)
def test_usernames(url_or_path, username, count):
    urls = StringCounter()
    urls[url_or_path] += count

    if username is not None:
        results = usernames(urls)
        assert results == StringCounter({username: count})

