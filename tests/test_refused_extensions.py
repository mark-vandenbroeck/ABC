import os
import tempfile
import json
import pytest
import sys
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from app import app
import database

@pytest.fixture
def client(tmp_path, monkeypatch):
    # Use a temporary sqlite DB for tests
    dbfile = tmp_path / "test_crawler.db"
    monkeypatch.setattr(database, 'DB_PATH', str(dbfile))
    # Initialize schema
    database.init_database()

    app.config['TESTING'] = True
    with app.test_client() as c:
        yield c


def test_add_and_list_refused_extension(client):
    # Add extension
    rv = client.post('/api/refused-extensions', json={'extension': 'exe', 'reason': 'binary'})
    assert rv.status_code == 200
    j = rv.get_json()
    assert j['status'] == 'ok'

    # List and verify
    rv = client.get('/api/refused-extensions')
    assert rv.status_code == 200
    j = rv.get_json()
    exts = j.get('refused_extensions')
    assert any(e['extension'] == 'exe' and e['reason'] == 'binary' for e in exts)


def test_add_url_rejected_by_refused_extension(client):
    # ensure the extension is configured as refused
    client.post('/api/refused-extensions', json={'extension': 'exe', 'reason': 'binary'})

    url = 'http://example.com/file.exe'
    rv = client.post('/api/urls', json={'url': url})
    assert rv.status_code == 400
    j = rv.get_json()
    assert 'refused' in j['message'].lower()


def test_delete_refused_extension_allows_url(client):
    # ensure extension is present
    client.post('/api/refused-extensions', json={'extension': 'zip', 'reason': 'no'})
    rv = client.get('/api/refused-extensions')
    exts = rv.get_json().get('refused_extensions')
    assert any(e['extension'] == 'zip' for e in exts)

    # Delete
    rv = client.delete('/api/refused-extensions/zip')
    assert rv.status_code == 200
    j = rv.get_json()
    assert j['status'] == 'ok'

    # Now adding URL with .zip should succeed
    url = 'http://example.com/archive.zip'
    rv = client.post('/api/urls', json={'url': url})
    assert rv.status_code == 200
    j = rv.get_json()
    assert j['status'] == 'ok'