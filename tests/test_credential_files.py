from util.database_credentials import read_credentials_file


def test_env_file():
    env1 = read_credentials_file('tests/test1.env.txt')
    assert env1['host'] == 'dbserver.example.com'
    assert env1['user'] == 'myuser'
    assert env1['password'] == 'fakepassword'
    assert env1['database'] == 'testdb'
