#
# util methods for reading database credentials
#

def read_credentials_file(filename:str) -> dict:
    creds = dict()
    with open(filename) as FILE:
        for line in FILE:
            line = line.strip()
            # skip empty strings
            if not line:
                continue
            # skip comments
            if line.startswith('#'):
                continue
            (key, value) = line.split('=', 1)
            key = key.strip()
            value = value.strip()
            creds[key] = value
    return creds
