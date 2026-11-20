"""The client's bearer: an explicit token, else a token file read per request."""

from aei import AeiClient
from aei import client as client_mod


class _Resp:
    def raise_for_status(self):
        pass

    def json(self):
        return {"phase": "Running"}


class _Session:
    """Records the Authorization header of each GET."""

    def __init__(self):
        self.bearers = []

    def get(self, url, headers=None, timeout=None):
        self.bearers.append(headers["Authorization"].removeprefix("Bearer "))
        return _Resp()


def _bearer(c: AeiClient, s: _Session) -> str:
    c.get("r")
    return s.bearers[-1]


def test_token_path_precedence_and_rotation(tmp_path, monkeypatch):
    opt = tmp_path / "opt"
    opt.write_text("from-option\n")
    env = tmp_path / "env"
    env.write_text("from-env\n")
    monkeypatch.setenv(client_mod.TOKEN_PATH_ENV, str(env))
    s = _Session()

    assert _bearer(AeiClient("http://aei", "a", token_path=str(opt), session=s), s) == "from-option"
    assert _bearer(AeiClient("http://aei", "a", token="explicit", token_path=str(opt), session=s), s) == "explicit"

    c = AeiClient("http://aei", "a", session=s)
    assert _bearer(c, s) == "from-env"
    env.write_text("rotated\n")  # the kubelet rewrites a projected token in place
    assert _bearer(c, s) == "rotated"


def test_default_token_path(monkeypatch):
    monkeypatch.delenv(client_mod.TOKEN_PATH_ENV, raising=False)
    assert AeiClient("http://aei", "a")._token_path == client_mod.DEFAULT_TOKEN_PATH
    monkeypatch.setenv(client_mod.TOKEN_PATH_ENV, "  ")
    assert AeiClient("http://aei", "a")._token_path == client_mod.DEFAULT_TOKEN_PATH
