from honeybadgerbft.core.reliablebroadcast import merkleTree, getMerkleBranch, merkleVerify
from honeybadgerbft.core.reliablebroadcast import encode, decode
import hashlib, pickle
from crypto.ecdsa.ecdsa import ecdsa_vrfy, ecdsa_sign

def hash(x):
    return hashlib.sha256(pickle.dumps(x)).digest()

def prbc_validate(sid, N, f, PK2s, proof):
    try:
        _sid, roothash, sigmas = proof
        assert _sid == sid
        assert len(sigmas) == N - f and len(set(sigmas)) == N - f
        digest = hash((sid, roothash))
        for (i, sig_i) in sigmas:
            assert ecdsa_vrfy(PK2s[i], digest, sig_i)
        return True
    except:
        return False

def cbc_validate():
    pass