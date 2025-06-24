"""

"""
from sys import argv
from os import environ
from re import sub
from json import loads

from airflow_se.utils import show_logo
from airflow_se.crypt import encrypt, decrypt
from airflow_se.commons import CLEAR_MASK_FOR_AIRFLOW_SE, SECMAN_KEY_FOR_SECRET

__all__ = ["run", ]

def run():
    show_logo()
    print(f"{'*' * 80}")
    res, nth, err, se = dict(), 0, 0, environ.get("AIRFLOW_SE")
    if se:
        try:
            se = sub(CLEAR_MASK_FOR_AIRFLOW_SE, "", se)
            environ.update(loads(decrypt(se)))
        except:
            err += 1
            print(f"ERROR: Don't convert value from `AIRFLOW_SE`")
            raise
        from airflow_se.secman import auth_secman, get_secman_data, push_secman_data
        token = auth_secman()
        sm_data = get_secman_data(SECMAN_KEY_FOR_SECRET, token) or dict()
        # если есть DELETE-NON-EXISTING, то не сохраняем то, что есть в СекМане
        for x in argv[1:]:
            if x.strip().upper() == "DELETE-NON-EXISTING":
                sm_data.clear()
                break
        for x in argv[1:]:
            if x.strip().upper() == "DELETE-NON-EXISTING":
                continue
            nth += 1
            k, v = x.split("=", 1)
            k, v = k.strip("\" '"), v.strip("\" '")
            if k and v:
                if v.upper().startswith("FILE:"):
                    fn = v[5:].strip("\" '")
                    try:
                        with open(fn, "rb") as f:
                            sm_data[k] = encrypt(f.read())
                    except Exception as r:
                        err += 1
                        print(f"ERROR: Don't read file `{fn}`:\n{r}")
                else:
                    sm_data[k] = encrypt(v)
            else:
                err += 1
                print(f"ERROR: Invalid parameter number {nth} // key={k}; value={v}")
        if push_secman_data(SECMAN_KEY_FOR_SECRET, sm_data, token):
            print("Data to SecMan pushed")
        else:
            err += 1
            print("ERROR: Data to SecMan DON'T pushed")
    else:
        err += 1
        print("ERROR: Environment variable `AIRFLOW_SE` is not set")
    print(f"{'*' * 80}")
    return err

