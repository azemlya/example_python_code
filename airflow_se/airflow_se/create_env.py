"""

"""
from sys import argv, stderr
from json import dumps

from airflow_se.crypt import encrypt
from airflow_se.commons import BEGIN_KEY_FOR_AIRFLOW_SE, END_KEY_FOR_AIRFLOW_SE

__all__ = [
    "run",
]

def run():
    res, nth, err = dict(), 0, 0
    if len(argv) > 1:
        for x in argv[1:]:
            nth += 1
            k, v = x.split("=", 1)
            k, v = k.strip("\" '"), v.strip("\" '")
            if k and v:
                res[k] = v
            else:
                err += 1
                print(f"Invalid parameter number {nth} // key={k}; value={v}", file=stderr)
        if err != 0:
            print(f"There are errors({err}), conversion to is possible", file=stderr)
        else:
            if len(res) > 0:
                _res = encrypt(dumps(res))
                n = 40
                chunks = "\n".join([_res[i:i + n] for i in range(0, len(_res), n)])
                _res_fin = f"""{BEGIN_KEY_FOR_AIRFLOW_SE}\n{chunks}\n{END_KEY_FOR_AIRFLOW_SE}"""
                print(_res_fin)
            else:
                err += 1
                print("The dictionary is empty, there is nothing to convert", file=stderr)
    else:
        err += 1
        print("Don't set parameters, must be key1=value1 key2=value2 ... keyN=valueN", file=stderr)
    return err

