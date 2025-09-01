import os

def get_flag(name: str, default: bool = False) -> bool:
    v=os.getenv(name,'')
    return v.strip().lower() in ('1','true','on','yes') if v else default
