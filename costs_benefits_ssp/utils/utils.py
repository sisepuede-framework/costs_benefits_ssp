from typing import List, Optional
import os

def build_path(PATH : List[str]) -> str:
    return os.path.abspath(os.path.join(*PATH))

def get_tx_prefix(tx : str, ssp_txs : List[str]) -> Optional[str]:
    """
    Return the first transformation_code in `ssp_txs` that `tx` starts with,
    or `None` if no candidate matches. Unlike the previous version, this
    function does NOT print to stdout when no match is found — the caller
    handles the consolidated report so we avoid hundreds of repeated lines.
    """
    for ssp_tx in ssp_txs:
        if tx.startswith(ssp_tx):
            return ssp_tx
    return None

