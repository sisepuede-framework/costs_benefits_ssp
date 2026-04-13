from typing import List, Optional
import os

def build_path(PATH : List[str]) -> str:
    return os.path.abspath(os.path.join(*PATH))

def get_tx_prefix(tx : str, ssp_txs : List[str]) -> Optional[str]:
    """
    Devuelve el primer transformation_code de `ssp_txs` del cual `tx` es un
    prefijo, o `None` si ninguno aplica. A diferencia de la versión previa,
    NO imprime a stdout cuando no hay match — el caller se encarga del
    reporte consolidado para evitar cientos de líneas repetidas.
    """
    for ssp_tx in ssp_txs:
        if tx.startswith(ssp_tx):
            return ssp_tx
    return None

