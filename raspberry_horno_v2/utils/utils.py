# utils/utils.py
from datetime import datetime

def get_current_date_formatted():
    """Obtiene la fecha actual formateada."""
    return datetime.now().strftime('%Y-%m-%d')

def get_current_time_formatted():
    """Obtiene la hora actual formateada."""
    return datetime.now().strftime('%H:%M:%S')

def get_current_year_short():
    """Obtiene los últimos dos dígitos del año actual."""
    return datetime.now().strftime("%y")
