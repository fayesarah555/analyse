import os

# Configuration de base
SECRET_KEY = os.environ.get('SUPERSET_SECRET_KEY', 'votre_clé_secrète_ici')
ENABLE_PROXY_FIX = True

# Configuration de la base de données (utilise SQLite par défaut)
SQLALCHEMY_DATABASE_URI = 'sqlite:////app/superset_home/superset.db'

# Désactiver le CSRF pour simplifier l'utilisation initiale (à ne pas faire en production)
WTF_CSRF_ENABLED = False

# Configuration du cache
CACHE_CONFIG = {
    'CACHE_TYPE': 'SimpleCache',
    'CACHE_DEFAULT_TIMEOUT': 300
}

# Configuration de Trino
TRINO_BINARY = '/app/trino'
