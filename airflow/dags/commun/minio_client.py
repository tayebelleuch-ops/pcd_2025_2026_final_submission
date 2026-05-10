
import s3fs
import os

def get_fs():
    """
    Retourne un objet S3FileSystem configuré pour se connecter à MinIO.
    
    Cette fonction centralise la configuration de connexion MinIO pour
    éviter la duplication de code dans les modules d'extraction, 
    normalisation et chargement.
    
    SECURITE : Les identifiants sont maintenant lus depuis les variables d'environnement.
    
    Returns:
        s3fs.S3FileSystem: Instance configurée pour MinIO
    """

    # Lecture des configurations depuis l'environnement (défini dans docker-compose)
    # SECURITE : On ne met PAS de valeur par défaut pour les secrets ici.
    # Si la variable n'existe pas, cela vaut None (et la connexion échouera, ce qui est mieux que de leaker un secret)
    minio_endpoint = os.getenv("MINIO_ENDPOINT")
    minio_key = os.getenv("MINIO_ACCESS_KEY")
    minio_secret = os.getenv("MINIO_SECRET_KEY")

    if not minio_key or not minio_secret:
        logger.critical("ERREUR : Les identifiants MinIO (MINIO_ACCESS_KEY, MINIO_SECRET_KEY) ne sont pas définis !")
        raise ValueError("Identifiants MinIO manquants dans les variables d'environnement.")

    return s3fs.S3FileSystem(
        key=minio_key,
        secret=minio_secret,
        client_kwargs={
            "endpoint_url": minio_endpoint
        }, 
    )
