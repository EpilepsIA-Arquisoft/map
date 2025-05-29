import os
import shutil
from datetime import datetime
from google.cloud import storage
from google.oauth2 import service_account

def get_storage_client():
    """
    Obtiene el cliente de Storage con las credenciales configuradas.
    """
    try:
        # Ruta al archivo de credenciales
        credentials_path = "C:/Users/juanf/s4-g6-sprint4-57b0584665d6.json"
        
        if not os.path.exists(credentials_path):
            raise Exception(f"El archivo de credenciales no existe en: {credentials_path}")
        
        # Cargar las credenciales
        credentials = service_account.Credentials.from_service_account_file(credentials_path)
        
        # Crear el cliente con las credenciales
        return storage.Client(credentials=credentials)
    except Exception as e:
        raise Exception(f"Error al crear el cliente de Storage: {str(e)}")

def check_bucket_exists(bucket_name: str) -> bool:
    """
    Verifica si el bucket existe.
    """
    print(f"Verificando existencia del bucket: {bucket_name}")
    try:
        client = get_storage_client()
        bucket = client.bucket(bucket_name)
        exists = bucket.exists()
        if exists:
            print(f"✅ Bucket encontrado: {bucket_name}")
        else:
            print(f"❌ Bucket no encontrado: {bucket_name}")
        return exists
    except Exception as e:
        print(f"❌ Error al verificar el bucket: {str(e)}")
        return False

def upload_exam(exam_path: str) -> str:
    """
    Sube un examen al bucket examenes_pull en la carpeta examenes_clinica.
    Retorna la URL del examen subido.
    """
    bucket_name = "examenes_pull"
    folder_name = "examenes_clinica"
    
    # Verificar que el bucket existe
    if not check_bucket_exists(bucket_name):
        raise Exception(f"El bucket {bucket_name} no existe")
    
    # Verificar que el archivo existe y es un .edf
    if not os.path.exists(exam_path):
        raise Exception(f"El archivo no existe: {exam_path}")
    if not exam_path.lower().endswith('.edf'):
        raise Exception(f"El archivo no es un .edf: {exam_path}")
    
    # Obtener solo el nombre del archivo sin la ruta
    exam_name = os.path.basename(exam_path)
    
    try:
        # Inicializar el cliente de Storage
        client = get_storage_client()
        bucket = client.bucket(bucket_name)
        
        # Crear el blob (archivo en el bucket)
        blob_path = f"{folder_name}/{exam_name}"
        blob = bucket.blob(blob_path)
        
        # Subir el archivo
        print(f"Subiendo archivo {exam_name}...")
        blob.upload_from_filename(exam_path)
        
        # Construir la URL del examen en el bucket
        exam_url = f"https://storage.googleapis.com/{bucket_name}/{blob_path}"
        print(f"✅ Examen subido exitosamente a: {exam_url}")
        return exam_url
        
    except Exception as e:
        raise Exception(f"Error al subir el archivo: {str(e)}")

def test_upload_exams():
    """
    Función de prueba que sube todos los exámenes .edf de la carpeta examenes_upload/
    """
    bucket_name = "examenes_pull"
    upload_dir = "examenes_upload"
    
    # Verificar que el bucket existe
    if not check_bucket_exists(bucket_name):
        raise Exception(f"El bucket {bucket_name} no existe")
    
    # Verificar que la carpeta existe
    if not os.path.exists(upload_dir):
        raise Exception(f"La carpeta {upload_dir} no existe")
    
    # Obtener todos los archivos .edf de la carpeta
    edf_files = [f for f in os.listdir(upload_dir) if f.lower().endswith('.edf')]
    
    if not edf_files:
        print(f"No se encontraron archivos .edf en la carpeta {upload_dir}")
        return []
    
    uploaded_exams = []
    
    for exam_file in edf_files:
        try:
            exam_path = os.path.join(upload_dir, exam_file)
            exam_url = upload_exam(exam_path)
            uploaded_exams.append({
                "name": exam_file,
                "url": exam_url,
                "timestamp": datetime.now().isoformat()
            })
            print(f"✅ Examen {exam_file} procesado correctamente")
        except Exception as e:
            print(f"❌ Error al procesar examen {exam_file}: {str(e)}")
    
    print("\nResumen de exámenes subidos:")
    for exam in uploaded_exams:
        print(f"- {exam['name']}: {exam['url']}")
    
    return uploaded_exams

# Ejecutar la función directamente
print("Iniciando proceso de subida de exámenes...")
test_upload_exams()
print("Proceso completado.")
    