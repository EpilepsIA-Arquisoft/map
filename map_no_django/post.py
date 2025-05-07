import os
import math
import tempfile
from google.cloud import storage
import gcs_settings as settings
from messaging import publish


def post(data: dict) -> None:
    """
    Descarga un archivo .edf, lo divide en fragmentos y publica cada fragmento en RabbitMQ.
    """
    id_paciente = data.get("id_paciente")
    id_examen = data.get("id_examen")
    ubicacion_examen = data.get("ubicacion_examen")

    bucket_name = settings.GCS_BUCKET_NAME
    prefix = f"https://storage.googleapis.com/{bucket_name}/"
    blob_name = ubicacion_examen.split(prefix)[-1]

    client = storage.Client()
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(blob_name)

    temp_file_path = os.path.join(tempfile.gettempdir(), f"{id_examen}.edf")
    blob.download_to_filename(temp_file_path)

    def handle_fragment(path, idx, total):
        print(f"Subiendo fragmento {idx}/{total}")
        fragmento_nombre = f"{id_examen}_fragmento_{idx}.edf"
        fragmento_url = upload_blob(path, fragmento_nombre)

        mensaje = {
            "id_paciente": id_paciente,
            "id_examen": id_examen,
            "num_fragmento": idx,
            "total_fragmentos": total,
            "ubicacion_fragmento": fragmento_url
        }
        publish(mensaje)
        print(f"✅ Mensaje publicado: {mensaje}")

    fragments, total_parts = split_edf_file(
        temp_file_path,
        part_size_mb=  250,
        on_fragment_created=handle_fragment
    )

    # Limpieza de archivos temporales
    os.remove(temp_file_path)
    for fragment in fragments:
        os.remove(fragment)

    print(f"✅ Fragmentos subidos: {[f for f in fragments]}")


def upload_blob(source_file_name: str, destination_blob_name: str = None) -> str:
    bucket_name = settings.GCS_BUCKET_NAME
    base_folder = getattr(settings, "GCS_BASE_FOLDER", "")

    if not destination_blob_name:
        destination_blob_name = os.path.basename(source_file_name)

    full_dest = os.path.join(base_folder, destination_blob_name).replace("\\", "/")
    client = storage.Client()
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(full_dest)
    precondition = 0
    blob.upload_from_filename(source_file_name, if_generation_match=precondition)

    url = f"https://storage.googleapis.com/{bucket_name}/{full_dest}"
    print(f"✅ Archivo subido: {url}")
    return full_dest


def split_edf_file(file_path: str, part_size_mb: int = 250, on_fragment_created=None):
    """
    Divide un archivo .edf en fragmentos de hasta `part_size_mb` MB.
    Llama a `on_fragment_created(path, idx, total)` tras crear cada fragmento.
    """
    part_bytes = part_size_mb * 1024 * 1024
    with open(file_path, 'rb') as f:
        f.seek(0, os.SEEK_END)
        total_size = f.tell()

    total_parts = math.ceil(total_size / part_bytes)
    fragments = []

    with open(file_path, 'rb') as f:
        for i in range(total_parts):
            chunk = f.read(part_bytes)
            if not chunk:
                break
            temp_path = os.path.join(
                tempfile.gettempdir(),
                f"fragmento_{i+1}_{os.path.basename(file_path)}"
            )
            with open(temp_path, 'wb') as out:
                out.write(chunk)
            fragments.append(temp_path)
            if on_fragment_created:
                on_fragment_created(temp_path, i+1, total_parts)
            else:
                print(f"➡️ Fragmento {i+1}/{total_parts} creado: {temp_path}")

    return fragments, total_parts


#data = {
#    "id_paciente": "12345",
#    "id_examen": "test101",
#    "ubicacion_examen": "https://storage.googleapis.com/examenes-eeg/examenes/p15_Record4.edf"
#}
#post(data)