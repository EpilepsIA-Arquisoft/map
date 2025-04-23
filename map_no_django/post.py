import os
import math
import tempfile
from google.cloud import storage
import gcs_settings as settings

# from start import publish

def post(data):
    id_paciente = data.get("id_paciente")
    id_examen = data.get("id_examen")
    ubicacion_examen = data.get("ubicacion_examen")

    #
    bucket_name = settings.GCS_BUCKET_NAME
    prefix = f"https://storage.googleapis.com/{bucket_name}/"
    blob_name = ubicacion_examen.split(prefix)[-1]

    client = storage.Client()
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(blob_name)

    temp_file_path = os.path.join(tempfile.gettempdir(), f"{id_examen}.edf")
    blob.download_to_filename(temp_file_path)

    urls_fragmentos = []

    def handle_fragment(fragmento_path, idx, total):
        print(f"Subiendo fragmento {idx}/{total}")
        fragmento_nombre = f"{id_examen}/fragmento_{idx}.edf"
        fragmento_url = upload_blob(fragmento_path, fragmento_nombre)

        out = {
            "id_paciente": id_paciente,
            "id_examen": id_examen,
            "numero_fragmento": idx,
            "total_fragmentos": total,
            "ubicacion_fragmento": fragmento_url
        }
        from start import publish
        publish(out)

        urls_fragmentos.append(fragmento_url)

    # 3. Dividir el archivo y subir cada fragmento tan pronto se genera
    fragmentos, total_partes = split_edf_file(
        temp_file_path,
        on_fragment_created=handle_fragment
    )

    # 4. Limpiar archivos temporales
    os.remove(temp_file_path)
    for ruta in fragmentos:
        os.remove(ruta)

    print(f"✅ Fragmentos subidos: {urls_fragmentos}")


def upload_blob(source_file_name, destination_blob_name=None):
    bucket_name = settings.GCS_BUCKET_NAME
    base_folder = getattr(settings, "GCS_BASE_FOLDER", "")

    if not destination_blob_name:
        destination_blob_name = os.path.basename(source_file_name)

    full_destination = os.path.join(base_folder, destination_blob_name).replace("\\", "/")

    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(full_destination)

    generation_match_precondition = 0
    blob.upload_from_filename(source_file_name, if_generation_match=generation_match_precondition)

    file_url = f"https://storage.googleapis.com/{bucket_name}/{full_destination}"
    print(f"✅ Archivo subido: {file_url}")
    return file_url


def split_edf_file(file_path, part_size_mb=1, on_fragment_created=None):
    """
    Divide un archivo .edf en partes de hasta `part_size_mb` MB.
    Llama a `on_fragment_created(path, idx, total)` tras crear cada fragmento.

    :return: (lista_rutas_fragmentos, total_partes)
    """
    part_size_bytes = part_size_mb * 1024 * 1024

    # Calcular tamaño total y número de partes
    with open(file_path, 'rb') as f:
        f.seek(0, os.SEEK_END)
        total_size = f.tell()

    total_parts = math.ceil(total_size / part_size_bytes)

    fragments = []

    with open(file_path, 'rb') as f:
        for i in range(total_parts):
            chunk = f.read(part_size_bytes)
            if not chunk:
                break

            temp_path = os.path.join(
                tempfile.gettempdir(),
                f"fragmento_{i+1}_{os.path.basename(file_path)}"
            )
            with open(temp_path, 'wb') as pf:
                pf.write(chunk)

            fragments.append(temp_path)

            if on_fragment_created:
                on_fragment_created(temp_path, i+1, total_parts)
            else:
                print(f"➡️ Fragmento {i+1}/{total_parts} creado: {temp_path}")

    return fragments, total_parts



