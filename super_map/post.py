import os
import math
import tempfile
import requests
from datetime import datetime
from start import publish

def post(data: dict) -> None:
    """
    Descarga un archivo .edf, lo divide en fragmentos y publica cada fragmento en el broker de mensajería.
    """
    id_paciente = data.get("id_paciente")
    id_examen = data.get("id_examen")
    ubicacion_examen = data.get("ubicacion_examen")
    part_size_mb = data.get("part_size_mb", 250)  # Tamaño por defecto: 250MB

    print(f"Descargando archivo desde: {ubicacion_examen}")
    
    # Descargar el archivo
    response = requests.get(ubicacion_examen)
    if response.status_code != 200:
        raise Exception(f"Error al descargar el archivo: {response.status_code}")
    
    temp_file_path = os.path.join(tempfile.gettempdir(), f"{id_examen}.edf")
    with open(temp_file_path, 'wb') as f:
        f.write(response.content)
    
    print(f"Archivo descargado exitosamente a: {temp_file_path}")

    def handle_fragment(path, idx, total):
        print(f"Subiendo fragmento {idx}/{total}")
        fragmento_nombre = f"{id_examen}_fragmento_{idx}.edf"
        fragmento_url = f"https://storage.googleapis.com/super-map-bucket/{fragmento_nombre}"

        mensaje = {
            "id_paciente": id_paciente,
            "id_examen": id_examen,
            "num_fragmento": idx,
            "total_fragmentos": total,
            "ubicacion_fragmento": fragmento_url,
            "timestamp": datetime.now().isoformat()
        }
        publish(mensaje)
        print(f"Mensaje publicado: {mensaje}")

    # Dividir el archivo en fragmentos
    fragments, total_parts = split_edf_file(
        temp_file_path,
        part_size_mb=part_size_mb,
        on_fragment_created=handle_fragment
    )

    # Limpieza de archivos temporales
    os.remove(temp_file_path)
    for fragment in fragments:
        os.remove(fragment)

    print(f"Fragmentos procesados: {len(fragments)}")
    print("Proceso completado exitosamente")


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
                print(f"Fragmento {i+1}/{total_parts} creado: {temp_path}")

    return fragments, total_parts


# Ejemplo de uso:
data = {
    "id_paciente": "12345",
    "id_examen": "test101",
    "ubicacion_examen": "https://storage.googleapis.com/super-map-bucket/examenes/p15_Record4.edf",
    "part_size_mb": 250  # Tamaño opcional de los fragmentos en MB
}
post(data)
