import os
import tempfile
import requests
import datetime

from django.conf import settings
from rest_framework.views import APIView
from rest_framework.response import Response
from rest_framework import status

from google.cloud import storage

from .models import Examen, FragmentoExamen
from .utils import split_edf_file, upload_blob


class CrearExamenAPIView(APIView):

    def post(self, request):
        try:
            data = request.data
            id_paciente = data.get("id_paciente")
            id_examen = data.get("id_examen")
            ubicacion_examen = data.get("ubicacion_examen")
            partes = data.get("partes", 3)

            if not all([id_paciente, id_examen, ubicacion_examen]):
                return Response(
                    {"error": "Faltan campos obligatorios: id_paciente, id_examen o ubicacion_examen"},
                    status=status.HTTP_400_BAD_REQUEST
                )

            # 1. Crear registro en BD (con URL pública o firmada)
            examen = Examen.objects.create(
                id_paciente=id_paciente,
                id_examen=id_examen,
                ubicacion_examen=ubicacion_examen
            )

            # 2. Descargar el archivo EDF desde GCS con Google Cloud Storage client
            # Extraer blob_name de la URL guardada:
            bucket_name = settings.GCS_BUCKET_NAME
            prefix = f"https://storage.googleapis.com/{bucket_name}/"
            try:
                blob_name = ubicacion_examen.split(prefix)[-1]
            except Exception:
                return Response(
                    {"error": "Error al parsear la URL del examen."},
                    status=status.HTTP_400_BAD_REQUEST
                )

            client = storage.Client()
            bucket = client.bucket(bucket_name)
            blob = bucket.blob(blob_name)

            # Descarga a archivo temporal local
            temp_file_path = os.path.join(tempfile.gettempdir(), f"{id_examen}.edf")
            blob.download_to_filename(temp_file_path)

            # 3. Dividir el EDF en fragmentos
            fragmentos = split_edf_file(temp_file_path, partes)

            urls_fragmentos = []
            for i, fragmento_path in enumerate(fragmentos, start=1):
                # 4. Subir cada fragmento al bucket
                fragmento_nombre = f"{id_examen}/fragmento_{i}.edf"
                fragmento_url = upload_blob(fragmento_path, fragmento_nombre)

                # 5. Guardar fragmento en BD
                FragmentoExamen.objects.create(
                    examen=examen,
                    numero_fragmento=i,
                    total_fragmentos=partes,
                    ubicacion_fragmento=fragmento_url
                )

                # 6. Llamar a la API externa
                #requests.post(
                #    "https://api.ejemplo.com/procesar_fragmento",
                #    json={
                #        "id_paciente": id_paciente,
                #        "id_examen": id_examen,
                #        "numero_fragmento": i,
                #        "total_fragmentos": partes,
                #        "ubicacion_fragmento": fragmento_url
                #
                #    }
                #)

                urls_fragmentos.append(fragmento_url)

            # 7. Limpieza de archivos locales
            os.remove(temp_file_path)
            for ruta in fragmentos:
                os.remove(ruta)

            return Response(
                {
                    "mensaje": "Examen procesado correctamente",
                    "fragmentos_subidos": urls_fragmentos
                },
                status=status.HTTP_201_CREATED
            )

        except Exception as e:
            return Response({"error": str(e)}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)
