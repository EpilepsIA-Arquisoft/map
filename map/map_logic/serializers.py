from rest_framework import serializers
from .models import Examen, FragmentoExamen

class FragmentoExamenSerializer(serializers.ModelSerializer):
    class Meta:
        model = FragmentoExamen
        fields = ("numero_fragmento", "total_fragmentos", "ubicacion_fragmento")


class ExamenSerializer(serializers.ModelSerializer):
    fragmentos = FragmentoExamenSerializer(many=True, read_only=True)

    class Meta:
        model = Examen
        fields = ("id_paciente", "id_examen", "ubicacion_examen", "fragmentos")
        read_only_fields = ("ubicacion_examen", "fragmentos")
