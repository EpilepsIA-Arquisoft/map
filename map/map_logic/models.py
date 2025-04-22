from django.db import models

class Examen(models.Model):
    id_paciente = models.CharField(max_length=100)
    id_examen = models.CharField(max_length=100)
    ubicacion_examen = models.URLField()

class FragmentoExamen(models.Model):
    examen = models.ForeignKey(Examen, related_name="fragmentos", on_delete=models.CASCADE)
    numero_fragmento = models.PositiveIntegerField()
    total_fragmentos = models.PositiveIntegerField()
    ubicacion_fragmento = models.URLField()

    class Meta:
        unique_together = ("examen", "numero_fragmento")
