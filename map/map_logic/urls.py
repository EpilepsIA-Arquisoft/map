from django.urls import path
from .views import CrearExamenAPIView

urlpatterns = [
    path('examen/', CrearExamenAPIView.as_view(), name='crear-examen'),
]
