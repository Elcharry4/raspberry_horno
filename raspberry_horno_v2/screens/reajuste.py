# screens/reajuste.py

from kivymd.uix.screen import MDScreen
from kivy.properties import NumericProperty, StringProperty
from kivymd.toast import toast
from kivy.clock import Clock
from db import db
from utils import utils
from utils.temperature_reader import TemperatureReader

class ReajusteScreen(MDScreen):
    acero_1010 = NumericProperty(0)
    carbono = NumericProperty(0)
    silicio = NumericProperty(0)
    record_id = NumericProperty(0)
    base_text = StringProperty("")
    numero_colada_formateado = StringProperty("")
    temperature_reader = None
    temperature_event = None
    potencia_seteada = NumericProperty(0)

    def on_enter(self):
        # Inicializar el lector de temperatura
        self.temperature_reader = TemperatureReader()
        # Programar la actualización de temperatura cada segundo
        self.temperature_event = Clock.schedule_interval(self.update_temperature_label, 1)

        def on_leave(self):
            # Cancelar la actualización cuando se salga de la pantalla
            if self.temperature_event:
                self.temperature_event.cancel()

    def set_record_id(self, record_id):
        self.record_id = record_id

    def set_base_text(self, base_text):
        self.base_text = base_text

    def set_numero_colada(self, numero_colada):
        self.numero_colada_formateado = numero_colada

    def incrementar_acero(self):
        self.acero_1010 += 5
        self.ids.acero_input.text = f"{self.acero_1010} KG"

    def decrementar_acero(self):
        if self.acero_1010 >= 5:
            self.acero_1010 -= 5
            self.ids.acero_input.text = f"{self.acero_1010} KG"
        else:
            toast("El valor no puede ser negativo.")

    def incrementar_carbono(self):
        self.carbono += 0.5
        self.ids.carbono_input.text = f"{self.carbono} KG"

    def decrementar_carbono(self):
        if self.carbono >= 0.5:
            self.carbono -= 0.5
            self.ids.carbono_input.text = f"{self.carbono} KG"
        else:
            toast("El valor no puede ser negativo.")

    def incrementar_silicio(self):
        self.silicio += 0.5
        self.ids.silicio_input.text = f"{self.silicio} KG"

    def decrementar_silicio(self):
        if self.silicio >= 0.5:
            self.silicio -= 0.5
            self.ids.silicio_input.text = f"{self.silicio} KG"
        else:
            toast("El valor no puede ser negativo.")

    def continuar(self):
        data = {
            'acero_1010': self.acero_1010,
            'carbono': self.carbono,
            'silicio': self.silicio,
            'hora_inicio_de_colada': utils.get_current_time_formatted()
        }
        # Actualizar el registro existente
        db.update_record('planilla_de_fusion', self.record_id, data, self.on_data_saved)

    def on_data_saved(self, success, error):
        def update_ui(dt):
            if success:
                toast("Datos guardados exitosamente.")
                # Pasar el record_id a la siguiente pantalla
                cucharas_screen = self.manager.get_screen('cucharas_por_material')
                cucharas_screen.set_record_id(self.record_id)
                cucharas_screen.set_base(self.base_text)
                cucharas_screen.set_colada(self.numero_colada_formateado)
                self.manager.current = 'cucharas_por_material'
            else:
                toast("Error al guardar los datos.")
        Clock.schedule_once(update_ui)

    def resetear_estados(self):
        self.acero_1010 = 0
        self.carbono = 0
        self.silicio = 0
        self.record_id = 0
        self.base_text = ""
        self.numero_colada_formateado = ""

        # Resetear los textos de los campos de entrada
        self.ids.acero_input.text = f"{self.acero_1010} KG"
        self.ids.carbono_input.text = f"{self.carbono} KG"
        self.ids.silicio_input.text = f"{self.silicio} KG"

    def set_potencia(self, valor):
        self.ids.control_de_potencia.value = valor
        self.potencia_seteada = valor  # Asegurar que esta línea esté presente
        self.ids.potencia_label.text = f"Potencia: {int(valor)}%"

    def on_potencia_value(self, instance, value):
        self.potencia_seteada = int(value)  # Agregar esta línea
        self.ids.potencia_label.text = f"Potencia: {int(value)}%"

    def guardar_temperatura(self):
        # Leer la temperatura actual
        temperatura = self.temperature_reader.read_temperature()
        if temperatura is not None:
            # Obtener el valor actual del slider de potencia
            potencia = int(self.ids.control_de_potencia.value)
            # Crear un diccionario con los datos
            data = {
                'temperatura_thermocupla': temperatura,
                'potencia_seteada': potencia
            }
            # Guardar en la base de datos
            db.save_record('temperatura_potencia', data, self.on_temperatura_potencia_saved)
        else:
            toast("Error al leer la temperatura.")

    def on_temperatura_potencia_saved(self, success, error, inserted_id):
        def update_ui(dt):
            if success:
                toast(f"Datos guardados con ID {inserted_id}.")
            else:
                toast(f"Error al guardar datos: {error}")
        Clock.schedule_once(update_ui)

    def update_temperature_label(self, dt):
        temperatura = self.temperature_reader.read_temperature()
        if temperatura is not None:
            self.ids.temperatura_label.text = f"Temperatura: {temperatura:.2f} °C"
        else:
            self.ids.temperatura_label.text = "Error al leer la temperatura."

    def ajustar_potencia(self, delta):
        nueva_potencia = self.potencia_seteada + delta
        if nueva_potencia < 0:
            nueva_potencia = 0
        if nueva_potencia > 100:
            nueva_potencia = 100
        self.set_potencia(nueva_potencia)