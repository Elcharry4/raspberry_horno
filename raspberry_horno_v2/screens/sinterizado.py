from kivymd.uix.screen import MDScreen
from kivy.clock import Clock
from kivy.properties import NumericProperty
from kivymd.uix.list import OneLineListItem
from db import db
from utils.temperature_reader import TemperatureReader
from utils import utils

class SinterizadoScreen(MDScreen):
    temperature_reader = None
    temperature_event = None
    potencia_seteada = NumericProperty(0)

    def on_enter(self):
        # Inicializar el lector de temperatura
        self.temperature_reader = TemperatureReader()
        # Programar la actualización de temperatura cada segundo
        self.temperature_event = Clock.schedule_interval(self.update_temperature_label, 1)
        # Cargar los datos al entrar en la pantalla
        self.cargar_datos()

    def on_leave(self):
        # Cancelar la actualización cuando se salga de la pantalla
        if self.temperature_event:
            self.temperature_event.cancel()

    def update_temperature_label(self, dt):
        temperatura = self.temperature_reader.read_temperature()
        if temperatura is not None:
            self.ids.temperatura_label.text = f"Temperatura: {temperatura:.2f} °C"
        else:
            self.ids.temperatura_label.text = "Error al leer la temperatura."

    def on_potencia_value(self, instance, value):
        self.potencia_seteada = int(value)
        self.ids.potencia_label.text = f"Potencia: {self.potencia_seteada}%"

    def ajustar_potencia(self, delta):
        nueva_potencia = self.potencia_seteada + delta
        if nueva_potencia < 0:
            nueva_potencia = 0
        if nueva_potencia > 100:
            nueva_potencia = 100
        self.set_potencia(nueva_potencia)

    def set_potencia(self, valor):
        self.ids.control_de_potencia.value = valor
        self.potencia_seteada = valor
        self.ids.potencia_label.text = f"Potencia: {valor}%"

    def on_potencia_value(self, instance, value):
        self.potencia_seteada = int(value)
        self.ids.potencia_label.text = f"Potencia: {self.potencia_seteada}%"

        # Guardar los datos en la base de datos
        temperatura = self.temperature_reader.read_temperature()
        if temperatura is None:
            temperatura = 0.0  # Manejar error de lectura

        data = {
            'fecha': utils.get_current_date_formatted(),
            'hora': utils.get_current_time_formatted(),
            'temperatura_actual': f"{temperatura:.2f}",
            'potencia_seteada': f"{self.potencia_seteada}"
        }
        db.save_record('datos_sinterizado', data, self.on_data_saved)

    def on_data_saved(self, success, error, inserted_id):
        """Callback para manejar el resultado del guardado."""
        if success:
            print(f"Datos guardados correctamente con ID {inserted_id}")
            self.cargar_datos()
        else:
            print(f"Error al guardar los datos: {error}")

    def cargar_datos(self):
        """Cargar los últimos 20 registros desde la base de datos."""
        def callback(records, error):
            if error:
                print(f"Error al cargar datos: {error}")
            else:
                # Mover la manipulación de widgets al hilo principal
                def actualizar_ui(dt):
                    self.ids.lista_datos.clear_widgets()
                    for record in records:
                        item_text = f"Fecha: {record[1]}, Hora: {record[2]}, Temp: {record[3]}, Potencia: {record[4]}"
                        self.ids.lista_datos.add_widget(OneLineListItem(text=item_text))
                Clock.schedule_once(actualizar_ui)

        db.fetch_last_records('datos_sinterizado', 20, callback)
