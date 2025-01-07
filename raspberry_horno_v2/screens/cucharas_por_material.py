# screens/cucharas_por_material.py

from kivymd.uix.screen import MDScreen
from kivy.properties import NumericProperty, StringProperty, DictProperty, ListProperty
from kivymd.toast import toast
from kivy.clock import Clock
from db import db
from utils import utils
from utils.temperature_reader import TemperatureReader


class CucharasPorMaterialScreen(MDScreen):
    record_id = NumericProperty(0)
    base_text = StringProperty("")
    colada_text = StringProperty("")
    materiales = ListProperty([])
    cantidades = DictProperty({})
    temperature_reader = None
    temperature_event = None
    potencia_seteada = NumericProperty(0)
    cuchara_general_contador = NumericProperty(0)  # Contador general de cucharas

    def on_enter(self):
        # Inicializar el lector de temperatura
        self.temperature_reader = TemperatureReader()
        # Programar la actualización de temperatura cada segundo
        self.temperature_event = Clock.schedule_interval(self.update_temperature_label, 1)

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

    def set_record_id(self, record_id):
        self.record_id = record_id

    def set_base(self, base_text):
        self.base_text = base_text
        self.cargar_materiales()

    def set_colada(self, colada_text):
        self.colada_text = colada_text

    def cargar_materiales(self):
        # Definir los materiales por base
        materiales_por_base = {
            "Base 2": ["Material 1", "Material 2", "Material 3", "Material 5"],
            "Base 5": ["Material 1", "Material 3", "Material 5", "Material 7"],
            "Base 8": ["Material 8", "Material 10", "Material 12"],
            "Base 10": ["Material 10", "Material 12"],
        }

        # Obtener los materiales correspondientes a la base seleccionada
        self.materiales = materiales_por_base.get(self.base_text, [])
        self.cantidades = {material: 0 for material in self.materiales}

        # Construir la interfaz dinámica
        Clock.schedule_once(self.construir_interfaz)

    def construir_interfaz(self, dt):
        # Limpiar el layout
        self.ids.materiales_box.clear_widgets()

        for material in self.materiales:
            from kivy.lang import Builder

            input_id = f'input_{material.replace(" ", "_")}'

            kv_string = f'''
BoxLayout:
    orientation: 'vertical'
    size_hint_y: None
    height: dp(100)
    spacing: dp(5)
    padding: dp(5)
    MDLabel:
        text: "{material}"
        font_style: "H6"
        halign: 'center'
        text_color: 0, 0, 0, 1
    BoxLayout:
        orientation: 'horizontal'
        size_hint_y: None
        height: dp(48)
        spacing: dp(10)
        pos_hint: {{'center_x': 0.5}}
        size_hint_x: 0.8

        MDIconButton:
            icon: "minus"
            size_hint: None, None
            width: dp(50)
            height: dp(50)
            icon_size: dp(40)
            on_release: app.root.get_screen('cucharas_por_material').decrementar_material("{material}")

        MDTextField:
            id: {input_id}
            text: str(app.root.get_screen('cucharas_por_material').cantidades["{material}"])
            readonly: True
            font_size: '18sp'
            theme_text_color: "Custom"
            text_color: 0, 0, 0, 1
            halign: 'center'
            text_color_normal: "black"

        MDIconButton:
            icon: "plus"
            size_hint: None, None
            width: dp(50)
            height: dp(50)
            icon_size: dp(40)
            on_release: app.root.get_screen('cucharas_por_material').incrementar_material("{material}")
'''

            item = Builder.load_string(kv_string)
            self.ids.materiales_box.add_widget(item)
            self.ids[input_id] = item.ids[input_id]

    def incrementar_material(self, material):
        self.cantidades[material] += 1
        input_id = f'input_{material.replace(" ", "_")}'
        if input_id in self.ids:
            self.ids[input_id].text = str(self.cantidades[material])

        # Incrementar el contador general de cucharas
        self.cuchara_general_contador += 1

        # Obtener la temperatura actual
        temperatura = self.temperature_reader.read_temperature()
        if temperatura is None:
            temperatura = 0.0  # Manejar error de lectura

        # Obtener la potencia actual
        potencia = self.potencia_seteada

        # Guardar en la tabla cucharas_log (contador general)
        data_general = {
            'colada': self.colada_text,
            'cuchara': self.cuchara_general_contador,
            'potencia': potencia,
            'temperatura': temperatura
        }
        db.save_record('cucharas_log', data_general, self.on_cuchara_saved)

    def on_cuchara_saved(self, success, error, inserted_id):
        def update_ui(dt):
            if success:
                pass  # Puedes mostrar un mensaje si lo deseas
            else:
                toast(f"Error al guardar la cuchara: {error}")
        Clock.schedule_once(update_ui)

    def decrementar_material(self, material):
        if self.cantidades[material] > 0:
            self.cantidades[material] -= 1
            input_id = f'input_{material.replace(" ", "_")}'
            if input_id in self.ids:
                self.ids[input_id].text = str(self.cantidades[material])

            # Decrementar el contador general de cucharas si es necesario
            if self.cuchara_general_contador > 0:
                self.cuchara_general_contador -= 1
        else:
            toast("El valor no puede ser negativo.")

    def finalizar_colada(self):
        # Actualizar la hora_fin_de_colada y fecha en planilla_de_fusion
        data_planilla = {
            'hora_fin_de_colada': utils.get_current_time_formatted(),
            'fecha': utils.get_current_date_formatted()
        }
        db.update_record('planilla_de_fusion', self.record_id, data_planilla, self.on_planilla_updated)

    def on_planilla_updated(self, success, error):
        def update_ui(dt):
            if success:
                # Guardar los datos en cucharas_por_material
                data_cucharas = {
                    'fecha': utils.get_current_date_formatted(),
                    'colada': self.colada_text,
                    'base': self.base_text,
                }
                # Añadir las cantidades de materiales
                for material in self.materiales:
                    material_num = ''.join(filter(str.isdigit, material))
                    key = f"material_{material_num}"
                    data_cucharas[key] = self.cantidades[material]
                db.save_record('cucharas_por_material', data_cucharas, self.on_cucharas_saved)
            else:
                toast("Error al actualizar planilla de fusión.")
        Clock.schedule_once(update_ui)

    def on_cucharas_saved(self, success, error, inserted_id):
        def update_ui(dt):
            if success:
                toast("Colada finalizada exitosamente.")
                # Resetear estados y volver a la pantalla inicio
                self.resetear_estados()
                # Resetear la pantalla Reajuste
                reajuste_screen = self.manager.get_screen('reajuste')
                reajuste_screen.resetear_estados()
                inicio_screen = self.manager.get_screen('inicio')
                inicio_screen.resetear_estados()
                self.manager.current = 'inicio'
            else:
                toast("Error al guardar los datos de cucharas.")
        Clock.schedule_once(update_ui)

    def resetear_estados(self):
        # Resetear las cantidades y contadores
        self.cantidades = {}
        self.materiales = []
        self.record_id = 0
        self.base_text = ""
        self.colada_text = ""
        self.ids.materiales_box.clear_widgets()
        # Resetear potencia y temperatura
        self.potencia_seteada = 0
        self.ids.control_de_potencia.value = 0
        self.ids.potencia_label.text = "Potencia: 0%"
        self.ids.temperatura_label.text = "Temperatura: -- °C"
        self.cuchara_general_contador = 0

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