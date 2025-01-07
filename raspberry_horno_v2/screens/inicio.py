# screens/inicio.py

from kivymd.uix.screen import MDScreen
from kivy.properties import StringProperty
from kivy.properties import NumericProperty
from kivymd.uix.menu import MDDropdownMenu
from kivy.metrics import dp
from kivymd.toast import toast
from kivy.clock import Clock
from db import db
from utils import utils

class InicioScreen(MDScreen):
    base_text = StringProperty("Base")
    numero_colada_formateado = StringProperty()
    crisol = StringProperty()
    current_year = StringProperty(utils.get_current_year_short())
    record_id = NumericProperty(0)
    base_text = StringProperty("Base")
    numero_colada_formateado = StringProperty()

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        # Crear el menú de selección de base
        menu_items = [
            {
                "viewclass": "OneLineListItem",
                "text": f"Base {i}",
                "height": dp(56),
                "on_release": lambda x=f"Base {i}": self.set_base(x),
            } for i in [2, 5, 8, 10]
        ]
        self.base_menu = MDDropdownMenu(
            caller=self.ids.base_button,
            items=menu_items,
            width_mult=4,
        )
        # Obtener el último número de colada
        db.get_last_value('planilla_de_fusion', 'colada', self.set_numero_colada_inicial)
        # Obtener el último número de crisol
        db.get_last_value('planilla_de_fusion', 'crisol', self.set_crisol_inicial)

    def set_numero_colada_inicial(self, last_colada, error):
        def update_ui(dt):
            if error:
                toast("Error al obtener el número de colada")
                self.numero_colada_formateado = f"0001/{self.current_year}"
            else:
                current_year = self.current_year
                if last_colada:
                    try:
                        last_number_str, last_year = last_colada.split('/')
                        last_number = int(last_number_str)
                        if last_year == current_year:
                            new_number = last_number + 1
                        else:
                            new_number = 1
                    except ValueError:
                        new_number = 1
                    self.numero_colada_formateado = f"{new_number:04d}/{current_year}"
                else:
                    self.numero_colada_formateado = f"0001/{current_year}"
            self.ids.colada_input.text = self.numero_colada_formateado
        Clock.schedule_once(update_ui)

    def set_crisol_inicial(self, last_crisol, error):
        def update_ui(dt):
            if error:
                toast("Error al obtener el número de crisol")
                self.crisol = "1"
            else:
                if last_crisol:
                    try:
                        numero_crisol = int(last_crisol) + 1
                    except ValueError:
                        numero_crisol = 1
                else:
                    numero_crisol = 1
                self.crisol = str(numero_crisol)
            self.ids.crisol_input.text = self.crisol
        Clock.schedule_once(update_ui)

    def open_base_menu(self):
        self.base_menu.open()

    def set_base(self, text_item):
        self.base_text = text_item
        self.base_menu.dismiss()

    def incrementar_colada(self):
        try:
            # Incrementar número de colada
            numero_str, year_str = self.numero_colada_formateado.split('/')
            numero = int(numero_str) + 1
            self.numero_colada_formateado = f"{numero:04d}/{year_str}"
            self.ids.colada_input.text = self.numero_colada_formateado

            # Incrementar crisol
            crisol_num = int(self.crisol) + 1
            self.crisol = str(crisol_num)
            self.ids.crisol_input.text = self.crisol
        except Exception as e:
            print(f"Error al incrementar colada y crisol: {e}")

    def decrementar_colada(self):
        try:
            # Decrementar número de colada
            numero_str, year_str = self.numero_colada_formateado.split('/')
            numero = int(numero_str)
            if numero > 1:
                numero -= 1
                self.numero_colada_formateado = f"{numero:04d}/{year_str}"
                self.ids.colada_input.text = self.numero_colada_formateado

                # Decrementar crisol
                crisol_num = int(self.crisol)
                if crisol_num > 1:
                    crisol_num -= 1
                else:
                    crisol_num = 1
                self.crisol = str(crisol_num)
                self.ids.crisol_input.text = self.crisol
            else:
                toast("El número de colada no puede ser menor que 0001.")
        except Exception as e:
            print(f"Error al decrementar colada y crisol: {e}")

    def continuar(self):
        if self.base_text == "Base":
            toast("Por favor, seleccione una base antes de continuar.")
        else:
            data = {
                'colada': self.numero_colada_formateado,
                'carga': self.base_text,
                'crisol': self.crisol,
                'hora_inicio_carga': utils.get_current_time_formatted()
            }
            db.save_record('planilla_de_fusion', data, self.on_colada_saved)

    def on_colada_saved(self, success, error, inserted_id):
        def update_ui(dt):
            if success:
                self.record_id = inserted_id
                toast("Colada guardada exitosamente.")
                # Pasar el ID y otros datos a la siguiente pantalla
                reajuste_screen = self.manager.get_screen('reajuste')
                reajuste_screen.set_record_id(self.record_id)
                reajuste_screen.set_base_text(self.base_text)
                reajuste_screen.set_numero_colada(self.numero_colada_formateado)
                self.manager.current = 'reajuste'
            else:
                toast("Error al guardar la colada.")
        Clock.schedule_once(update_ui)

    def resetear_estados(self):
        # Incrementar colada y crisol
        self.incrementar_colada()
        # Restablecer base
        self.base_text = "Base"
        self.ids.base_button.text = self.base_text
        # Restablecer variables
        self.numero_colada_formateado = self.ids.colada_input.text
        self.crisol = self.ids.crisol_input.text

    def reset_crisol(self):
        self.crisol = "0"
        self.ids.crisol_input.text = self.crisol