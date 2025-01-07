# screens/control_de_diametro.py

from kivymd.uix.screen import MDScreen
from kivymd.uix.menu import MDDropdownMenu
from kivy.properties import BooleanProperty, StringProperty
from kivymd.toast import toast
from kivy.clock import Clock
from db import db

class ControlDiametrosScreen(MDScreen):
    # Propiedades para los checkboxes
    mangueras = BooleanProperty(False)
    hidraulico = BooleanProperty(False)
    limpieza = BooleanProperty(False)
    nivel_agua_opciones = ["1/4", "1/2", "3/4", "lleno"]
    nivel_agua_seleccion = StringProperty("")
    menu = None

    # Asumimos que colada_text y base_text se establecen al pasar a esta pantalla
    colada_text = StringProperty("")
    base_text = StringProperty("")  # Esto será 'crisol'

    def on_enter(self):
        # Crear el menú desplegable para nivel de agua
        menu_items = [{
            "text": opcion,
            "viewclass": "OneLineListItem",
            "on_release": lambda x=opcion: self.set_nivel_agua(x)
        } for opcion in self.nivel_agua_opciones]

        self.menu = MDDropdownMenu(
            caller=self.ids.nivel_agua_dropdown,
            items=menu_items,
            width_mult=2
        )

    def toggle_mangueras(self, active):
        self.mangueras = active

    def toggle_hidraulico(self, active):
        self.hidraulico = active

    def toggle_limpieza(self, active):
        self.limpieza = active

    def open_nivel_agua_menu(self):
        if self.menu:
            self.menu.open()

    def set_nivel_agua(self, opcion):
        self.nivel_agua_seleccion = opcion
        self.ids.nivel_agua_dropdown.text = f"Nivel de agua: {opcion}"
        self.menu.dismiss()

    def guardar_datos(self):
        # Obtener los valores de las entradas de texto
        arriba_val = self.ids.arriba_input.text if 'arriba_input' in self.ids else ""
        medio_val = self.ids.medio_input.text if 'medio_input' in self.ids else ""
        abajo_val = self.ids.abajo_input.text if 'abajo_input' in self.ids else ""
        altura_val = self.ids.altura_input.text if 'altura_input' in self.ids else ""

        # Convertir booleanos a "Si" o "No"
        mangueras_val = "Si" if self.mangueras else "No"
        hidraulico_val = "Si" if self.hidraulico else "No"
        limpieza_val = "Si" if self.limpieza else "No"

        nivel_agua_val = self.nivel_agua_seleccion if self.nivel_agua_seleccion else ""

        # Crear el diccionario con los datos a guardar
        data = {
            'colada': self.colada_text,
            'crisol': self.base_text,
            'diametro_arriba': arriba_val,
            'diametro_medio': medio_val,
            'diametro_abajo': abajo_val,
            'altura': altura_val,
            'nivel_del_agua': nivel_agua_val,
            'mangueras': mangueras_val,
            'hidraulico': hidraulico_val,
            'limpieza_sector': limpieza_val
        }

        # Guardar en la base de datos
        db.save_record('control_diametro', data, self.on_data_saved)

    def on_data_saved(self, success, error, inserted_id):
        def update_ui(dt):
            if success:
                toast("Datos guardados exitosamente.")
                # Opcional: Resetear campos después de guardar
                self.resetear_campos()
                self.manager.current = 'inicio'
            else:
                toast(f"Error al guardar datos: {error}")
        Clock.schedule_once(update_ui)

    def resetear_campos(self):
        # Limpiar los campos de texto
        if 'arriba_input' in self.ids:
            self.ids.arriba_input.text = ""
        if 'medio_input' in self.ids:
            self.ids.medio_input.text = ""
        if 'abajo_input' in self.ids:
            self.ids.abajo_input.text = ""
        if 'altura_input' in self.ids:
            self.ids.altura_input.text = ""

        # Desmarcar checks
        self.mangueras = False
        self.hidraulico = False
        self.limpieza = False

        # Resetear el nivel de agua
        self.nivel_agua_seleccion = ""
        self.ids.nivel_agua_dropdown.text = "Nivel de agua"

