# main.py

from kivy.config import Config
#Config.set('graphics', 'fullscreen', 'auto')
#Config.set('graphics', 'show_cursor', '0')

from kivy.lang import Builder
from kivymd.app import MDApp  
from kivy.uix.screenmanager import ScreenManager
import os
from kivy.core.window import Window
from time import time

# Importamos las pantallas
from screens.inicio import InicioScreen
from screens.reajuste import ReajusteScreen
from screens.cucharas_por_material import CucharasPorMaterialScreen
from screens.control_de_diametros import ControlDiametrosScreen

rate_limit = 1.0
last_touch_time = 0.0
last_mouse_time = 0.0

def global_touch_down_filter(window, touch):
    global last_touch_time, rate_limit
    current_time = time()
    diff = current_time - last_touch_time
    # Mostrar debug
    if diff < rate_limit:
        return True
    last_touch_time = current_time
    return False

def global_mouse_down_filter(window, x, y, button, modifiers):
    global last_mouse_time, rate_limit
    current_time = time()
    diff = current_time - last_mouse_time
    # Mostrar debug
    if diff < rate_limit:
        return True
    last_mouse_time = current_time
    return False

class MyScreenManager(ScreenManager):
    pass

class MyApp(MDApp):
    def build(self):
        Window.bind(on_touch_down=global_touch_down_filter)
        Window.bind(on_mouse_down=global_mouse_down_filter)
        self.theme_cls.theme_style = "Light"
        self.theme_cls.primary_palette = "Blue"
        self.theme_cls.accent_palette = "Red"

        # Cargar los archivos KV de las pantallas
        Builder.load_file(os.path.join('screens', 'inicio.kv'))
        Builder.load_file(os.path.join('screens', 'reajuste.kv'))
        Builder.load_file(os.path.join('screens', 'cucharas_por_material.kv'))
        Builder.load_file(os.path.join('screens', 'control_de_diametros.kv'))

        sm = MyScreenManager()
        sm.add_widget(InicioScreen(name='inicio'))
        sm.add_widget(ReajusteScreen(name='reajuste'))
        sm.add_widget(CucharasPorMaterialScreen(name='cucharas_por_material'))
        sm.add_widget(ControlDiametrosScreen(name='control_de_diametros'))
        return sm

if __name__ == '__main__':
    MyApp().run()
