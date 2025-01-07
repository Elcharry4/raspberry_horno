from kivymd.uix.button import MDRaisedButton
from behaviors.rate_limit_behavior import RateLimitBehavior

class RateLimitedButton(RateLimitBehavior, MDRaisedButton):
    def on_release(self):
        # Primero verificamos si ha pasado el tiempo suficiente
        if self.on_press_limited():
            # Si ha pasado, llamamos al on_release real del MDRaisedButton
            return super().on_release()
        # Si no ha pasado el tiempo, simplemente no hacemos nada (ignoramos la pulsación)
