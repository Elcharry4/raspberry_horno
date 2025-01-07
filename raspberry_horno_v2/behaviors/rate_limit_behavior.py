from time import time

class RateLimitBehavior:
    rate_limit = 5.0  # Un evento por segundo
    _last_press_time = 0

    def on_press_limited(self):
        """Verifica si ha pasado al menos 'rate_limit' segundos desde la última pulsación.
        Retorna True si se puede ejecutar la acción, False si no."""
        current_time = time()
        if current_time - self._last_press_time > self.rate_limit:
            self._last_press_time = current_time
            return True
        return False
