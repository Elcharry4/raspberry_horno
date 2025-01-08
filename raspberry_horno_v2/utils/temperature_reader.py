# utils/temperature_reader.py
import spidev
import time

class TemperatureReader:
    def __init__(self, cs_pin=None, clock_pin=None, data_pin=None):
        # Como ya sabemos que es bus=0, device=0
        self.bus = 0
        self.device = 0
        self.spi = spidev.SpiDev()
        self.spi.open(self.bus, self.device)
        self.spi.max_speed_hz = 500000
        # Los parámetros cs_pin, clock_pin, data_pin ya no se usan, 
        # el wiring se hace por hardware SPI.

    def read_temperature(self):
        # Leer 2 bytes del MAX6675
        raw_data = self.spi.xfer2([0x00, 0x00])
        data = (raw_data[0] << 8) | raw_data[1]

        if data & 0x4:
            # Si el bit D2 está en 1, no hay termocupla conectada o error
            return None

        # Los bits de temperatura se obtienen desplazando a la derecha 3
        temperature_c = (data >> 3) * 0.25
        return temperature_c

#utils/temperature_reader.py

# import random

# class TemperatureReader:
#     def __init__(self, cs_pin=None, clock_pin=None, data_pin=None):
#         self.cs_pin = cs_pin
#         self.clock_pin = clock_pin
#         self.data_pin = data_pin
#         # Aquí puedes inicializar el sensor real cuando lo tengas

#     def read_temperature(self):
#         # Simular una temperatura para pruebas
#         return random.uniform(1000.0, 1600.0)
