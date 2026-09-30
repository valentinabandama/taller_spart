import socket
import random
import time

HOST = "localhost"
PORT = 9999
class Sensor:
    def __init__(self):
        self.temperatura = 0.0
        self.ruido = 0.0
        self.vibracion = 0.0

    def generar_datos(self):
        self.temperatura = round(random.uniform(20, 100), 2)
        self.ruido = round(random.uniform(25, 125), 2)
        self.vibracion = round(random.uniform(0.05, 6), 2)

    def obtener_datos(self):
        return f"{self.temperatura},{self.ruido},{self.vibracion}\n"

def main():
    servidor = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    servidor.bind((HOST, PORT))
    servidor.listen(1)

    print("Esperando conexión...")
    conexion, direccion = servidor.accept()

    print("Consumidor conectado.")

    while True:
        sensor = Sensor()
        sensor.generar_datos()
        datos = sensor.obtener_datos()

        conexion.sendall(datos.encode())

        print("Datos enviados:", datos.strip())

        time.sleep(2)
if __name__ == "__main__":
    main()