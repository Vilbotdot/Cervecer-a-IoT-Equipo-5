/*
  =============================================================
    MONITOR IoT PARA ZONA DE FERMENTACION CERVECERA (SPRINT 3)
    Integración de Wi-Fi 802.11 + TCP + MQTT Pub/Sub
  =============================================================

  Hardware:
    - ESP32 DevKit
    - DHT11: temperatura y humedad ambiental (GPIO4)
    - MQ-135: respuesta relativa ante cambios en el aire (GPIO34)
    - PIR HC-SR501: deteccion de movimiento (GPIO27)

  Nuevas Funcionalidades Sprint 3:
    - Conexión a Red Wi-Fi (802.11 b/g/n)
    - Conexión al Broker MQTT (orientado a TCP en puerto 1883)
    - Transmisión continua de payload en formato JSON conteniendo:
      { temp, humedad, mq_adc, mq_var_pct, pir_activo, pir_eventos, estado }
  =============================================================
*/

#include <DHT.h>
#include <math.h>
#include <WiFi.h>
#include <PubSubClient.h>

// =============================================================
// 0. CONFIGURACIÓN DE RED Y BROKER MQTT (SPRINT 3)
// =============================================================
//const char* WIFI_SSID     = "iCUCEI";       // Cambia por el nombre de tu red
//const char* WIFI_PASSWORD = "";  // Cambia por la contraseña de tu red

const char* WIFI_SSID     = "iPhone de Diego";       // Cambia por el nombre de tu red
const char* WIFI_PASSWORD = "Obscenas123";  // Cambia por la contraseña de tu red

// Broker MQTT público (puedes cambiarlo por 'test.mosquitto.org' o una IP local)
const char* MQTT_SERVER   = "broker.hivemq.com";
const int   MQTT_PORT     = 1883;
const char* MQTT_TOPIC    = "agencia/cerveceria/sensores";

WiFiClient espClient;
PubSubClient mqttClient(espClient);

// =============================================================
// 1. CONFIGURACION DE PINES
// =============================================================
#define DHT_PIN       4
#define DHT_TYPE      DHT11
#define MQ135_PIN     34
#define PIR_PIN       27

DHT dht(DHT_PIN, DHT_TYPE);

// =============================================================
// 2. CONFIGURACION GENERAL
// =============================================================
const unsigned long MQ_SAMPLE_INTERVAL_MS = 250;
const unsigned long DHT_INTERVAL_MS       = 2000;
const unsigned long REPORT_INTERVAL_MS    = 2000;

const unsigned long STARTUP_SETTLING_MS = 15000;
const unsigned long CALIBRATION_MS      = 15000;
const unsigned long PIR_STARTUP_IGNORE_MS = 30000;
const unsigned long PIR_DEBOUNCE_MS = 250;
const unsigned long PIR_RECENT_WINDOW_MS = 30000;

const int MQ_FILTER_SIZE = 10;
const float MQ_CHANGE_THRESHOLD_PERCENT = 18.0;
const int MQ_MIN_RAW_CHANGE = 80;
const int MQ_ALERT_CONFIRMATIONS = 3;

const float TEMP_MIN_C = 18.0;
const float TEMP_MAX_C = 28.0;
const float HUMIDITY_MIN_PERCENT = 25.0;
const float HUMIDITY_MAX_PERCENT = 85.0;

// =============================================================
// 3. ESTADOS DEL SISTEMA
// =============================================================
enum MonitoringPhase { PHASE_WARMUP, PHASE_CALIBRATING, PHASE_RUNNING };
enum GasStatus { GAS_NORMAL, GAS_CHANGE, GAS_ALERT };

MonitoringPhase phase = PHASE_WARMUP;
GasStatus gasStatus = GAS_NORMAL;

// =============================================================
// 4. VARIABLES DE TIEMPO
// =============================================================
unsigned long bootAt = 0;
unsigned long phaseStartedAt = 0;
unsigned long lastMQSampleAt = 0;
unsigned long lastDHTReadAt = 0;
unsigned long lastReportAt = 0;

// =============================================================
// 5. VARIABLES DEL MQ-135
// =============================================================
int mqBuffer[MQ_FILTER_SIZE] = {0};
int mqBufferIndex = 0;
int mqBufferCount = 0;
long mqBufferSum = 0;
int mqRaw = 0;
float mqFiltered = 0.0;
float mqBaseline = 0.0;
bool mqBaselineValid = false;
double calibrationSum = 0.0;
unsigned long calibrationCount = 0;
int gasAlertConsecutiveReports = 0;

// =============================================================
// 6. VARIABLES DEL DHT11
// =============================================================
float temperatureC = NAN;
float humidityPercent = NAN;
bool dhtReadingValid = false;
bool temperatureAlert = false;
bool humidityAlert = false;

// =============================================================
// 7. VARIABLES DEL PIR HC-SR501
// =============================================================
bool pirMotionActive = false;
bool pirLastState = false;
bool pirReady = false;
unsigned long pirEventCount = 0;
unsigned long pirLastMotionAt = 0;
bool pirHasMotionEvent = false;
unsigned long pirLastEdgeAt = 0;

// =============================================================
// 8. MODO DE SALIDA Y GESTIÓN DE RED
// =============================================================
bool csvMode = false;
bool csvHeaderPrinted = false;

void setup_wifi() {
  delay(10);
  Serial.println();
  Serial.print("[WIFI] Conectando a ");
  Serial.println(WIFI_SSID);

  WiFi.mode(WIFI_STA);
  WiFi.begin(WIFI_SSID, WIFI_PASSWORD);

  int retries = 0;
  while (WiFi.status() != WL_CONNECTED && retries < 20) {
    delay(500);
    Serial.print(".");
    retries++;
  }

  if (WiFi.status() == WL_CONNECTED) {
    Serial.println("\n[WIFI] ¡Conectado con éxito!");
    Serial.print("[WIFI] Dirección IP asignada: ");
    Serial.println(WiFi.localIP());
  } else {
    Serial.println("\n[WIFI] No se pudo conectar a la red. Se reintentará en el loop.");
  }
}

void reconnect_mqtt() {
  if (WiFi.status() != WL_CONNECTED) {
    return;
  }

  while (!mqttClient.connected()) {
    Serial.print("[MQTT] Intentando conexión al Broker...");
    String clientId = "ESP32-Cerveceria-";
    clientId += String(random(0xffff), HEX);

    if (mqttClient.connect(clientId.c_str())) {
      Serial.println(" ¡Conectado!");
    } else {
      Serial.print(" Falló, rc=");
      Serial.print(mqttClient.state());
      Serial.println(" Reintentando en 3 segundos...");
      delay(3000);
    }
  }
}

// =============================================================
// 9. FUNCIONES DE ESTADO
// =============================================================
const char* getPhaseName() {
  switch (phase) {
    case PHASE_WARMUP: return "ESTABILIZACION_INICIAL";
    case PHASE_CALIBRATING: return "CALIBRANDO_REFERENCIA";
    case PHASE_RUNNING: return "MONITOREANDO";
  }
  return "DESCONOCIDO";
}

const char* getGasStatusName() {
  switch (gasStatus) {
    case GAS_NORMAL: return "NORMAL";
    case GAS_CHANGE: return "CAMBIO_EN_OBSERVACION";
    case GAS_ALERT: return "ALERTA_PERSISTENTE";
  }
  return "DESCONOCIDO";
}

bool pirMotionRecentlyDetected(unsigned long now) {
  if (!pirHasMotionEvent) return false;
  return (now - pirLastMotionAt) <= PIR_RECENT_WINDOW_MS;
}

const char* getPIRStatusName(unsigned long now) {
  if (!pirReady) return "ESTABILIZANDO";
  if (pirMotionActive) return "MOVIMIENTO_ACTIVO";
  if (pirMotionRecentlyDetected(now)) return "MOVIMIENTO_RECIENTE";
  return "SIN_MOVIMIENTO";
}

const char* getTemperatureStatusName() {
  if (!dhtReadingValid) return "SIN_LECTURA_VALIDA";
  if (temperatureC < TEMP_MIN_C) return "TEMPERATURA_BAJA";
  if (temperatureC > TEMP_MAX_C) return "TEMPERATURA_ALTA";
  return "DENTRO_DEL_RANGO";
}

const char* getHumidityStatusName() {
  if (!dhtReadingValid) return "SIN_LECTURA_VALIDA";
  if (humidityPercent < HUMIDITY_MIN_PERCENT) return "HUMEDAD_BAJA";
  if (humidityPercent > HUMIDITY_MAX_PERCENT) return "HUMEDAD_ALTA";
  return "DENTRO_DEL_RANGO";
}

const char* getOverallStatus() {
  unsigned long now = millis();
  if (phase != PHASE_RUNNING) return getPhaseName();
  if (!mqBaselineValid) return "REVISAR_REFERENCIA_MQ135";
  if (!dhtReadingValid) return "REVISAR_DHT11";
  if (gasStatus == GAS_ALERT || temperatureAlert || humidityAlert) return "ALERTA_AMBIENTAL";
  if (gasStatus == GAS_CHANGE) return "CAMBIO_EN_OBSERVACION";
  if (pirMotionActive || pirMotionRecentlyDetected(now)) return "ACTIVIDAD_DETECTADA";
  return "NORMAL";
}

// =============================================================
// 10. FILTRO MQ-135, CALIBRACIÓN Y LECTURAS
// =============================================================
void addMQSample(int newReading) {
  if (mqBufferCount < MQ_FILTER_SIZE) {
    mqBuffer[mqBufferIndex] = newReading;
    mqBufferSum += newReading;
    mqBufferCount++;
  } else {
    mqBufferSum -= mqBuffer[mqBufferIndex];
    mqBuffer[mqBufferIndex] = newReading;
    mqBufferSum += newReading;
  }
  mqBufferIndex++;
  if (mqBufferIndex >= MQ_FILTER_SIZE) mqBufferIndex = 0;
  if (mqBufferCount > 0) mqFiltered = (float)mqBufferSum / (float)mqBufferCount;
}

void startCalibration() {
  phase = PHASE_CALIBRATING;
  phaseStartedAt = millis();
  calibrationSum = 0.0;
  calibrationCount = 0;
  mqBaselineValid = false;
  gasStatus = GAS_NORMAL;
  gasAlertConsecutiveReports = 0;
}

void updateMQCalibration(unsigned long now) {
  if (phase == PHASE_WARMUP) {
    if (now - phaseStartedAt >= STARTUP_SETTLING_MS) startCalibration();
    return;
  }

  if (phase == PHASE_CALIBRATING) {
    calibrationSum += mqFiltered;
    calibrationCount++;
    if (now - phaseStartedAt >= CALIBRATION_MS) {
      if (calibrationCount > 0) {
        mqBaseline = (float)(calibrationSum / calibrationCount);
        if (mqBaseline > 10.0 && mqBaseline < 4090.0) {
          mqBaselineValid = true;
          phase = PHASE_RUNNING;
          phaseStartedAt = now;
        } else {
          mqBaselineValid = false;
          phase = PHASE_RUNNING;
        }
      } else {
        mqBaselineValid = false;
        phase = PHASE_RUNNING;
      }
    }
  }
}

void sampleMQ135(unsigned long now) {
  if (now - lastMQSampleAt < MQ_SAMPLE_INTERVAL_MS) return;
  lastMQSampleAt = now;
  mqRaw = analogRead(MQ135_PIN);
  addMQSample(mqRaw);
  updateMQCalibration(now);
}

float getMQVariationPercent() {
  if (!mqBaselineValid || mqBaseline <= 0.0) return NAN;
  return ((mqFiltered - mqBaseline) / mqBaseline) * 100.0;
}

void evaluateGasStatus() {
  if (phase != PHASE_RUNNING || !mqBaselineValid) {
    gasStatus = GAS_NORMAL;
    gasAlertConsecutiveReports = 0;
    return;
  }
  float variation = getMQVariationPercent();
  if (isnan(variation)) {
    gasStatus = GAS_NORMAL;
    gasAlertConsecutiveReports = 0;
    return;
  }
  float absoluteVariation = fabs(variation);
  float absoluteRawChange = fabs(mqFiltered - mqBaseline);
  bool significantChange = absoluteVariation >= MQ_CHANGE_THRESHOLD_PERCENT && absoluteRawChange >= MQ_MIN_RAW_CHANGE;

  if (significantChange) {
    gasAlertConsecutiveReports++;
    if (gasAlertConsecutiveReports >= MQ_ALERT_CONFIRMATIONS) gasStatus = GAS_ALERT;
    else gasStatus = GAS_CHANGE;
  } else {
    gasAlertConsecutiveReports = 0;
    gasStatus = GAS_NORMAL;
  }
}

void readDHT11(unsigned long now) {
  if (now - lastDHTReadAt < DHT_INTERVAL_MS) return;
  lastDHTReadAt = now;
  float newTemperature = dht.readTemperature();
  float newHumidity = dht.readHumidity();

  if (isnan(newTemperature) || isnan(newHumidity)) {
    dhtReadingValid = false;
    temperatureAlert = false;
    humidityAlert = false;
    return;
  }
  temperatureC = newTemperature;
  humidityPercent = newHumidity;
  dhtReadingValid = true;
  temperatureAlert = temperatureC < TEMP_MIN_C || temperatureC > TEMP_MAX_C;
  humidityAlert = humidityPercent < HUMIDITY_MIN_PERCENT || humidityPercent > HUMIDITY_MAX_PERCENT;
}

void readPIR(unsigned long now) {
  pirMotionActive = (digitalRead(PIR_PIN) == HIGH);
  if (now - bootAt < PIR_STARTUP_IGNORE_MS) {
    pirLastState = pirMotionActive;
    return;
  }
  if (!pirReady) {
    pirReady = true;
    pirLastState = pirMotionActive;
    pirLastEdgeAt = now;
    return;
  }
  if (pirMotionActive != pirLastState) {
    if (now - pirLastEdgeAt >= PIR_DEBOUNCE_MS) {
      pirLastEdgeAt = now;
      pirLastState = pirMotionActive;
      if (pirMotionActive) {
        pirEventCount++;
        pirLastMotionAt = now;
        pirHasMotionEvent = true;
      }
    }
  }
}

// =============================================================
// 11. PUBLICACIÓN MQTT DE DATOS (SPRINT 3)
// =============================================================
void publishMQTTData() {
  if (!mqttClient.connected()) return;

  float mqVar = getMQVariationPercent();

  // Creación del Payload JSON
  String payload = "{";
  payload += "\"temp\":" + (dhtReadingValid ? String(temperatureC, 1) : "null") + ",";
  payload += "\"humedad\":" + (dhtReadingValid ? String(humidityPercent, 1) : "null") + ",";
  payload += "\"mq_adc\":" + String(mqFiltered, 1) + ",";
  payload += "\"mq_var_pct\":" + (isnan(mqVar) ? "0.0" : String(mqVar, 1)) + ",";
  payload += "\"pir_activo\":" + String(pirMotionActive ? "true" : "false") + ",";
  payload += "\"pir_eventos\":" + String(pirEventCount) + ",";
  payload += "\"estado\":\"" + String(getOverallStatus()) + "\"";
  payload += "}";

  Serial.print("[MQTT] Publicando en ");
  Serial.print(MQTT_TOPIC);
  Serial.print(" -> ");
  Serial.println(payload);

  mqttClient.publish(MQTT_TOPIC, payload.c_str());
}

// =============================================================
// 12. SETUP & LOOP PRINCIPAL
// =============================================================
void setup() {
  Serial.begin(115200);
  delay(1000);
  bootAt = millis();

  dht.begin();
  analogReadResolution(12);
  pinMode(MQ135_PIN, INPUT);
  pinMode(PIR_PIN, INPUT);

  pirLastState = (digitalRead(PIR_PIN) == HIGH);
  phase = PHASE_WARMUP;
  phaseStartedAt = millis();

  // Inicialización de Red para Sprint 3
  setup_wifi();
  mqttClient.setServer(MQTT_SERVER, MQTT_PORT);
}

void loop() {
  unsigned long now = millis();

  // Gestión de conexión MQTT
  if (WiFi.status() == WL_CONNECTED) {
    if (!mqttClient.connected()) {
      reconnect_mqtt();
    }
    mqttClient.loop();
  }

  // Adquisición de datos
  sampleMQ135(now);
  readDHT11(now);
  readPIR(now);

  // Intervalo de reporte y envío (2 segundos)
  if (now - lastReportAt >= REPORT_INTERVAL_MS) {
    lastReportAt = now;
    evaluateGasStatus();

    // Transmisión del dato vía MQTT al broker
    publishMQTTData();
  }
}
