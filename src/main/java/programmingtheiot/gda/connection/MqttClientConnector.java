/**
 * This class is part of the Programming the Internet of Things project.
 * 
 * It is provided as a simple shell to guide the student and assist with
 * implementation for the Programming the Internet of Things exercises,
 * and designed to be modified by the student as needed.
 */

package programmingtheiot.gda.connection;

import java.util.Properties;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.eclipse.paho.client.mqttv3.IMqttDeliveryToken;
import org.eclipse.paho.client.mqttv3.MqttCallbackExtended;
import org.eclipse.paho.client.mqttv3.MqttClient;
import org.eclipse.paho.client.mqttv3.MqttConnectOptions;
import org.eclipse.paho.client.mqttv3.MqttException;
import org.eclipse.paho.client.mqttv3.MqttMessage;
import org.eclipse.paho.client.mqttv3.MqttPersistenceException;
import org.eclipse.paho.client.mqttv3.MqttSecurityException;
import org.eclipse.paho.client.mqttv3.persist.MemoryPersistence;

import programmingtheiot.common.ConfigConst;
import programmingtheiot.common.ConfigUtil;
import programmingtheiot.common.IDataMessageListener;
import programmingtheiot.common.ResourceNameEnum;

import java.io.File;
import javax.net.ssl.SSLSocketFactory;
import org.eclipse.paho.client.mqttv3.IMqttMessageListener;
import org.eclipse.paho.client.mqttv3.MqttAsyncClient;

import programmingtheiot.common.SimpleCertManagementUtil;
import programmingtheiot.data.ActuatorData;
import programmingtheiot.data.DataUtil;
import programmingtheiot.data.SensorData;
import programmingtheiot.data.SystemPerformanceData;

/**
 * Shell representation of class for student implementation.
 * 
 */
public class MqttClientConnector implements IPubSubClient, MqttCallbackExtended {
	// static

	private static final Logger _Logger = Logger.getLogger(MqttClientConnector.class.getName());

	private boolean useAsyncClient = true;

	// private MqttClient mqttClient = null;
	private MqttConnectOptions connOpts = null;
	private MemoryPersistence persistence = null;
	private IDataMessageListener dataMsgListener = null;

	private String clientID = null;
	private String brokerAddr = null;
	private String host = ConfigConst.DEFAULT_HOST;
	private String protocol = ConfigConst.DEFAULT_MQTT_PROTOCOL;
	private int port = ConfigConst.DEFAULT_MQTT_PORT;
	private int brokerKeepAlive = ConfigConst.DEFAULT_KEEP_ALIVE;

	private String pemFileName = null;
	private boolean enableEncryption = false;
	private boolean useCleanSession = false;
	private boolean enableAutoReconnect = true;

	private MqttClient mqttClient = null;
	private MqttAsyncClient mqttAsyncClient = null;

	// params

	// constructors

	/**
	 * Default.
	 * 
	 */
	public MqttClientConnector() {
		super();
		initClientParameters(ConfigConst.MQTT_GATEWAY_SERVICE);

	}

	// public methods

	@Override
	// Connectivity using or not asynchronous client
	public boolean connectClient() {
		try {
			if (this.useAsyncClient) {

				// Asynchronous client
				if (this.mqttAsyncClient == null) {
					this.mqttAsyncClient = new MqttAsyncClient(this.brokerAddr, this.clientID, this.persistence);
					this.mqttAsyncClient.setCallback(this);
				}
				// Asynchronous client connection
				if (!this.mqttAsyncClient.isConnected()) {
					_Logger.info("MQTT ASYNC client CONNECTING to broker: " + this.brokerAddr);
					this.mqttAsyncClient.connect(this.connOpts);

					return true;
				} else {
					_Logger.warning("MQTT ASYNC client already connected to broker: " + brokerAddr);
				}
			} else {
				// Synchronous client
				if (this.mqttClient == null) {
					this.mqttClient = new MqttClient(this.brokerAddr, this.clientID, this.persistence);
					this.mqttClient.setCallback(this);
				}
				// Synchronous client connection
				if (!mqttClient.isConnected()) {
					_Logger.info("MQTT SYNC client CONNECTING to broker: " + this.brokerAddr);
					this.mqttClient.connect(this.connOpts);
					return true;
				} else {
					_Logger.warning("MQTT SYNC client already connected to broker: " + this.brokerAddr);
				}
			}
		} catch (MqttException e) {
			_Logger.log(Level.SEVERE, "FAILED to connect MQTT client to broker.", e);
		}
		return false;
	}

	@Override
	// Disconnection using or not asynchronous client
	public boolean disconnectClient() {
		try {
			if (this.useAsyncClient) {

				// Asynchronous client
				if (this.mqttAsyncClient != null) {
					if (this.mqttAsyncClient.isConnected()) { // with asynchronous client connected proceed to
																// disconnect
						_Logger.info("DISCONNECTING MQTT ASYNC client from broker: " + this.brokerAddr);
						this.mqttAsyncClient.disconnect();
						return true;
					} else {
						_Logger.warning("MQTT ASYNC client NOT CONNECTED to broker: " + this.brokerAddr);
					}
				}
				return false;

			} else {

				// Synchronous client
				if (this.mqttClient != null) {
					if (this.mqttClient.isConnected()) { // with synchronous client connected proceed to disconnect
						_Logger.info("DISCONNECTING MQTT SYNC client from broker: " + this.brokerAddr);
						this.mqttClient.disconnect();
						return true;
					} else {
						_Logger.warning("MQTT SYNC client NOT CONNECTED to broker: " + this.brokerAddr);
					}
				}
				return false;
			}
		} catch (Exception e) {
			_Logger.log(Level.SEVERE, "FAILED to disconnect MQTT SYNC client from broker: " + this.brokerAddr, e);
			return false;
		}
	}

	//Verifies whether the MQTT  client is currently connected
	public boolean isConnected() {
		if (this.useAsyncClient) {

			// Aynchronous client will connect (with value True) only complying with these requirements
			return this.mqttAsyncClient != null && this.mqttAsyncClient instanceof MqttAsyncClient
					&& this.mqttAsyncClient.isConnected();

		} else {

			// Synchronous client will connect (with value True) only complying with these requirements
			return this.mqttClient != null && this.mqttClient instanceof MqttClient && this.mqttClient.isConnected();
		}
	}

	@Override
	public boolean publishMessage(ResourceNameEnum topicName, String msg, int qos) {

		// Validate that the topic is not null
		if (topicName == null) {
			_Logger.warning("ERROR!: Resource is null. Unable to publish message on broker: " + this.brokerAddr);
			return false;
		}

		// Check that the message is not null or empty
		if (msg == null || msg.trim().isEmpty()) {
			_Logger.warning("ERROR!: Message is null or empty. Publishing aborted on broker: " + this.brokerAddr);
			return false;
		}

		// Validate QoS value
		if (qos < 0 || qos > 2) {
			_Logger.warning("WARNING!: Invalid QoS value (" + qos + ") detected. Using default QoS: " + ConfigConst.DEFAULT_QOS);
			qos = ConfigConst.DEFAULT_QOS;
		}

		try {
			//In many MQTT modules, it is necessary to convert the message into a byte array for proper transmission.
			byte[] payload = msg.getBytes();
			MqttMessage mqttMsg = new MqttMessage(payload);
			mqttMsg.setQos(qos);

			// Publish the message on the given topic depending using or not asynchronous client
			if (this.useAsyncClient) {

				this.mqttAsyncClient.publish(topicName.getResourceName(), mqttMsg);

			} else {

				this.mqttClient.publish(topicName.getResourceName(), mqttMsg);

			}
			return true;

		} catch (Exception e) {
			_Logger.log(Level.SEVERE, "Exception while publishing message on topic: " + topicName + ". Details: ", e);
		}

		return false;
	}

	@Override
	public boolean subscribeToTopic(ResourceNameEnum topicName, int qos) {
		// Validate that the topic is not null
		if (topicName == null) {
			_Logger.warning("Error!: Resource is null. Unable to subscribe to topic");
			return false;
		}

		// Validate QoS value
		if (qos < 0 || qos > 2) {
			_Logger.warning("Warning!: Invalid QoS value (" + qos + ") detected. Using default QoS: " + ConfigConst.DEFAULT_QOS);
			qos = ConfigConst.DEFAULT_QOS;
		}

		try {

			if (this.useAsyncClient) {

				this.mqttAsyncClient.subscribe(topicName.getResourceName(), qos);
				_Logger.info("Congrats! ASYNC Client successfully subscribed to topic: " + topicName.getResourceName());

			} else {

				this.mqttClient.subscribe(topicName.getResourceName(), qos);
				_Logger.info("Congrats! SYNC Client successfully subscribed to topic: " + topicName.getResourceName());

			}
			return true;

		} catch (Exception e) {
			_Logger.log(Level.SEVERE, "ERROR! FAILED to subscribe to topic: " + topicName, e);
		}
		return false;
	}

	@Override
	public boolean unsubscribeFromTopic(ResourceNameEnum topicName) {
		if (topicName == null) {
			_Logger.warning("Resource is null. Unable to unsubscribe from topic: " + this.brokerAddr);
			return false;
		}

		try {
			if (this.useAsyncClient) {

				this.mqttAsyncClient.unsubscribe(topicName.getResourceName());
				_Logger.info("CONGRATS! ASYNC Client successfully unsubscribed from topic: " + topicName.getResourceName());

			} else {

				this.mqttClient.unsubscribe(topicName.getResourceName());
				_Logger.info("CONGRATS! SYNC Client successfully unsubscribed from topic: " + topicName.getResourceName());

			}
			return true;

		} catch (Exception e) {
			_Logger.log(Level.SEVERE, "ERROR! FAILED to unsubscribe from topic: " + topicName, e);
		}
		return false;
	}

	@Override
	public boolean setConnectionListener(IConnectionListener listener) {
		return false;
	}

	@Override
	public boolean setDataMessageListener(IDataMessageListener listener) {
		if (listener != null) {
			this.dataMsgListener = listener;
			return true;
		}

		return false;
	}

	// callbacks

	@Override
	public void connectComplete(boolean reconnect, String serverURI) {
		_Logger.info("Conexión exitosa con MQTT. Broker: " + serverURI);
		int qos = 1;

		// Suscripción a los tres tópicos
		this.subscribeToTopic(ResourceNameEnum.CDA_ACTUATOR_RESPONSE_RESOURCE, qos);
		this.subscribeToTopic(ResourceNameEnum.CDA_SENSOR_MSG_RESOURCE, qos);
		this.subscribeToTopic(ResourceNameEnum.CDA_SYSTEM_PERF_MSG_RESOURCE, qos);

	}

	@Override
	public void connectionLost(Throwable t) {
		_Logger.log(Level.WARNING, "Lost connection to MQTT broker: " + this.brokerAddr, t);

	}

	@Override
	public void deliveryComplete(IMqttDeliveryToken token) {
		_Logger.fine("Delivered MQTT message with ID: " + token.getMessageId());
	}

	@Override
	public void messageArrived(String topic, MqttMessage msg) throws Exception {
		// Asegurarse de que el mensaje es válido
		String payload = new String(msg.getPayload());
		_Logger.info("Mensaje recibido en el tema: " + topic);

		// Verificar el tema y redirigir al método correspondiente
		if (topic.equals(ResourceNameEnum.CDA_ACTUATOR_RESPONSE_RESOURCE.getResourceName())) {
			messageArrivedActuatorData(topic, msg);
		} else if (topic.equals(ResourceNameEnum.CDA_SENSOR_MSG_RESOURCE.getResourceName())) {
			messageArrivedSensorData(topic, msg);
		} else if (topic.equals(ResourceNameEnum.CDA_SYSTEM_PERF_MSG_RESOURCE.getResourceName())) {
			messageArrivedSystemPerformanceData(topic, msg);
		} else {
			_Logger.warning("Tema desconocido recibido: " + topic);
		}

	}

	// private methods

	/**
	 * Called by the constructor to set the MQTT client parameters to be used for
	 * the connection.
	 * 
	 * @param configSectionName The name of the configuration section to use for
	 *                          the MQTT client configuration parameters.
	 */
	private void initClientParameters(String configSectionName) {
		ConfigUtil configUtil = ConfigUtil.getInstance();

		this.host = configUtil.getProperty(configSectionName, ConfigConst.HOST_KEY, ConfigConst.DEFAULT_HOST);
		this.port = configUtil.getInteger(configSectionName, ConfigConst.PORT_KEY, ConfigConst.DEFAULT_MQTT_PORT);
		this.brokerKeepAlive = configUtil.getInteger(configSectionName, ConfigConst.KEEP_ALIVE_KEY,
				ConfigConst.DEFAULT_KEEP_ALIVE);
		this.enableEncryption = configUtil.getBoolean(configSectionName, ConfigConst.ENABLE_CRYPT_KEY);
		this.pemFileName = configUtil.getProperty(configSectionName, ConfigConst.CERT_FILE_KEY);

		this.useAsyncClient = configUtil.getBoolean(ConfigConst.MQTT_GATEWAY_SERVICE, ConfigConst.USE_ASYNC_CLIENT_KEY);
		this.clientID = configUtil.getProperty(ConfigConst.GATEWAY_DEVICE, ConfigConst.DEVICE_LOCATION_ID_KEY,
				MqttClient.generateClientId());

		this.persistence = new MemoryPersistence();
		this.connOpts = new MqttConnectOptions();

		this.connOpts.setKeepAliveInterval(this.brokerKeepAlive);
		this.connOpts.setCleanSession(this.useCleanSession);
		this.connOpts.setAutomaticReconnect(this.enableAutoReconnect);

		if (this.enableEncryption) {
			initSecureConnectionParameters(configSectionName);
		}

		if (configUtil.hasProperty(configSectionName, ConfigConst.CRED_FILE_KEY)) {
			initCredentialConnectionParameters(configSectionName);
		}

		this.brokerAddr = this.protocol + "://" + this.host + ":" + this.port;
		_Logger.info("Using URL for broker conn: " + this.brokerAddr);
	}

	/**
	 * Called by {@link #initClientParameters(String)} to load credentials.
	 * 
	 * @param configSectionName The name of the configuration section to use for
	 *                          the MQTT client configuration parameters.
	 */
	private void initCredentialConnectionParameters(String configSectionName) {
		ConfigUtil configUtil = ConfigUtil.getInstance();

		try {
			_Logger.info("Checking if credentials file exists and is loadable...");

			Properties props = configUtil.getCredentials(configSectionName);

			if (props != null) {
				this.connOpts.setUserName(props.getProperty(ConfigConst.USER_NAME_TOKEN_KEY, ""));
				this.connOpts.setPassword(props.getProperty(ConfigConst.USER_AUTH_TOKEN_KEY, "").toCharArray());

				_Logger.info("Credentials now set.");
			} else {
				_Logger.warning("No credentials are set.");
			}
		} catch (Exception e) {
			_Logger.log(Level.WARNING, "Credential file non-existent. Disabling auth requirement.");
		}
	}

	/**
	 * Called by {@link #initClientParameters(String)} to enable encryption.
	 * 
	 * @param configSectionName The name of the configuration section to use for
	 *                          the MQTT client configuration parameters.
	 */
	private void initSecureConnectionParameters(String configSectionName) {
		ConfigUtil configUtil = ConfigUtil.getInstance();

		try {
			_Logger.info("Configuring TLS...");

			if (this.pemFileName != null) {
				File file = new File(this.pemFileName);

				if (file.exists()) {
					_Logger.info("PEM file valid. Using secure connection: " + this.pemFileName);
				} else {
					this.enableEncryption = false;
					_Logger.log(Level.WARNING, "PEM file invalid. Using insecure connection: " + this.pemFileName,
							new Exception());
					return;
				}
			}

			SSLSocketFactory sslFactory = SimpleCertManagementUtil.getInstance().loadCertificate(this.pemFileName);
			this.connOpts.setSocketFactory(sslFactory);

			this.port = configUtil.getInteger(configSectionName, ConfigConst.SECURE_PORT_KEY,
					ConfigConst.DEFAULT_MQTT_SECURE_PORT);
			this.protocol = ConfigConst.DEFAULT_MQTT_SECURE_PROTOCOL;

			_Logger.info("TLS enabled.");
		} catch (Exception e) {
			_Logger.log(Level.SEVERE, "Failed to initialize secure MQTT connection. Using insecure connection.", e);
			this.enableEncryption = false;
		}
	}

	private void messageArrivedActuatorData(String topic, MqttMessage msg) throws Exception {
		String payload = new String(msg.getPayload());
		_Logger.info("Mensaje recibido en el tema: " + topic);

		try {
			// Procesar el mensaje como ActuatorData
			ActuatorData actuatorData = DataUtil.getInstance().jsonToActuatorData(payload);
			_Logger.info("Recibido ActuatorData: " + actuatorData.getValue());

			// Llamar al listener de ActuatorData
			if (this.dataMsgListener != null) {
				this.dataMsgListener.handleActuatorCommandResponse(ResourceNameEnum.CDA_ACTUATOR_RESPONSE_RESOURCE,
						actuatorData);
			}
		} catch (Exception e) {
			_Logger.warning("Error al procesar ActuatorData: " + e.getMessage());
		}
	}

	private void messageArrivedSensorData(String topic, MqttMessage msg) throws Exception {
		String payload = new String(msg.getPayload());
		_Logger.info("Mensaje recibido en el tema: " + topic);

		try {
			// Procesar el mensaje como SensorData
			SensorData sensorData = DataUtil.getInstance().jsonToSensorData(payload);
			_Logger.info("Recibido SensorData: " + sensorData.getValue());

			// Llamar al listener de SensorData
			if (this.dataMsgListener != null) {
				this.dataMsgListener.handleSensorMessage(ResourceNameEnum.CDA_SENSOR_MSG_RESOURCE, sensorData);
			}
		} catch (Exception e) {
			_Logger.warning("Error al procesar SensorData: " + e.getMessage());
		}
	}

	private void messageArrivedSystemPerformanceData(String topic, MqttMessage msg) throws Exception {
		String payload = new String(msg.getPayload());
		_Logger.info("Mensaje recibido en el tema: " + topic);

		try {
			// Procesar el mensaje como SystemPerformanceData
			SystemPerformanceData systemPerformanceData = DataUtil.getInstance().jsonToSystemPerformanceData(payload);
			// _Logger.info("Recibido SystemPerformanceData: " +
			// systemPerformanceData.getValue());

			// Llamar al listener de SystemPerformanceData
			if (this.dataMsgListener != null) {
				this.dataMsgListener.handleSystemPerformanceMessage(ResourceNameEnum.CDA_SYSTEM_PERF_MSG_RESOURCE,
						systemPerformanceData);
			}
		} catch (Exception e) {
			_Logger.warning("Error al procesar SystemPerformanceData: " + e.getMessage());
		}
	}
}
