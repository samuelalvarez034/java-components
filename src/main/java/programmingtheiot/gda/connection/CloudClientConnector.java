/**
 * This class is part of the Programming the Internet of Things project.
 * 
 * It is provided as a simple shell to guide the student and assist with
 * implementation for the Programming the Internet of Things exercises,
 * and designed to be modified by the student as needed.
 */

package programmingtheiot.gda.connection;

import java.util.logging.Level;
import java.util.logging.Logger;

import org.eclipse.paho.client.mqttv3.IMqttMessageListener;
import org.eclipse.paho.client.mqttv3.MqttMessage;

import programmingtheiot.common.ConfigConst;
import programmingtheiot.common.ConfigUtil;
import programmingtheiot.common.IDataMessageListener;
import programmingtheiot.common.ResourceNameEnum;
import programmingtheiot.data.SensorData;
import programmingtheiot.data.SystemPerformanceData;

import java.util.Properties;

import programmingtheiot.data.ActuatorData;
import programmingtheiot.data.DataUtil;

/**
 * Shell representation of class for student implementation.
 *
 */
public class CloudClientConnector implements ICloudClient, IConnectionListener {
	// static
	private String topicPrefix = "";
	private MqttClientConnector mqttClient = null;
	private IDataMessageListener dataMsgListener = null;
	private int qosLevel = 1;

	private static final Logger _Logger = Logger.getLogger(CloudClientConnector.class.getName());

	// private var's

	// constructors

	/**
	 * Default.
	 * 
	 */
	public CloudClientConnector() {
		super();
		ConfigUtil configUtil = ConfigUtil.getInstance();
		this.topicPrefix = configUtil.getProperty(ConfigConst.CLOUD_GATEWAY_SERVICE, ConfigConst.BASE_TOPIC_KEY);

		if (topicPrefix == null) {
			topicPrefix = "/";
		} else {
			if (!topicPrefix.endsWith("/")) {
				topicPrefix += "/";
			}
		}
	}

	// public methods

	@Override
	public boolean connectClient() {
		if (this.mqttClient == null) {
			this.mqttClient = new MqttClientConnector(ConfigConst.CLOUD_GATEWAY_SERVICE);
			this.mqttClient.setConnectionListener(this);
		}

		return this.mqttClient.connectClient();
	}

	@Override
	public boolean disconnectClient() {
		if (this.mqttClient != null && this.mqttClient.isConnected()) {
			return this.mqttClient.disconnectClient();
		}
		return false;
	}

	@Override
	public boolean setDataMessageListener(IDataMessageListener listener) {
		if (listener != null) {
			this.dataMsgListener = listener;
			return true;
		} else {
			return false;
		}
	}

	@Override
	public boolean sendEdgeDataToCloud(ResourceNameEnum resource, SensorData data) {
		if (resource != null && data != null) {
			String payload = DataUtil.getInstance().sensorDataToJson(data);
			return publishMessageToCloud(resource, data.getName(), payload);
		}
		return false;
	}

	@Override
	public boolean sendEdgeDataToCloud(ResourceNameEnum resource, SystemPerformanceData data) {
		if (resource != null && data != null) {
			SensorData cpuData = new SensorData();
			cpuData.updateData(data);
			cpuData.setName(ConfigConst.CPU_UTIL_NAME);
			cpuData.setValue(data.getCpuUtilization());

			boolean cpuSuccess = sendEdgeDataToCloud(resource, cpuData);

			SensorData memData = new SensorData();
			memData.updateData(data);
			memData.setName(ConfigConst.MEM_UTIL_NAME);
			memData.setValue(data.getMemoryUtilization());

			boolean memSuccess = sendEdgeDataToCloud(resource, memData);

			return (cpuSuccess == memSuccess);
		}
		return false;
	}

	@Override
	public boolean subscribeToCloudEvents(ResourceNameEnum resource) {
		if (this.mqttClient != null && this.mqttClient.isConnected()) {
			String topicName = createTopicName(resource);
			this.mqttClient.subscribeToTopic(topicName, this.qosLevel);
			return true;
		}
		return false;
	}

	@Override
	public boolean unsubscribeFromCloudEvents(ResourceNameEnum resource) {
		if (this.mqttClient != null && this.mqttClient.isConnected()) {
			String topicName = createTopicName(resource);
			this.mqttClient.unsubscribeFromTopic(topicName);
			return true;
		}
		return false;
	}

	// private methods

	private String createTopicName(ResourceNameEnum resource) {
		return createTopicName(resource.getDeviceName(), resource.getResourceType());
	}

	private String createTopicName(String deviceName, String resourceTypeName) {
		StringBuilder buf = new StringBuilder();

		if (deviceName != null && deviceName.trim().length() > 0) {
			buf.append(topicPrefix).append(deviceName);
		}

		if (resourceTypeName != null && resourceTypeName.trim().length() > 0) {
			buf.append('/').append(resourceTypeName);
		}

		return buf.toString().toLowerCase();
	}

	private boolean publishMessageToCloud(ResourceNameEnum resource, String itemName, String payload) {
		String topicName = createTopicName(resource) + "-" + itemName;
		return publishMessageToCloud(topicName, payload);
	}

	private boolean publishMessageToCloud(String topicName, String payload) {
		try {
			_Logger.finest("Publishing to: " + topicName);
			this.mqttClient.publishMessage(topicName, payload.getBytes(), this.qosLevel);
			return true;
		} catch (Exception e) {
			_Logger.warning("Failed to publish message: " + topicName);
		}
		return false;
	}

	private class LedEnablementMessageListener implements IMqttMessageListener {
		private IDataMessageListener dataMsgListener = null;

		private ResourceNameEnum resource = ResourceNameEnum.CDA_ACTUATOR_CMD_RESOURCE;

		private int typeID = ConfigConst.LED_ACTUATOR_TYPE;
		private String itemName = ConfigConst.LED_ACTUATOR_NAME;

		LedEnablementMessageListener(IDataMessageListener dataMsgListener) {
			this.dataMsgListener = dataMsgListener;
		}

		public ResourceNameEnum getResource() {
			return this.resource;
		}

		@Override
		public void messageArrived(String topic, MqttMessage message) throws Exception {
			try {
				String jsonData = new String(message.getPayload());

				ActuatorData actuatorData = DataUtil.getInstance().jsonToActuatorData(jsonData);

				actuatorData.setLocationID(ConfigConst.CONSTRAINED_DEVICE);
				actuatorData.setTypeID(this.typeID);
				actuatorData.setName(this.itemName);

				int val = (int) actuatorData.getValue();

				switch (val) {
					case ConfigConst.ON_COMMAND:
						_Logger.info("Received LED enablement message [ON].");
						actuatorData.setStateData("LED switching ON");
						break;

					case ConfigConst.OFF_COMMAND:
						_Logger.info("Received LED enablement message [OFF].");
						actuatorData.setStateData("LED switching OFF");
						break;

					default:
						return;
				}

				if (this.dataMsgListener != null) {
					jsonData = DataUtil.getInstance().actuatorDataToJson(actuatorData);
					this.dataMsgListener.handleIncomingMessage(
							ResourceNameEnum.CDA_ACTUATOR_CMD_RESOURCE, jsonData);
				}
			} catch (Exception e) {
				_Logger.warning("Failed to convert message payload to ActuatorData.");
			}
		}

	}

	@Override
	public void onConnect() {
		_Logger.info("Handling CSP subscriptions and device topic provisioning...");

		LedEnablementMessageListener leml = new LedEnablementMessageListener(this.dataMsgListener);

		ActuatorData ad = new ActuatorData();
		ad.setAsResponse();
		ad.setName(ConfigConst.LED_ACTUATOR_NAME);
		ad.setValue((float) -1.0);

		String ledTopic = createTopicName(leml.getResource().getDeviceName(), ad.getName());
		String adJson = DataUtil.getInstance().actuatorDataToJson(ad);

		this.publishMessageToCloud(ledTopic, adJson);
		this.mqttClient.subscribeToTopic(ledTopic, this.qosLevel, leml);
	}

	@Override
	public void onDisconnect() {
		_Logger.info("MQTT client disconnected. Nothing else to do.");
	}

	private String createTopicName(ResourceNameEnum resource, String itemName) {
		return (createTopicName(resource) + "-" + itemName).toLowerCase();
	}

}
