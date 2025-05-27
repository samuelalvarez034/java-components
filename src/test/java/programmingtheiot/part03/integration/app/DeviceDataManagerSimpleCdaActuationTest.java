package programmingtheiot.part03.integration.app;

import java.util.logging.Logger;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import programmingtheiot.common.ConfigConst;
import programmingtheiot.common.ConfigUtil;
import programmingtheiot.common.ResourceNameEnum;
import programmingtheiot.data.DataUtil;
import programmingtheiot.data.SensorData;
import programmingtheiot.gda.app.DeviceDataManager;
import programmingtheiot.gda.connection.IPubSubClient;
import programmingtheiot.gda.connection.MqttClientConnector;

public class DeviceDataManagerSimpleCdaActuationTest
{
	@Test
	public void testSendActuationEventsToCda()
	{
		DeviceDataManager devDataMgr = new DeviceDataManager();
		devDataMgr.startManager();

		ConfigUtil cfgUtil = ConfigUtil.getInstance();

		float nominalVal = cfgUtil.getFloat(ConfigConst.GATEWAY_DEVICE, "nominalHumiditySetting");
		float lowVal = cfgUtil.getFloat(ConfigConst.GATEWAY_DEVICE, "triggerHumidifierFloor");
		float highVal = cfgUtil.getFloat(ConfigConst.GATEWAY_DEVICE, "triggerHumidifierCeiling");
		int delay = cfgUtil.getInteger(ConfigConst.GATEWAY_DEVICE, "humidityMaxTimePastThreshold");

		// Ejecutar prueba simple
		generateAndProcessHumiditySensorDataSequence(devDataMgr, nominalVal, lowVal, highVal, delay);

		devDataMgr.stopManager();
	}

	private void generateAndProcessHumiditySensorDataSequence(DeviceDataManager ddm, float nominalVal, float lowVal, float highVal, int delay)
	{
		SensorData sd = new SensorData();
		sd.setName("Test Humidity Sensor");
		sd.setLocationID("constraineddevice001");
		sd.setTypeID(ConfigConst.HUMIDITY_SENSOR_TYPE);

		// Paso 1: Enviar valor normal dos veces (no debe haber activación)
		sd.setValue(nominalVal);
		ddm.handleSensorMessage(ResourceNameEnum.CDA_SENSOR_MSG_RESOURCE, sd);
		waitForSeconds(2);

		sd.setValue(nominalVal);
		ddm.handleSensorMessage(ResourceNameEnum.CDA_SENSOR_MSG_RESOURCE, sd);
		waitForSeconds(2);

		// Paso 2: Enviar valor fuera de umbral por más del tiempo límite → debe activar
		sd.setValue(lowVal - 2);
		ddm.handleSensorMessage(ResourceNameEnum.CDA_SENSOR_MSG_RESOURCE, sd);
		waitForSeconds(delay + 1);

		sd.setValue(lowVal - 1);
		ddm.handleSensorMessage(ResourceNameEnum.CDA_SENSOR_MSG_RESOURCE, sd);
		waitForSeconds(delay + 1);

		// Paso 3: Regresar a valor normal → debe desactivar
		sd.setValue(nominalVal);
		ddm.handleSensorMessage(ResourceNameEnum.CDA_SENSOR_MSG_RESOURCE, sd);
		waitForSeconds(delay + 1);
	}

	private void waitForSeconds(int seconds)
	{
		try {
			Thread.sleep(seconds * 1000);
		} catch (InterruptedException e) {
			// ignorar
		}
	}
}
