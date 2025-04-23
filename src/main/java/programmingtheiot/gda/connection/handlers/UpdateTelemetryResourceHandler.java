package programmingtheiot.gda.connection.handlers;

import java.util.logging.Logger;

import org.eclipse.californium.core.server.resources.CoapExchange;
import org.eclipse.californium.core.coap.CoAP.ResponseCode;

import programmingtheiot.common.IDataMessageListener;
import programmingtheiot.common.ResourceNameEnum;
import programmingtheiot.data.SensorData;
import programmingtheiot.data.DataUtil;

public class UpdateTelemetryResourceHandler extends GenericCoapResourceHandler
{
	private static final Logger _Logger = Logger.getLogger(UpdateTelemetryResourceHandler.class.getName());

	private IDataMessageListener dataMsgListener = null;

	public UpdateTelemetryResourceHandler(String resourceName)
	{
		super(resourceName);
	}

	public void setDataMessageListener(IDataMessageListener listener)
	{
		if (listener != null) {
			this.dataMsgListener = listener;
		}
	}

	@Override
	public void handlePUT(CoapExchange context)
	{
		ResponseCode code = ResponseCode.NOT_ACCEPTABLE;

		context.accept();

		if (this.dataMsgListener != null) {
			try {
				String jsonData = new String(context.getRequestPayload());

				SensorData sensorData =
					DataUtil.getInstance().jsonToSensorData(jsonData);

				this.dataMsgListener.handleSensorMessage(
					ResourceNameEnum.CDA_SENSOR_MSG_RESOURCE, sensorData);

				code = ResponseCode.CHANGED;
			} catch (Exception e) {
				_Logger.warning("Error al manejar solicitud PUT: " + e.getMessage());
				code = ResponseCode.BAD_REQUEST;
			}
		} else {
			_Logger.info("No hay listener disponible. PUT ignorado.");
			code = ResponseCode.CONTINUE;
		}

		String msg = "Solicitud PUT manejada para recurso: " + super.getName();
		context.respond(code, msg);
	}

	@Override
	public void handleGET(CoapExchange context)
	{
		context.accept();
		_Logger.info("Solicitud GET recibida.");
		context.respond(ResponseCode.CONTENT, "GET en recurso: " + super.getName());
	}

	@Override
	public void handlePOST(CoapExchange context)
	{
		context.accept();
		_Logger.info("Solicitud POST recibida.");
		context.respond(ResponseCode.CHANGED, "POST en recurso: " + super.getName());
	}

	@Override
	public void handleDELETE(CoapExchange context)
	{
		context.accept();
		_Logger.info("Solicitud DELETE recibida.");
		context.respond(ResponseCode.DELETED, "DELETE en recurso: " + super.getName());
	}
}
