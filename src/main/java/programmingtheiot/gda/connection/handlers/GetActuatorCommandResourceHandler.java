package programmingtheiot.gda.connection.handlers;

import java.util.logging.Logger;

import org.eclipse.californium.core.CoapResource;
import org.eclipse.californium.core.server.resources.CoapExchange;
import org.eclipse.californium.core.coap.CoAP.ResponseCode;

import programmingtheiot.common.IActuatorDataListener;
import programmingtheiot.data.ActuatorData;
import programmingtheiot.data.DataUtil;

/**
 * Resource handler for GET-based observable actuator commands.
 */
public class GetActuatorCommandResourceHandler extends CoapResource implements IActuatorDataListener
{
	// logging
	private static final Logger _Logger =
		Logger.getLogger(GetActuatorCommandResourceHandler.class.getName());

	// actuator data
	private ActuatorData actuatorData = new ActuatorData();

	/**
	 * Constructor with resource name.
	 * 
	 * @param resourceName The name of the resource.
	 */
	public GetActuatorCommandResourceHandler(String resourceName)
	{
		super(resourceName);

		// set the resource as observable
		super.setObservable(true);
		_Logger.info("Created observable resource: " + resourceName);
	}

	/**
	 * Callback for actuator data updates.
	 * 
	 * @param data The updated actuator data.
	 * @return True if successful, false otherwise.
	 */
	@Override
	public boolean onActuatorDataUpdate(ActuatorData data)
	{
		if (data != null && this.actuatorData != null) {
			this.actuatorData.updateData(data);

			// notify observers
			super.changed();

			_Logger.fine("Actuator data updated for URI: " + super.getURI()
				+ " | Data value = " + this.actuatorData.getValue());

			return true;
		}

		return false;
	}

	/**
	 * Handles a CoAP GET request.
	 * 
	 * @param context The exchange context.
	 */
	@Override
	public void handleGET(CoapExchange context)
	{
		if (context == null) {
			_Logger.warning("Received null CoapExchange in handleGET.");
			return;
		}

		_Logger.info("Handling GET request from: " + context.getSourceAddress());

		// accept the request
		context.accept();

		// convert ActuatorData to JSON
		String jsonData = DataUtil.getInstance().actuatorDataToJson(this.actuatorData);

		// respond with the JSON content and success code
		context.respond(ResponseCode.CONTENT, jsonData);
	}
}
