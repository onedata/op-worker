Errors & Validation
===================

	If a request fails due to client error, the resource will return an HTTP response code in the 4xx range or 500. These can be broadly categorised into: 
	
	========================== ===================================================
	HTTP Code                  | Description
	========================== ===================================================
	404 Not Found              | The requested resource could not be found but 
	                           | may be available again in the future.
	405 Method Not Allowed     | A request was made of a resource using a request 
	                           | method not supported by that resource.
	415 Unsupported Media Type | The request entity has a media type which 
	                           | the server or resource does not support.
	422 Unprocessable Entity   | The request was well-formed but was unable 
	                           | to be followed due to semantic errors.
	500 Internal Server Error  | A generic error message, given when 
	                           | an unexpected condition was encountered.
	========================== ===================================================

	Moreover in each case a JSON object will be returned providing detailed description:

		* error type: status (*alert*, *warning*, *error*)
		* error code: code (*string*)
		* error description: description (string)
