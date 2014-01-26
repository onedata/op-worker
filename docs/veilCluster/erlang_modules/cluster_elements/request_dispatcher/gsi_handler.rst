.. _gsi_handler:

gsi_handler
===========

	:Authors: Rafal Slota
	:Copyright: This software is released under the :ref:`license`.
	:Descritpion: This module manages GSI validation

Function Index
~~~~~~~~~~~~~~~

	* :ref:`call/3 <gsi_handler:call/3>`
	* :ref:`find_eec_cert/3 <gsi_handler:find_eec_cert/3>`
	* :ref:`init/0 <gsi_handler:init/0>`
	* :ref:`is_proxy_certificate/1 <gsi_handler:is_proxy_certificate/1>`
	* :ref:`load_certs/1 <gsi_handler:load_certs/1>`
	* :ref:`proxy_subject/1 <gsi_handler:proxy_subject/1>`
	* :ref:`update_crls/1 <gsi_handler:update_crls/1>`
	* :ref:`verify_callback/3 <gsi_handler:verify_callback/3>`

Function Details
~~~~~~~~~~~~~~~~~

	.. _`gsi_handler:call/3`:

	.. function:: call(Module :: atom(), Method :: atom(), Args :: [term()]) -> ok | no_return()
		:noindex:

	Calls apply(Module, Method, Args) on one of started slave nodes. If slave node is down, initializes restart procedure and tries to use another node. However is all nodes are down, error is returned and GSI action is interrupted (e.g. peer verification fails).

	.. _`gsi_handler:find_eec_cert/3`:

	.. function:: find_eec_cert(CurrentOtp :: #'OTPCertificate'{}, Chain :: [#'OTPCertificate'{}], IsProxy :: boolean()) -> {ok, #'OTPCertificate'{}} | no_return()
		:noindex:

	For given proxy certificate returns its EEC

	.. _`gsi_handler:init/0`:

	.. function:: init() -> ok
		:noindex:

	Initializes GSI Handler. This method should be called once, before using any other method from this module.

	.. _`gsi_handler:is_proxy_certificate/1`:

	.. function:: is_proxy_certificate(OtpCert :: #'OTPCertificate'{}) -> boolean()
		:noindex:

	Checks is given OTP Certificate has an proxy extension

	.. _`gsi_handler:load_certs/1`:

	.. function:: load_certs(CADir :: string()) -> ok | no_return()
		:noindex:

	Loads all PEM encoded CA certificates from given directory along with their CRL certificates (if any). Note that CRL certificates should also be PEM encoded and the CRL filename should match their CA filename but with '.crl' extension.

	.. _`gsi_handler:proxy_subject/1`:

	.. function:: proxy_subject(OtpCert :: #'OTPCertificate'{}) -> {rdnSequence, [#'AttributeTypeAndValue'{}]}
		:noindex:

	Returns subject of given certificate. If proxy certificate is given, EEC subject is returned.

	.. _`gsi_handler:update_crls/1`:

	.. function:: update_crls(CADir :: string()) -> ok | no_return()
		:noindex:

	Updates CRL certificates based on their distribution point (x509 CA extension). Not yet fully implemented.

	.. _`gsi_handler:verify_callback/3`:

	.. function:: verify_callback(OtpCert :: #'OTPCertificate'{}, Status :: term(), Certs :: [#'OTPCertificate'{}]) -> {valid, UserState :: any()} | {fail, Reason :: term()}
		:noindex:

	This method is an registered callback, called foreach peer certificate. This callback saves whole certificate chain in GSI ETS based state for further use.

