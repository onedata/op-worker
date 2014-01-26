.. _gsi_nif:

gsi_nif
=======

	:Authors: Rafal Slota
	:Copyright: This software is released under the :ref:`license`.
	:Descritpion: This module loads GSI NIF libs

Function Index
~~~~~~~~~~~~~~~

	* :ref:`start/1 <gsi_nif:start/1>`
	* :ref:`verify_cert_c/4 <gsi_nif:verify_cert_c/4>`

Function Details
~~~~~~~~~~~~~~~~~

	.. _`gsi_nif:start/1`:

	.. function:: start(Prefix :: string()) -> ok | {error, Reason :: term()}
		:noindex:

	This method loads NIF library into erlang VM. This should be used once before using any other method in this module.

	.. _`gsi_nif:verify_cert_c/4`:

	.. function:: verify_cert_c(PeerCert :: binary(), PeerChain :: [binary()], CAChain :: [binary()], CRLs :: [binary()]) -> {ok, 1} | {ok, 0, Errno :: integer()} | {error, Reason :: term()}
		:noindex:

	This method validates peer certificate. All input certificates should be DER encoded. {ok, 1} result means that peer is valid, {ok, 0, Errno} means its not. Errno is a raw errno returned by openssl/globus library {error, Reason} on the other hand is returned only when something went horribly wrong during validation.

