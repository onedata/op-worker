%%%--------------------------------------------------------------------
%%% @author Piotr Ociepka
%%% @copyright (C) 2015 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%--------------------------------------------------------------------
%%% @doc This file defines errors thrown while handling CDMI request.
%%% @end
%%%--------------------------------------------------------------------

-ifndef(CDMI_ERRORS_HRL).
-define(CDMI_ERRORS_HRL, 1).

%% HTTP error code 400 and request sent to client
-define(unsupported_version,
    {400, [{<<"error_unsupported_version">>, <<"Given CDMI version is not supported. Use 1.1.1 instead.">>}]}).

%% HTTP error code 400 and request sent to client
-define(invalid_range,
    {400, [{<<"error_invalid_range">>, <<"Given range is invalid">>}]}).

-endif.