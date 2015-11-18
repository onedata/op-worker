%%%-------------------------------------------------------------------
%%% @author Tomasz Lichon
%%% @copyright (C): 2014 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%% @end
%%%-------------------------------------------------------------------
%%% @doc This header provides common definitions for cdmi_objects,
%%% (they can be accessed via cdmi_object.erl handler)
%%% @end
%%%-------------------------------------------------------------------

%% the default json response for get/put cdmi_object will contain this entities, they can be choosed selectively by appending '?name1;name2' list to the request url
-define(DEFAULT_GET_FILE_OPTS, [<<"objectType">>, <<"objectID">>, <<"objectName">>, <<"parentURI">>, <<"parentID">>, <<"capabilitiesURI">>, <<"completionStatus">>, <<"metadata">>, <<"mimetype">>, <<"valuetransferencoding">>, <<"valuerange">>, <<"value">>]).
-define(DEFAULT_PUT_FILE_OPTS, [<<"objectType">>, <<"objectID">>, <<"objectName">>, <<"parentURI">>, <<"parentID">>, <<"capabilitiesURI">>, <<"completionStatus">>, <<"metadata">>, <<"mimetype">>]).

%% Keys of special cdmi attrs
-define(MIMETYPE_XATTR_KEY, <<"cdmi_mimetype">>).
-define(ENCODING_XATTR_KEY, <<"cdmi_valuetransferencoding">>).
-define(COMPLETION_STATUS_XATTR_KEY, <<"cdmi_completion_status">>).

%% Default values of special cdmi attrs
-define(MIMETYPE_DEFAULT_VALUE, <<"application/octet-stream">>).
-define(ENCODING_DEFAULT_VALUE, <<"base64">>).
-define(COMPLETION_STATUS_DEFAULT_VALUE, <<"Complete">>).
