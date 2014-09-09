%% ===================================================================
%% @author Tomasz Lichon
%% @copyright (C): 2014 ACK CYFRONET AGH
%% This software is released under the MIT license
%% cited in 'LICENSE.txt'.
%% @end
%% ===================================================================
%% @doc This header provides common definitions for cdmi_objects,
%% (they can be accessed via cdmi_object.erl handler)
%% ===================================================================

%% the default json response for get/put cdmi_object will contain this entities, they can be choosed selectively by appending '?name1;name2' list to the request url
-define(default_get_file_opts, [<<"objectType">>, <<"objectID">>, <<"objectName">>, <<"parentURI">>, <<"parentID">>, <<"capabilitiesURI">>, <<"completionStatus">>, <<"metadata">>, <<"mimetype">>, <<"valuetransferencoding">>, <<"valuerange">>, <<"value">>]).
-define(default_put_file_opts, [<<"objectType">>, <<"objectID">>, <<"objectName">>, <<"parentURI">>, <<"parentID">>, <<"capabilitiesURI">>, <<"completionStatus">>, <<"metadata">>, <<"mimetype">>]).

%% Keys of mimetype end encoding attrs
-define(mimetype_xattr_key, <<"cdmi_mimetype">>).
-define(encoding_xattr_key, <<"cdmi_valuetransferencoding">>).

%% Default values of mimetype end encoding attrs
-define(mimetype_default_value, <<"application/octet-stream">>).
-define(encoding_default_value, <<"base64">>).
