%% ===================================================================
%% @author Tomasz Lichon
%% @copyright (C): 2014 ACK CYFRONET AGH
%% This software is released under the MIT license 
%% cited in 'LICENSE.txt'.
%% @end
%% ===================================================================
%% @doc This behaviour should be implemented by modules handling cdmi requests.
%% It ensures the presence of required callbacks.
%% @end
%% ===================================================================

-module(cdmi_module_behaviour).

-include("veil_modules/control_panel/cdmi.hrl").

%% ====================================================================
%% Callbacks descriptions
%% ====================================================================

%% NOTE!
%% All callbacks take cowboy #http_req{} record as an argument. This is
%% so that the callbacks can use or alter the request.
%% The request record must be returned from every callback, and if it
%% has been altered, the changed version should be returned for
%% the modifications to persist.
%% Another argument passed to every callback is #state defined in cdmi header file.
%% It is State record containg context of current request

%% allowed_methods/2
%% ====================================================================
%% @doc
%% Returns binary list of methods that are allowed (i.e GET, PUT, DELETE).
%% @end
%% ====================================================================
-callback allowed_methods(req(), #state{}) -> {[binary()], req(), #state{}}.

%% malformed_request/2
%% ====================================================================
%% @doc Checks if request contains all mandatory fields and their values are set properly
%% depending on requested operation
%% @end
%% ====================================================================
-callback malformed_request(req(), #state{}) -> {boolean(), req(), #state{}}.

%% resource_exists/2
%% ====================================================================
%% @doc
%% Determines if resource, that can be obtained from state, exists.
%% @end
%% ====================================================================
-callback resource_exists(req(), #state{}) -> {boolean(), req(), #state{}}.

%% content_types_provided/2
%% ====================================================================
%% @doc
%% Returns content types that can be provided and what functions should be used to process the request.
%% Before adding new content type make sure that adequate routing function
%% exists in cdmi_handler
%% @end
%% ====================================================================
-callback content_types_provided(req(), #state{}) -> {[{ContentType,Method}], req(), #state{}} when
    ContentType :: binary(),
    Method :: atom().

%% content_types_accepted/2
%% ====================================================================
%% @doc
%% Returns content-types that are accepted and what
%% functions should be used to process the requests.
%% Before adding new content type make sure that adequate routing function
%% exists in cdmi_handler
%% @end
%% ====================================================================
-callback content_types_accepted(req(), #state{}) -> {[{ContentType,Method}], req(), #state{}} when
    ContentType :: binary(),
    Method :: atom().

%% delete_resource/3
%% ====================================================================
%% @doc Deletes the resource. Returns whether the deletion was successful.
%% @end
%% ====================================================================
-callback delete_resource(req(), #state{}) -> {term(), req(), #state{}}.
