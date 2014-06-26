%% ===================================================================
%% @author Tomasz Lichon
%% @copyright (C): 2014 ACK CYFRONET AGH
%% This software is released under the MIT license
%% cited in 'LICENSE.txt'.
%% @end
%% ===================================================================
%% @doc: This module implements rest_module_behaviour, handles GET requests directed at /rest/connection_check, and always returns <<"rest">>,
%% it's used by globalregistry to test connection
%% @end
%% ===================================================================
-module(rest_connection_check).
-behaviour(rest_module_behaviour).

-include("veil_modules/control_panel/vcn_common.hrl").
-include("veil_modules/control_panel/rest_messages.hrl").
-include("err.hrl").
-include("veil_modules/control_panel/connection_check_values.hrl").


%% API
-export([methods_and_versions_info/1, exists/3]).
-export([get/3, delete/3, post/4, put/4]).

%% methods_and_versions_info/1
%% ====================================================================
%% @doc Should return list of tuples, where each tuple consists of version of API version and
%% list of methods available in the API version. Latest version must be at the end of list.
%% e.g.: `[{<<"1.0">>, [<<"GET">>, <<"POST">>]}]'
%% @end
-spec methods_and_versions_info(req()) -> {[{binary(), [binary()]}], req()}.
%% ====================================================================
methods_and_versions_info(Req) ->
    {[{<<"1.0">>, [<<"GET">>]}], Req}.

%% exists/3
%% ====================================================================
%% @doc Should return whether resource specified by given ID exists.
%% Will be called for GET, PUT and DELETE when ID is contained in the URL.
%% @end
-spec exists(req(), binary(), binary()) -> {boolean(), req()}.
%% ====================================================================
exists(Req, _Version, _Id) ->
    {true,Req}.

%% get/3
%% ====================================================================
%% @doc Will be called for GET requests. Must return one of answers
%% described in rest_module_behaviour.
%% @end
-spec get(req(), binary(), binary()) -> {Response, req()} when
    Response :: ok | {body, binary()} | {stream, integer(), function()} | error | {error, binary()}.
%% ====================================================================
get(Req, <<"1.0">>, _Id) ->
    {{body, ?rest_connection_check_value},Req}.

%% delete/3
%% ====================================================================
%% @doc Will be called for DELETE request on given ID. Must return one of answers
%% described in rest_module_behaviour.
%% @end
-spec delete(req(), binary(), binary()) -> {Response, req()} when
    Response :: ok | {body, binary()} | {stream, integer(), function()} | error | {error, binary()}.
%% ====================================================================
delete(Req, <<"1.0">>, _Id) ->
    ErrorRec = ?report_error(?error_method_unsupported),
    {{error, rest_utils:error_reply(ErrorRec)}, Req}.

%% post/4
%% ====================================================================
%% @doc Will be called for POST request. Must return one of answers
%% described in rest_module_behaviour.
%% @end
-spec post(req(), binary(), binary(), term()) -> {Response, req()} when
    Response :: ok | {body, binary()} | {stream, integer(), function()} | error | {error, binary()}.
%% ====================================================================
post(Req, <<"1.0">>, _Id, _Data) ->
    ErrorRec = ?report_error(?error_method_unsupported),
    {{error, rest_utils:error_reply(ErrorRec)}, Req}.

%% put/4
%% ====================================================================
%% @doc Will be called for PUT request on given ID. Must return one of answers
%% described in rest_module_behaviour.
%% @end
-spec put(req(), binary(), binary(), term()) -> {Response, req()} when
    Response :: ok | {body, binary()} | {stream, integer(), function()} | error | {error, binary()}.
%% ====================================================================
put(Req, <<"1.0">>, _Id, _Data) ->
    ErrorRec = ?report_error(?error_method_unsupported),
    {{error, rest_utils:error_reply(ErrorRec)}, Req}.