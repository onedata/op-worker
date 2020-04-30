%%%-------------------------------------------------------------------
%%% @author Łukasz Opioła
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2019 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Common definitions for REST.
%%% @end
%%%-------------------------------------------------------------------

-include_lib("ctool/include/http/codes.hrl").

-ifndef(REST_HRL).
-define(REST_HRL, 1).

% (bound GRI) - GRI with bindings that is converted to proper GRI
% when bindings are resolved.
-record(b_gri, {
    type :: undefined | gs_protocol:entity_type(),
    id :: undefined | rest_handler:binding(),
    aspect :: undefined | gs_protocol:aspect(),
    scope = private :: gs_protocol:scope()
}).

%% Record containing the state of REST request.
-record(rest_req, {
    method = 'GET' :: rest_handler:method(),
    parse_body = ignore :: rest_handler:parse_body(),
    consumes = ['*'] :: ['*'] | [binary()],
    produces = [<<"application/json">>] :: [binary()],
    b_gri :: rest_handler:bound_gri()
}).

%% Record representing REST response.
-record(rest_resp, {
    code = ?HTTP_200_OK :: integer(),
    headers = #{} :: #{binary() => binary()},
    body = {binary, <<"">>} :: json_utils:json_term() | {binary, binary()}
}).

% Convenience macros used in rest_req, they will be processed before passed
% further to internal logic.
-define(BINDING(__KEY), {binding, __KEY}).
-define(OBJECTID_BINDING(__KEY), {objectid_binding, __KEY}).
-define(PATH_BINDING, path_binding).

% Convenience macros used for constructing REST replies
-define(OK_REPLY(__Body), #rest_resp{code = ?HTTP_200_OK, body = __Body}).
-define(NO_CONTENT_REPLY, #rest_resp{code = ?HTTP_204_NO_CONTENT}).
-define(CREATED_REPLY(__PathTokens, __Body), #rest_resp{
    code = ?HTTP_201_CREATED,
    headers = #{<<"Location">> => list_to_binary(oneprovider:get_rest_endpoint(
        string:trim(filename:join([<<"/">> | __PathTokens]), leading, [$/])
    ))},
    body = __Body
}).

-endif.
