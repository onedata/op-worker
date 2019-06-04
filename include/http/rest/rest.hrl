%%%-------------------------------------------------------------------
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
    method = get :: rest_handler:method(),
    parse_body = ignore :: rest_handler:parse_body(),
    consumes = ['*'] :: ['*'] | [binary()],
    produces = ['*'] :: ['*'] | [binary()],
    b_gri :: rest_handler:bound_gri()
}).

%% Record representing REST response.
-record(rest_resp, {
    code = 200 :: integer(),
    headers = #{} :: #{binary() => binary()},
    body = {binary, <<"">>} :: json_utils:json_term() | {binary, binary()}
}).

% Convenience macros used in rest_req, they will be processed before passed
% further to internal logic.
-define(BINDING(__KEY), {binding, __KEY}).
-define(OBJECTID_BINDING(__KEY), {objectid_binding, __KEY}).
-define(PATH_BINDING, path_binding).

% Defines with HTTP codes
-define(HTTP_200_OK, 200).
-define(HTTP_201_CREATED, 201).
-define(HTTP_202_ACCEPTED, 202).
-define(HTTP_204_NO_CONTENT, 204).
-define(HTTP_206_PARTIAL_CONTENT, 206).

-define(HTTP_301_MOVED_PERMANENTLY, 301).

-define(HTTP_400_BAD_REQUEST, 400).
-define(HTTP_401_UNAUTHORIZED, 401).
-define(HTTP_403_FORBIDDEN, 403).
-define(HTTP_404_NOT_FOUND, 404).
-define(HTTP_409_CONFLICT, 409).
-define(HTTP_415_UNSUPPORTED_MEDIA_TYPE, 415).
-define(HTTP_426_UPGRADE_REQUIRED, 426).

-define(HTTP_500_INTERNAL_SERVER_ERROR, 500).
-define(HTTP_501_NOT_IMPLEMENTED, 501).

-endif.
