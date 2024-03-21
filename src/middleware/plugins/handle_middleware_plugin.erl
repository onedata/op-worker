%%%-------------------------------------------------------------------
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2020-2021 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This module handles middleware operations (create, get, update, delete)
%%% corresponding to handles.
%%% @end
%%%-------------------------------------------------------------------
-module(handle_middleware_plugin).
-author("Bartosz Walkowicz").

-behaviour(middleware_router).
-behaviour(middleware_handler).

-include("middleware/middleware.hrl").
-include_lib("ctool/include/errors.hrl").
-include_lib("ctool/include/logging.hrl").
-include_lib("ctool/include/privileges.hrl").

%% middleware_router callbacks
-export([resolve_handler/3]).

%% middleware_handler callbacks
-export([data_spec/1, fetch_entity/1, authorize/2, validate/2]).
-export([create/1, get/2, update/1, delete/1]).


%%%===================================================================
%%% middleware_router callbacks
%%%===================================================================


%%--------------------------------------------------------------------
%% @doc
%% {@link middleware_router} callback resolve_handler/3.
%% @end
%%--------------------------------------------------------------------
-spec resolve_handler(middleware:operation(), gri:aspect(), middleware:scope()) ->
    module() | no_return().
resolve_handler(create, instance, private) -> ?MODULE;

resolve_handler(get, instance, private) -> ?MODULE;
resolve_handler(get, instance, public) -> ?MODULE;

resolve_handler(_, _, _) -> throw(?ERROR_NOT_SUPPORTED).


%%%===================================================================
%%% middleware_handler callbacks
%%%===================================================================


%%--------------------------------------------------------------------
%% @doc
%% {@link middleware_handler} callback data_spec/1.
%% @end
%%--------------------------------------------------------------------
-spec data_spec(middleware:req()) -> undefined | middleware_sanitizer:data_spec().
data_spec(#op_req{operation = create, gri = #gri{aspect = instance}}) -> #{
    required => #{
        <<"shareId">> => {binary, non_empty},
        <<"handleServiceId">> => {binary, non_empty},
        <<"metadataPrefix">> => {binary, non_empty}
    },
    optional => #{<<"metadataString">> => {binary, any}}
};

data_spec(#op_req{operation = get, gri = #gri{aspect = instance}}) ->
    undefined.


%%--------------------------------------------------------------------
%% @doc
%% {@link middleware_handler} callback fetch_entity/1.
%%
%% For now fetches only records for authorized users.
%% @end
%%--------------------------------------------------------------------
-spec fetch_entity(middleware:req()) ->
    {ok, middleware:versioned_entity()} | errors:error().
fetch_entity(#op_req{auth = Auth, gri = #gri{id = HandleId, scope = public}}) ->
    case handle_logic:get_public_data(Auth#auth.session_id, HandleId) of
        {ok, #document{value = Handle}} ->
            {ok, {Handle, 1}};
        {error, _} = Error ->
            Error
    end;

fetch_entity(#op_req{auth = ?NOBODY}) ->
    ?ERROR_UNAUTHORIZED;

fetch_entity(#op_req{auth = Auth, gri = #gri{id = HandleId, scope = private}}) ->
    case handle_logic:get(Auth#auth.session_id, HandleId) of
        {ok, #document{value = Handle}} ->
            {ok, {Handle, 1}};
        {error, _} = Error ->
            Error
    end.


%%--------------------------------------------------------------------
%% @doc
%% {@link middleware_handler} callback authorize/2.
%% @end
%%--------------------------------------------------------------------
-spec authorize(middleware:req(), middleware:entity()) -> boolean().
authorize(#op_req{auth = ?GUEST, operation = get, gri = #gri{aspect = instance, scope = public}}, _) ->
    true;

authorize(#op_req{auth = ?GUEST}, _) ->
    false;

authorize(#op_req{operation = create, gri = #gri{aspect = instance}}, _) ->
    % authorization will be checked by oz in during handle creation
    true;

authorize(#op_req{operation = get, gri = #gri{aspect = instance}}, _) ->
    % authorization was checked by oz in `fetch_entity`
    true.


%%--------------------------------------------------------------------
%% @doc
%% {@link middleware_handler} callback validate/2.
%% @end
%%--------------------------------------------------------------------
-spec validate(middleware:req(), middleware:entity()) -> ok | no_return().
validate(#op_req{operation = create, gri = #gri{aspect = instance}}, _) ->
    ok;
validate(#op_req{operation = get, gri = #gri{aspect = instance}}, _) ->
    % validation was checked by oz in `fetch_entity`
    ok.


%%--------------------------------------------------------------------
%% @doc
%% {@link middleware_handler} callback create/1.
%% @end
%%--------------------------------------------------------------------
-spec create(middleware:req()) -> middleware:create_result().
create(#op_req{auth = Auth, data = Data, gri = #gri{aspect = instance} = GRI}) ->
    SessionId = Auth#auth.session_id,

    ShareId = maps:get(<<"shareId">>, Data),
    HServiceId = maps:get(<<"handleServiceId">>, Data),
    MetadataPrefix = maps:get(<<"metadataPrefix">>, Data),
    Metadata = maps:get(<<"metadataString">>, Data, <<"">>),

    case handle_logic:create(SessionId, HServiceId, <<"Share">>, ShareId, MetadataPrefix, Metadata) of
        {ok, HandleId} ->
            {ok, #document{value = Handle}} = handle_logic:get(SessionId, HandleId),
            {ok, resource, {GRI#gri{id = HandleId}, Handle}};
        {error, _} = Error ->
            Error
    end.


%%--------------------------------------------------------------------
%% @doc
%% {@link middleware_handler} callback get/2.
%% @end
%%--------------------------------------------------------------------
-spec get(middleware:req(), middleware:entity()) -> middleware:get_result().
get(#op_req{gri = #gri{aspect = instance, scope = private}}, Handle) ->
    {ok, Handle};
get(#op_req{gri = #gri{aspect = instance, scope = public}}, #od_handle{
    public_handle = PublicHandle,
    metadata = Metadata
}) ->
    {ok, #{
        <<"url">> => utils:undefined_to_null(PublicHandle),
        <<"metadataString">> => utils:undefined_to_null(Metadata)
    }}.


%%--------------------------------------------------------------------
%% @doc
%% {@link middleware_handler} callback update/1.
%% @end
%%--------------------------------------------------------------------
-spec update(middleware:req()) -> middleware:update_result().
update(_) ->
    ?ERROR_NOT_SUPPORTED.


%%--------------------------------------------------------------------
%% @doc
%% {@link middleware_handler} callback delete/1.
%% @end
%%--------------------------------------------------------------------
-spec delete(middleware:req()) -> middleware:delete_result().
delete(_) ->
    ?ERROR_NOT_SUPPORTED.
