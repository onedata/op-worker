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

resolve_handler(get, instance, public) -> ?MODULE;

resolve_handler(update, instance, private) -> ?MODULE;

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
        <<"metadataPrefix">> => {binary, non_empty},
        <<"metadataString">> => {binary, non_empty}
    }
};

data_spec(#op_req{operation = get, gri = #gri{aspect = instance}}) ->
    undefined;

data_spec(#op_req{operation = update, gri = #gri{aspect = instance}}) -> #{
    required => #{
        <<"metadataString">> => {binary, any}
    }
}.


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

fetch_entity(#op_req{operation = update, gri = #gri{scope = private}}) ->
    % authorization will be checked by oz in during handle update
    {ok, {undefined, 1}}.


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
    true;

authorize(#op_req{operation = update, gri = #gri{aspect = instance}}, _) ->
    % authorization will be checked by oz in during handle update
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
    ok;
validate(#op_req{operation = update, gri = #gri{aspect = instance}}, _) ->
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
    Metadata = maps:get(<<"metadataString">>, Data),

    case handle_logic:create(SessionId, HServiceId, <<"Share">>, ShareId, MetadataPrefix, Metadata) of
        {ok, HandleId} ->
            {ok, #document{value = Handle}} = handle_logic:get_public_data(SessionId, HandleId),
            {ok, resource, {GRI#gri{id = HandleId, scope = public}, record_to_data(Handle)}};
        {error, _} = Error ->
            Error
    end.


%%--------------------------------------------------------------------
%% @doc
%% {@link middleware_handler} callback get/2.
%% @end
%%--------------------------------------------------------------------
-spec get(middleware:req(), middleware:entity()) -> middleware:get_result().
get(#op_req{gri = #gri{aspect = instance, scope = public}}, Handle) ->
    {ok, record_to_data(Handle)}.


%%--------------------------------------------------------------------
%% @doc
%% {@link middleware_handler} callback update/1.
%% @end
%%--------------------------------------------------------------------
-spec update(middleware:req()) -> middleware:update_result().
update(#op_req{auth = #auth{session_id = SessionId}, data = Data, gri = #gri{id = HandleId, aspect = instance}}) ->
    Metadata = maps:get(<<"metadataString">>, Data, <<"">>),
    handle_logic:update(SessionId, HandleId, Metadata).


%%--------------------------------------------------------------------
%% @doc
%% {@link middleware_handler} callback delete/1.
%% @end
%%--------------------------------------------------------------------
-spec delete(middleware:req()) -> middleware:delete_result().
delete(_) ->
    ?ERROR_NOT_SUPPORTED.


%% @private
-spec record_to_data(od_handle:record()) -> middleware:data().
record_to_data(#od_handle{
    public_handle = PublicHandle,
    metadata_prefix = MetadataPrefix,
    metadata = Metadata,
    handle_service = HandleServiceId
}) ->
    #{
        <<"handleServiceId">> => HandleServiceId,
        <<"url">> => utils:undefined_to_null(PublicHandle),
        <<"metadataPrefix">> => MetadataPrefix,
        <<"metadataString">> => utils:undefined_to_null(Metadata)
    }.