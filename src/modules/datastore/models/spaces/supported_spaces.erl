%%%-------------------------------------------------------------------
%%% @author Michal Stanisz
%%% @copyright (C) 2021 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Model that stores spaces IDs supported by this provider along with supporting storages.
%%% Based on information stored in this model provider can determine if space support was 
%%% ceased when it was offline or if space was deleted so space cleanup could be scheduled.
%%% @end
%%%-------------------------------------------------------------------
-module(supported_spaces).
-author("Michal Stanisz").

-include("modules/datastore/datastore_models.hrl").
-include("modules/datastore/datastore_runner.hrl").
-include_lib("ctool/include/logging.hrl").

%% API
-export([add/2, get_supports/0, remove/2]).

%% datastore_model callbacks
-export([get_ctx/0, get_record_version/0, get_record_struct/1]).

-type id() :: od_space:id().
-type record() :: #supported_spaces{}.
-type doc() :: datastore_doc:doc(record()).
-type diff() :: datastore_doc:diff(record()).

-export_type([id/0, record/0, doc/0, diff/0]).

-define(CTX, #{model => ?MODULE}).

-compile({no_auto_import, [get/0]}).

%%%===================================================================
%%% API
%%%===================================================================

-spec add(od_space:id(), storage:id()) -> ok | {error, term()}.
add(SpaceId, StorageId) ->
    UpdateFun = fun(#supported_spaces{supports = Supports} = Value) ->
        SpaceStorages = maps:get(SpaceId, Supports, []),
        {ok, Value#supported_spaces{supports = Supports#{
            SpaceId => lists:umerge([StorageId], SpaceStorages)
        }}}
    end,
    update(UpdateFun, #supported_spaces{supports = #{SpaceId => [StorageId]}}).

-spec get_supports() -> #{od_space:id() => [storage:id()]}.
get_supports() ->
    case get() of
        {ok, #document{value = #supported_spaces{supports = SpaceIds}}} ->
            SpaceIds;
        _ -> #{}
    end.

-spec remove(od_space:id(), storage:id()) -> ok | {error, term()}.
remove(SpaceId, StorageId) ->
    UpdateFun = fun(#supported_spaces{supports = Supports} = Value) ->
        SpaceStorages = maps:get(SpaceId, Supports, []),
        NewSpaceStorages = lists:delete(StorageId, SpaceStorages),
        NewSupports = case NewSpaceStorages of
            [] -> maps:without([SpaceId], Supports);
            _ -> Supports#{SpaceId => NewSpaceStorages}
        end,
        {ok, Value#supported_spaces{supports = NewSupports}}
    end,
    update(UpdateFun, #supported_spaces{supports = #{}}).
    

%%%===================================================================
%%% Internal functions
%%%===================================================================

-spec get() -> {ok, doc()} | {error, term()}.
get() ->
    datastore_model:get(?CTX, oneprovider:get_id()).

-spec update(datastore_model:diff(), #supported_spaces{}) -> ok | {error, term()}.
update(UpdateFun, Default) ->
    Id = oneprovider:get_id(),
    ?extract_ok(datastore_model:update(?CTX, Id, UpdateFun, #document{key = Id, value = Default})).

%%%===================================================================
%%% datastore_model callbacks
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Returns model's context.
%% @end
%%--------------------------------------------------------------------
-spec get_ctx() -> datastore:ctx().
get_ctx() ->
    ?CTX.

%%--------------------------------------------------------------------
%% @doc
%% Returns model's record version.
%% @end
%%--------------------------------------------------------------------
-spec get_record_version() -> datastore_model:record_version().
get_record_version() ->
    1.

%%--------------------------------------------------------------------
%% @doc
%% Returns model's record structure in provided version.
%% @end
%%--------------------------------------------------------------------
-spec get_record_struct(datastore_model:record_version()) ->
    datastore_model:record_struct().
get_record_struct(1) ->
    {record, [
        {space_ids, #{string => [string]}}
    ]}.
