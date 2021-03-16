%%%-------------------------------------------------------------------
%%% @author Jakub Kudzia
%%% @copyright (C) 2021 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% WRITEME
%%% @end
%%%-------------------------------------------------------------------
-module(dataset).
-author("Jakub Kudzia").

-include("global_definitions.hrl").
-include("modules/dataset/dataset.hrl").
-include("modules/datastore/datastore_models.hrl").
-include("modules/datastore/datastore_runner.hrl").

%% API
-export([create/2, delete/1]).
-export([get_uuid/1, get_space_id/1, get_state/1, get_detached_info/1, get/1]).
-export([mark_detached/3, mark_reattached/1]).

%% datastore_model callbacks
-export([get_ctx/0, get_record_struct/1]).

-compile([{no_auto_import, [get/1]}]).

-type id() :: datastore:key().
-type state() :: ?ATTACHED_DATASET | ?DETACHED_DATASET.
-type diff() :: datastore_doc:diff(record()).
-type record() :: #dataset{}.
-type doc() :: datastore_doc:doc(record()).
-type path() :: file_meta:uuid_based_path().
-type error() :: {error, term()}.
-type detached_info() :: detached_dataset_info:info().

-export_type([id/0, state/0, path/0, detached_info/0]).


% @formatter:on
-define(CTX, #{
    model => ?MODULE,
    sync_enabled => true,
    remote_driver => datastore_remote_driver,
    mutator => oneprovider:get_id_or_undefined(),
    local_links_tree_id => oneprovider:get_id_or_undefined()
}).
% @formatter:off


%%%===================================================================
%%% API functions
%%%===================================================================

-spec create(file_meta:uuid(), od_space:id()) -> {ok, id()} | error().
create(FileUuid, SpaceId) ->
  ?extract_key(datastore_model:create(?CTX, #document{
      value = #dataset{
          uuid = FileUuid,
          creation_time = global_clock:timestamp_seconds(),
          state = ?ATTACHED_DATASET
      },
      scope = SpaceId
  })).


-spec delete(id()) -> ok | error().
delete(DatasetId) ->
    datastore_model:delete(?CTX, DatasetId).


-spec get_space_id(doc()) -> {ok, od_space:id()}.
get_space_id(#document{scope = SpaceId}) ->
    {ok, SpaceId};
get_space_id(DatasetId) ->
    case get(DatasetId) of
        {ok, Doc} -> get_space_id(Doc);
        {error, _} = Error -> Error
    end.


-spec get_uuid(id() | doc()) -> {ok, file_meta:uuid()} | error().
get_uuid(#document{value = #dataset{uuid = Uuid}}) ->
    {ok, Uuid};
get_uuid(DatasetId) ->
    case get(DatasetId) of
        {ok, Doc} -> get_uuid(Doc);
        {error, _} = Error -> Error
    end.


-spec get_state(id() | doc()) -> {ok, state()} | error().
get_state(#document{value = #dataset{state = State}}) ->
    {ok, State};
get_state(DatasetId) ->
    case get(DatasetId) of
        {ok, Doc} -> get_state(Doc);
        {error, _} = Error -> Error
    end.


-spec get_creation_time(id() | doc()) -> {ok, time:seconds()} | error().
get_creation_time(#document{value = #dataset{creation_time = CreationTime}}) ->
    {ok, CreationTime};
get_creation_time(DatasetId) ->
    case get(DatasetId) of
        {ok, Doc} -> get_creation_time(Doc);
        {error, _} = Error -> Error
    end.


-spec get_detached_info(id() | doc()) -> {ok, detached_info()} | error().
get_detached_info(#document{value = #dataset{detached_info = Info}}) ->
    {ok, Info};
get_detached_info(DatasetId) ->
    case get(DatasetId) of
        {ok, Doc} -> get_detached_info(Doc);
        {error, _} = Error -> Error
    end.


-spec get(id()) -> {ok, doc()} | error().
get(DatasetId) ->
    datastore_model:get(?CTX, DatasetId).


-spec mark_detached(id(), path(), file_meta:path()) -> ok | error().
mark_detached(DatasetId, DatasetPath, FileRootPath) ->
    update(DatasetId, fun(Dataset) ->
        {ok, Dataset#dataset{
            state = ?DETACHED_DATASET,
            detached_info = detached_dataset_info:create_info(DatasetPath, FileRootPath, FileRootType)
        }}
    end).


-spec mark_reattached(id()) -> ok | {error, term()}.
mark_reattached(DatasetId) ->
    update(DatasetId, fun(Dataset) ->
        {ok, Dataset#dataset{
            state = ?ATTACHED_DATASET,
            detached_info = undefined
        }}
    end).

%%%===================================================================
%%% Internal functions
%%%===================================================================

-spec update(id(), diff()) -> ok | error().
update(DatasetId, Diff) ->
    ?extract_ok(datastore_model:update(?CTX, DatasetId, Diff)).

%%%===================================================================
%%% Datastore callbacks
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
%% Returns model's record structure in provided version.
%% @end
%%--------------------------------------------------------------------
-spec get_record_struct(datastore_model:record_version()) -> datastore_model:record_struct().
get_record_struct(1) ->
    {record, [
        {uuid, string},
        {creation_time, integer},
        {state, atom},
        {detached_info, {record, [
            {dataset_path, string},
            {file_root_path, string},
            {file_root_type, atom}
        ]}}
    ]}.
