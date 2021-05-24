%%%-------------------------------------------------------------------
%%% @author Jakub Kudzia
%%% @copyright (C) 2021 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Model for storing information about datasets.
%%% Id of a dataset is a file_meta:uuid() of file on which dataset is
%%% established.
%%% @end
%%%-------------------------------------------------------------------
-module(dataset).
-author("Jakub Kudzia").

-include("global_definitions.hrl").
-include("modules/dataset/dataset.hrl").
-include("modules/datastore/datastore_models.hrl").
-include("modules/datastore/datastore_runner.hrl").
-include_lib("ctool/include/errors.hrl").


%% API
-export([create/2, delete/1]).
-export([
    get_id/1, get_root_file_uuid/1,
    get_space_id/1, get_state/1,
    get_detached_info/1, get_creation_time/1,
    get/1
]).
-export([mark_detached/5, mark_reattached/1]).

%% datastore_model callbacks
-export([get_ctx/0, get_record_struct/1]).

-compile([{no_auto_import, [get/1]}]).

-type id() :: file_meta:uuid().
-type state() :: ?ATTACHED_DATASET | ?DETACHED_DATASET.
-type diff() :: datastore_doc:diff(record()).
-type record() :: #dataset{}.
-type doc() :: datastore_doc:doc(record()).
-type path() :: file_meta:uuid_based_path().
-type name() :: file_meta:name().
-type error() :: {error, term()}.
-type detached_info() :: detached_dataset_info:info().
-type membership() :: ?NONE_DATASET_MEMBERSHIP | ?DIRECT_DATASET_MEMBERSHIP | ?ANCESTOR_DATASET_MEMBERSHIP.

-export_type([id/0, doc/0, name/0, state/0, path/0, detached_info/0, membership/0]).


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
      key = FileUuid,
      value = #dataset{
          creation_time = global_clock:timestamp_seconds(),
          state = ?ATTACHED_DATASET
      },
      scope = SpaceId
  })).


-spec delete(id()) -> ok | error().
delete(DatasetId) ->
    datastore_model:delete(?CTX, DatasetId).


-spec get_id(doc()) -> {ok, id()}.
get_id(#document{key = DatasetId}) ->
    {ok, DatasetId}.


%%--------------------------------------------------------------------
%% @doc
%% Returns uuid of dataset root file.
%% NOTE !!!
%% DatasetId is uuid of a root file.
%% @end
%%--------------------------------------------------------------------
-spec get_root_file_uuid(doc() | id()) -> {ok, file_meta:uuid()}.
get_root_file_uuid(#document{} = DatasetDoc) ->
    get_id(DatasetDoc).


-spec get_space_id(doc() | dataset:id()) -> {ok, od_space:id()}.
get_space_id(#document{scope = SpaceId}) ->
    {ok, SpaceId};
get_space_id(DatasetId) ->
    ?get_field(DatasetId, fun get_space_id/1).


-spec get_state(id() | doc()) -> {ok, state()} | error().
get_state(#document{value = #dataset{state = State}}) ->
    {ok, State};
get_state(DatasetId) ->
    ?get_field(DatasetId, fun get_state/1).


-spec get_creation_time(id() | doc()) -> {ok, time:seconds()} | error().
get_creation_time(#document{value = #dataset{creation_time = CreationTime}}) ->
    {ok, CreationTime};
get_creation_time(DatasetId) ->
    ?get_field(DatasetId, fun get_creation_time/1).


-spec get_detached_info(id() | doc()) -> {ok, detached_info()} | error().
get_detached_info(#document{value = #dataset{detached_info = Info}}) ->
    {ok, Info};
get_detached_info(DatasetId) ->
    ?get_field(DatasetId, fun get_detached_info/1).


-spec get(id()) -> {ok, doc()} | error().
get(DatasetId) ->
    datastore_model:get(?CTX, DatasetId).


-spec mark_detached(id(), path(), file_meta:path(), file_meta:type(), data_access_control:bitmask()) -> ok | error().
mark_detached(DatasetId, DatasetPath, RootFilePath, RootFileType, ProtectionFlags) ->
    update(DatasetId, fun
        (Dataset = #dataset{state = ?ATTACHED_DATASET}) ->
            {ok, Dataset#dataset{
                state = ?DETACHED_DATASET,
                detached_info = detached_dataset_info:create_info(DatasetPath, RootFilePath, RootFileType, ProtectionFlags)
            }};
        (#dataset{state = ?DETACHED_DATASET}) ->
            ?ERROR_ALREADY_EXISTS
    end).


-spec mark_reattached(id()) -> ok | error().
mark_reattached(DatasetId) ->
    update(DatasetId, fun
        (Dataset = #dataset{state = ?DETACHED_DATASET}) ->
            {ok, Dataset#dataset{
                state = ?ATTACHED_DATASET,
                detached_info = undefined
            }};
        (#dataset{state = ?ATTACHED_DATASET}) ->
            ?ERROR_ALREADY_EXISTS
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

-spec get_ctx() -> datastore:ctx().
get_ctx() ->
    ?CTX.

-spec get_record_struct(datastore_model:record_version()) -> datastore_model:record_struct().
get_record_struct(1) ->
    {record, [
        {creation_time, integer},
        {state, atom},
        {detached_info, {record, [
            {dataset_path, string},
            {root_file_path, string},
            {root_file_type, atom},
            {protection_flags, integer}
        ]}}
    ]}.