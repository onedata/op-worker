%%%-------------------------------------------------------------------
%%% @author Jakub Kudzia
%%% @copyright (C) 2021 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Model for storing information about archives.
%%% @end
%%%-------------------------------------------------------------------
-module(archive).
-author("Jakub Kudzia").

-include("modules/archive/archive.hrl").
-include("modules/datastore/datastore_models.hrl").
-include("modules/datastore/datastore_runner.hrl").

%% API
-export([create/5, get/1, mark_purging/1, modify_attrs/2, delete/1]).

% getters
-export([get_id/1, get_creation_time/1, get_dataset_id/1, get_root_dir/1, get_space_id/1,
    get_state/1, get_params/1, get_attrs/1
]).

%% datastore_model callbacks
-export([get_ctx/0, get_record_struct/1]).

-compile([{no_auto_import, [get/1]}]).

-type id() :: binary().
-type record() :: #archive{}.
-type doc() :: datastore_doc:doc(record()).
-type diff() :: datastore_doc:diff(record()).

-type creator() :: od_user:id().

-type type() :: archive_params:type().
-type dip() :: archive_params:dip().
-type data_structure() :: archive_params:data_structure().
-type metadata_structure() :: archive_params:metadata_structure().

-type state() :: ?EMPTY | ?INITIALIZING | ?PERSISTED | ?PURGING.
-type timestamp() :: time:seconds().
-type description() :: archive_attrs:description() | undefined.

-type params() :: archive_params:params().
-type attrs() :: archive_attrs:attrs().

-type error() :: {error, term()}.

-export_type([
    id/0, doc/0,
    creator/0, type/0, state/0, dip/0,
    data_structure/0, metadata_structure/0,
    timestamp/0, description/0, params/0, attrs/0
]).

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

-spec create(dataset:id(), od_space:id(), creator(), params(), attrs()) ->
    {ok, doc()} | error().
create(DatasetId, SpaceId, Creator, Params, Attrs) ->
    datastore_model:create(?CTX, #document{
        value = #archive{
            dataset_id = DatasetId,
            creation_time = global_clock:timestamp_seconds(),
            creator = Creator,
            state = ?EMPTY,
            params = Params,
            attrs = Attrs
        },
        scope = SpaceId
    }).


-spec mark_purging(id()) -> ok | error().
mark_purging(ArchiveId) ->
    ?extract_ok(update(ArchiveId, fun(Archive) -> {ok, Archive#archive{state = ?PURGING}} end)).


-spec modify_attrs(id(), attrs()) -> ok | error().
modify_attrs(ArchiveId, Attrs) ->
    ?extract_ok(update(ArchiveId, fun(Archive = #archive{attrs = CurrentAttrs}) ->
        {ok, Archive#archive{
            attrs = archive_attrs:update(CurrentAttrs, Attrs)
        }}
    end)).


-spec get(id()) -> {ok, doc()} | error().
get(ArchiveId) ->
    datastore_model:get(?CTX, ArchiveId).


-spec delete(archive:id()) -> ok | error().
delete(ArchiveId) ->
    datastore_model:delete(?CTX, ArchiveId).

%%%===================================================================
%%% Getters/setters for #archive record
%%%===================================================================

-spec get_id(doc()) -> id().
get_id(#document{key = ArchiveId}) ->
    ArchiveId.

-spec get_creation_time(record() | doc()) -> timestamp().
get_creation_time(#archive{creation_time = CreationTime}) ->
    CreationTime;
get_creation_time(#document{value = Archive}) ->
    get_creation_time(Archive).

-spec get_dataset_id(record() | doc()) -> dataset:id().
get_dataset_id(#archive{dataset_id = DatasetId}) ->
    DatasetId;
get_dataset_id(#document{value = Archive}) ->
    get_dataset_id(Archive).

-spec get_root_dir(record() | doc()) -> file_id:file_guid() | undefined.
get_root_dir(#archive{root_dir_guid = RootDir}) ->
    RootDir;
get_root_dir(#document{value = Archive}) ->
    get_root_dir(Archive).

-spec get_space_id(doc()) -> od_space:id().
get_space_id(#document{scope = SpaceId}) ->
    SpaceId.

-spec get_state(record() | doc()) -> state().
get_state(#archive{state = State}) ->
    State;
get_state(#document{value = Archive}) ->
    get_state(Archive).

-spec get_params(record() | doc()) -> params().
get_params(#archive{params = Params}) ->
    Params;
get_params(#document{value = Archive}) ->
    get_params(Archive).

-spec get_attrs(record() | doc()) -> attrs().
get_attrs(#archive{attrs = Attrs}) ->
    Attrs;
get_attrs(#document{value = Archive}) ->
    get_attrs(Archive).


%%%===================================================================
%%% Internal functions
%%%===================================================================

-spec update(id(), diff()) -> {ok, doc()} | error().
update(ArchiveId, Diff) ->
    datastore_model:update(?CTX, ArchiveId, Diff).

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
        {dataset_id, string},
        {root_dir_guid, string},
        {creation_time, integer},
        {creator, string},
        {state, atom},
        {params, {record, [
            {type, atom},
            {dip, boolean},
            {data_structure, atom},
            {metadata_structure, atom},
            {callback, string}
        ]}},
        {attrs, {record, [
            {description, binary}
        ]}}
    ]}.
