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
-export([create/7, get/1, modify_attrs/2, delete/1, mark_purging/2]).

% getters
-export([get_id/1, get_creation_time/1, get_dataset_id/1, get_root_dir/1, get_space_id/1,
    get_state/1, get_config/1, get_preserved_callback/1, get_purged_callback/1,
    get_description/1
]).

%% datastore_model callbacks
-export([get_ctx/0, get_record_struct/1]).

-compile([{no_auto_import, [get/1]}]).

-type id() :: binary().
-type record() :: #archive{}.
-type doc() :: datastore_doc:doc(record()).
-type diff() :: map().
%% Below is the description of diff that can be applied to modify archive record.
%% #{
%%     <<"description">> => description(),
%%     <<"preservedCallback">> => callback(),
%%     <<"purgedCallback">> => callback(),
%% }

-type creator() :: od_user:id().

-type type() :: archive_config:incremental().
-type include_dip() :: archive_config:include_dip().
-type layout() :: archive_config:layout().

-type state() :: ?ARCHIVE_PENDING | ?ARCHIVE_BUILDING | ?ARCHIVE_PRESERVED | ?ARCHIVE_PURGING.
-type timestamp() :: time:seconds().
-type description() :: binary().
-type callback() :: http_client:url() | undefined.

-type config() :: archive_config:config().

-type error() :: {error, term()}.

-export_type([
    id/0, doc/0,
    creator/0, type/0, state/0, include_dip/0,
    layout/0, timestamp/0, description/0,
    config/0, description/0, callback/0, diff/0
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

-spec create(dataset:id(), od_space:id(), creator(), config(), callback(), callback(), description()) ->
    {ok, doc()} | error().
create(DatasetId, SpaceId, Creator, Config, PreservedCallback, PurgedCallback, Description) ->
    datastore_model:create(?CTX, #document{
        value = #archive{
            dataset_id = DatasetId,
            creation_time = global_clock:timestamp_seconds(),
            creator = Creator,
            state = ?ARCHIVE_PENDING,
            config = Config,
            preserved_callback = PreservedCallback,
            purged_callback = PurgedCallback,
            description = Description
        },
        scope = SpaceId
    }).


-spec get(id()) -> {ok, doc()} | error().
get(ArchiveId) ->
    datastore_model:get(?CTX, ArchiveId).


-spec modify_attrs(id(), diff()) -> ok | error().
modify_attrs(ArchiveId, Diff) when is_map(Diff) ->
    ?extract_ok(update(ArchiveId, fun(Archive = #archive{
        description = PrevDescription,
        preserved_callback = PrevPreservedCallback,
        purged_callback = PrevPurgedCallback
    }) ->
        {ok, Archive#archive{
            description = utils:ensure_defined(maps:get(<<"description">>, Diff, undefined), PrevDescription),
            preserved_callback = utils:ensure_defined(maps:get(<<"preservedCallback">>, Diff, undefined), PrevPreservedCallback),
            purged_callback = utils:ensure_defined(maps:get(<<"purgedCallback">>, Diff, undefined), PrevPurgedCallback)
        }}
    end)).


-spec delete(archive:id()) -> ok | error().
delete(ArchiveId) ->
    datastore_model:delete(?CTX, ArchiveId).


-spec mark_purging(id(), callback()) -> {ok, doc()} | error().
mark_purging(ArchiveId, Callback) ->
    update(ArchiveId, fun(Archive = #archive{purged_callback = PrevPurgedCallback}) ->
        {ok, Archive#archive{
            state = ?ARCHIVE_PURGING,
            purged_callback = utils:ensure_defined(Callback, PrevPurgedCallback)
        }} 
    end).


%%%===================================================================
%%% Getters for #archive record
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

-spec get_config(record() | doc()) -> config().
get_config(#archive{config = Config}) ->
    Config;
get_config(#document{value = Archive}) ->
    get_config(Archive).

-spec get_preserved_callback(record() | doc()) -> callback().
get_preserved_callback(#archive{preserved_callback = PreservedCallback}) ->
    PreservedCallback;
get_preserved_callback(#document{value = Archive}) ->
    get_preserved_callback(Archive).

-spec get_purged_callback(record() | doc()) -> callback().
get_purged_callback(#archive{purged_callback = PurgedCallback}) ->
    PurgedCallback;
get_purged_callback(#document{value = Archive}) ->
    get_purged_callback(Archive).

-spec get_description(record() | doc()) -> description().
get_description(#archive{description = Description}) ->
    Description;
get_description(#document{value = Archive}) ->
    get_description(Archive).

%%%===================================================================
%%% Internal functions
%%%===================================================================

-spec update(id(), datastore_doc:diff(record())) -> {ok, doc()} | error().
update(ArchiveId, Diff) when is_function(Diff)->
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
        {config, {record, [
            {incremental, boolean},
            {dip, boolean},
            {layout, atom}
        ]}},
        {preserved_callback, string},
        {purged_callback, string},
        {description, binary}
    ]}.
