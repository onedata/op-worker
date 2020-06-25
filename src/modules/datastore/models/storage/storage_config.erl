%%%-------------------------------------------------------------------
%%% @author Rafal Slota
%%% @copyright (C) 2015 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Model for holding storage configuration. It contains provider specific
%%% information and private storage details. It should not be shared with
%%% other providers. To share storage details with other providers(through Onezone)
%%% module `storage_logic` and `od_storage` model are used.
%%%
%%% Module `storage` is an overlay to this module and `storage_logic`.
%%%
%%% NOTE: Functions from this module should not be called directly.
%%% Use module `storage` instead.
%%% @end
%%%-------------------------------------------------------------------
-module(storage_config).
-author("Rafal Slota").

-include("modules/datastore/datastore_models.hrl").
-include("modules/datastore/datastore_runner.hrl").
-include("modules/storage/helpers/helpers.hrl").
-include("modules/fslogic/fslogic_common.hrl").
-include_lib("ctool/include/errors.hrl").

%% API
-export([create/2, create/3, get/1, exists/1, delete/1]).
-export([get_id/1, get_helper/1,
    get_luma_feed/1, get_luma_config/1,
    should_skip_storage_detection/1, is_imported_storage/1]).

-export([update_helper/2, update_luma_config/2, set_luma_config/2]).

-export([delete_all/0]).

%% function for migrating old #storage{} record
-export([migrate_to_storage_config_v1/1]).

%% datastore_model callbacks
-export([get_ctx/0]).
-export([get_record_version/0, get_record_struct/1, upgrade_record/2]).

-type record() :: #storage_config{}.
-type doc() :: datastore_doc:doc(record()).
-type diff() :: datastore_doc:diff(record()).
-type name() :: binary().

-export_type([record/0, doc/0, name/0]).

-define(CTX, #{
    model => ?MODULE,
    % fold enabled to allow for listing and deleting local
    % documents after provider was deregistered
    fold_enabled => true,
    memory_copies => all
}).

-compile({no_auto_import, [get/1]}).

%%%===================================================================
%%% API
%%%===================================================================

-spec create(storage:id(), record()) -> {ok, storage:id()} | {error, term()}.
create(StorageId, StorageConfig) ->
    ?extract_key(datastore_model:create(?CTX, #document{
        key = StorageId,
        value = StorageConfig
    })).

-spec create(storage:id(), helpers:helper(), undefined | storage:luma_config()) ->
    {ok, storage:id()} | {error, term()}.
create(StorageId, Helper, LumaConfig) ->
    create(StorageId, #storage_config{
        helper = Helper,
        luma_config = utils:ensure_defined(LumaConfig, luma_config:new(?AUTO_FEED))
    }).


-spec get(storage:id()) -> {ok, doc()} | {error, term()}.
get(Key) ->
    datastore_model:get(?CTX, Key).


%% @private
-spec update(storage:id(), diff()) -> {ok, doc()} | {error, term()}.
update(Key, Diff) ->
    datastore_model:update(?CTX, Key, Diff).


-spec exists(storage:id()) -> boolean().
exists(Key) ->
    {ok, Exists} = datastore_model:exists(?CTX, Key),
    Exists.


-spec delete(storage:id()) -> ok | {error, term()}.
delete(StorageId) ->
    datastore_model:delete(?CTX, StorageId).

%%%===================================================================
%%% API functions
%%%===================================================================

-spec get_id(doc()) -> storage:id().
get_id(#document{key = StorageId, value = #storage_config{}}) ->
    StorageId.


-spec get_helper(doc() | record() | storage:id()) -> helpers:helper().
get_helper(#document{value = StorageConfig}) ->
    get_helper(StorageConfig);
get_helper(#storage_config{helper = Helper}) ->
    Helper;
get_helper(StorageId) ->
    {ok, StorageDoc} = storage_config:get(StorageId),
    get_helper(StorageDoc).

-spec get_luma_feed(storage:id() | doc() | record()) -> storage:luma_feed().
get_luma_feed(Storage) ->
    LumaConfig = get_luma_config(Storage),
    luma_config:get_feed(LumaConfig).

-spec get_luma_config(storage:id() | doc() | record()) -> storage:luma_config().
get_luma_config(#document{value = Storage = #storage_config{}}) ->
    get_luma_config(Storage);
get_luma_config(#storage_config{luma_config = LumaConfig}) ->
    LumaConfig;
get_luma_config(StorageId) ->
    {ok, StorageDoc} = get(StorageId),
    get_luma_config(StorageDoc).

-spec should_skip_storage_detection(doc() | record() | storage:id()) -> boolean().
should_skip_storage_detection(Storage) ->
    Helper = get_helper(Storage),
    helper:should_skip_storage_detection(Helper).

%% @TODO VFS-5856 deprecated, included for upgrade procedure
-spec is_imported_storage(doc() | record() | storage:id()) -> boolean().
is_imported_storage(#document{value = #storage_config{} = Value}) ->
    is_imported_storage(Value);
is_imported_storage(#storage_config{imported_storage = ImportedStorage}) ->
    ImportedStorage;
is_imported_storage(StorageId) ->
    {ok, #document{value = Value}} = get(StorageId),
    is_imported_storage(Value).


-spec update_helper(storage:id(), fun((helpers:helper()) -> helpers:helper())) ->
    ok | {error, term()}.
update_helper(StorageId, UpdateFun) ->
    ?extract_ok(update(StorageId, fun
        (#storage_config{helper = PreviousHelper} = StorageConfig) ->
            case UpdateFun(PreviousHelper) of
                {ok, PreviousHelper} ->
                    % this error informs higher level module, that no changes were made
                    % and there is no need to execute `on_helper_changed` callback
                    {error, no_changes};
                {ok, NewHelper} ->
                    {ok, StorageConfig#storage_config{helper = NewHelper}};
                {error, _} = Error ->
                    Error
            end
    end)).


%%--------------------------------------------------------------------
%% @doc
%% Updates LUMA configuration of the storage.
%% LUMA cannot be enabled or disabled, only its parameters may be changed.
%% @end
%%--------------------------------------------------------------------
-spec update_luma_config(storage:id(), UpdateFun) -> ok | {error, term()}
    when UpdateFun :: fun((storage:luma_config()) -> {ok, storage:luma_config()} | {error, term()}).
update_luma_config(StorageId, UpdateFun) ->
    ?extract_ok(update(StorageId, fun
        (#storage_config{luma_config = PreviousLumaConfig} = StorageConfig) ->
            case UpdateFun(PreviousLumaConfig) of
                {ok, NewLumaConfig} ->
                    {ok, StorageConfig#storage_config{luma_config = NewLumaConfig}};
                {error, _} = Error ->
                    Error
            end
    end)).


-spec set_luma_config(storage:id(), storage:luma_config()) -> ok.
set_luma_config(StorageId, LumaConfig) ->
    ?extract_ok(update(StorageId, fun(#storage_config{} = Storage) ->
        {ok, Storage#storage_config{luma_config = LumaConfig}}
    end)).


-spec delete_all() -> ok.
delete_all() ->
    {ok, StorageList} = datastore_model:fold(?CTX, fun(Doc, Acc) -> {ok, [Doc | Acc]} end, []),
    lists:foreach(fun(#document{key = StorageId}) ->
        delete(StorageId)
    end, StorageList).

%%%===================================================================
%%% TODO
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% This function upgrades old `storage` record to `storage_config`
%% in version 1.
%% @end
%%--------------------------------------------------------------------
-spec migrate_to_storage_config_v1(#storage{}) -> tuple().
migrate_to_storage_config_v1(#storage{
    helpers = [Helper],
    readonly = Readonly,
    luma_config = LumaConfig
}) ->
    {?MODULE, Helper, Readonly, LumaConfig, false}.

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
    2.


%%--------------------------------------------------------------------
%% @doc
%% Returns model's record structure in provided version.
%% @end
%%--------------------------------------------------------------------
-spec get_record_struct(datastore_model:record_version()) ->
    datastore_model:record_struct().
get_record_struct(1) ->
    {record, [
        {helper, {record, [
            {name, string},
            {args, #{string => string}},
            {admin_ctx, #{string => string}},
            {insecure, boolean},
            {extended_direct_io, boolean},
            {storage_path_type, string}
        ]}},
        {readonly, boolean},
        {luma_config, {record, [
            {url, string},
            {api_key, string}
        ]}},
        {imported_storage, boolean} % @TODO 5856 deprecated remove in next major version
    ]};
get_record_struct(2) ->
    {record, [
        {helper, {record, [
            {name, string},
            {args, #{string => string}},
            {admin_ctx, #{string => string}}
            % deleted insecure field
            % deleted extended_direct_io field
            % storage_path_type{ field renamed to storagePathType and moved to args
        ]}},
        % field readonly was rename to skipStorageDetection and moved to helper args
        {luma_config, {record, [
            % added field feed in luma_config record
            {feed, atom},
            {url, string},
            {api_key, string}
        ]}},
        {imported_storage, boolean} % @TODO 5856 deprecated remove in next major version
    ]}.

%%--------------------------------------------------------------------
%% @doc
%% Upgrades model's record from provided version to the next one.
%% @end
%%--------------------------------------------------------------------
-spec upgrade_record(datastore_model:record_version(), datastore_model:record()) ->
    {datastore_model:record_version(), datastore_model:record()}.
upgrade_record(1, {?MODULE, Helper, Readonly, LumaConfig, ImportedStorage}) ->
    LumaConfig2 = case LumaConfig =:= undefined of
        true -> luma_config:new(?AUTO_FEED);
        false -> luma_config:set_feed(LumaConfig, ?EXTERNAL_FEED)
    end,
    {helper,
        HelperName,
        Args,
        AdminCtx,
        _Insecure,
        _ExtendedDirectIO,
        StoragePathType
    } = Helper,
    NewArgs = Args#{
        % readonly field was used to notify that detecting storage should be skipped
        % it was renamed and moved to helper args
        <<"skipStorageDetection">> => atom_to_binary(Readonly, utf8),
        % storagePathType field was moved to helper args
        <<"storagePathType">> => StoragePathType
    },
    {2, {?MODULE,
        {helper, HelperName, NewArgs, AdminCtx},
        LumaConfig2,
        ImportedStorage
    }}.