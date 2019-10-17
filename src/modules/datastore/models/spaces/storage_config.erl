%%%-------------------------------------------------------------------
%%% @author Rafal Slota
%%% @copyright (C) 2015 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc Model for holding storage configuration.
%%%      @todo: rewrite without "ROOT_STORAGE" when implementation of persistent_store:list will be ready
%%% @end
%%%-------------------------------------------------------------------
-module(storage_config).
-author("Rafal Slota").

-include("modules/datastore/datastore_models.hrl").
-include("modules/datastore/datastore_runner.hrl").
-include("modules/storage_file_manager/helpers/helpers.hrl").
-include_lib("ctool/include/errors.hrl").

%% API
-export([new/2, new/4]).
-export([get_id/1, get_name/1, is_readonly/1, is_mounted_in_root/1,
    get_helpers/1, get_luma_config_map/1]).
-export([select_helper/2, select/1]).
-export([get/1, exists/1, delete/1, update/2, save_doc/1, list/0]).
-export([supports_any_space/1]).
-export([on_storage_created/1]).
-export([delete_all/0]).

%% Exports for onepanel RPC
-export([update_name/2, update_helper_args/3, update_admin_ctx/3,
    update_luma_config/2, set_luma_config/2, set_insecure/3,
    set_readonly/2, set_mount_in_root/1, safe_remove/1, describe/1]).
-export([get_luma_config/1, is_luma_enabled/1]).

%% datastore_model callbacks
-export([get_ctx/0]).
-export([get_record_version/0, get_record_struct/1, upgrade_record/2]).

-type record() :: #storage_config{}.
-type doc() :: datastore_doc:doc(record()).
-type diff() :: datastore_doc:diff(record()).
-type name() :: binary().
-type helper() :: helpers:helper().

-export_type([record/0, doc/0, name/0, helper/0]).

-define(CTX, #{
    model => ?MODULE,
    fold_enabled => true,
    memory_copies => all
}).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Updates storage.
%% @end
%%--------------------------------------------------------------------
-spec update(od_storage:id(), diff()) -> {ok, doc()} | {error, term()}.
update(Key, Diff) ->
    datastore_model:update(?CTX, Key, Diff).

%%--------------------------------------------------------------------
%% @doc
%% Creates storage.
%% @end
%%--------------------------------------------------------------------
-spec save_doc(doc()) -> {ok, od_storage:id()} | {error, term()}.
save_doc(#document{value = #storage_config{}} = Doc) ->
    ?extract_key(datastore_model:create(?CTX, Doc)).

%%--------------------------------------------------------------------
%% @doc
%% Returns storage.
%% @end
%%--------------------------------------------------------------------
-spec get(od_storage:id()) -> {ok, doc()} | {error, term()}.
get(Key) ->
    datastore_model:get(?CTX, Key).

%%--------------------------------------------------------------------
%% @doc
%% Deletes storage.
%% @end
%%--------------------------------------------------------------------
-spec delete(od_storage:id()) -> ok | {error, term()}.
delete(StorageId) ->
    datastore_model:delete(?CTX, StorageId).

%%--------------------------------------------------------------------
%% @doc
%% Checks whether storage exists.
%% @end
%%--------------------------------------------------------------------
-spec exists(od_storage:id()) -> boolean().
exists(Key) ->
    {ok, Exists} = datastore_model:exists(?CTX, Key),
    Exists.

%%--------------------------------------------------------------------
%% @doc
%% Returns list of all records.
%% @end
%%--------------------------------------------------------------------
-spec list() -> {ok, [doc()]} | {error, term()}.
list() ->
    datastore_model:fold(?CTX, fun(Doc, Acc) -> {ok, [Doc | Acc]} end, []).

%%%===================================================================
%%% API functions
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% @equiv new(Name, Helpers, false).
%% @end
%%--------------------------------------------------------------------
-spec new(name(), [helper()]) -> doc().
new(Name, Helpers) ->
    new(Name, Helpers, false, undefined).


%%--------------------------------------------------------------------
%% @doc
%% Constructs storage record.
%% @end
%%--------------------------------------------------------------------
-spec new(name(), [helper()], boolean(), undefined | luma_config:config()) -> doc().
new(Name, Helpers, ReadOnly, LumaConfig) ->
    #document{value = #storage_config{
        name = Name,
        helpers = Helpers,
        readonly = ReadOnly,
        luma_config = LumaConfig
    }}.


%%--------------------------------------------------------------------
%% @doc
%% Returns storage ID.
%% @end
%%--------------------------------------------------------------------
-spec get_id(od_storage:id() | doc()) -> od_storage:id().
get_id(<<_/binary>> = StorageId) ->
    StorageId;
get_id(#document{key = StorageId, value = #storage_config{}}) ->
    StorageId.


%%--------------------------------------------------------------------
%% @doc
%% Returns storage name.
%% @end
%%--------------------------------------------------------------------
-spec get_name(record() | doc()) -> name().
get_name(#storage_config{name = Name}) ->
    Name;
get_name(#document{value = #storage_config{} = Value}) ->
    get_name(Value).


%%--------------------------------------------------------------------
%% @doc
%% Checks whether storage is readonly.
%% @end
%%--------------------------------------------------------------------
-spec is_readonly(record() | doc()) -> boolean().
is_readonly(#storage_config{readonly = ReadOnly}) ->
    ReadOnly;
is_readonly(#document{value = #storage_config{} = Value}) ->
    is_readonly(Value).


%%--------------------------------------------------------------------
%% @doc
%% Checks whether storage is mounted in root.
%% @end
%%--------------------------------------------------------------------
-spec is_mounted_in_root(record() | doc() | od_storage:id()) -> boolean().
is_mounted_in_root(#storage_config{mount_in_root = MiR}) ->
    MiR;
is_mounted_in_root(#document{value = #storage_config{} = Value}) ->
    is_mounted_in_root(Value);
is_mounted_in_root(StorageId) ->
    {ok, #document{value = Value}} = ?MODULE:get(StorageId),
    is_mounted_in_root(Value).


%%--------------------------------------------------------------------
%% @doc
%% Returns list of storage helpers.
%% @end
%%--------------------------------------------------------------------
-spec get_helpers(record() | doc() | od_storage:id()) -> [helper()].
get_helpers(#storage_config{helpers = Helpers}) ->
    Helpers;
get_helpers(#document{value = #storage_config{} = Value}) ->
    get_helpers(Value);
get_helpers(StorageId) ->
    {ok, StorageConfig} = ?MODULE:get(StorageId),
    get_helpers(StorageConfig).


%%-------------------------------------------------------------------
%% @doc
%% Returns map describing luma configuration
%% @end
%%-------------------------------------------------------------------
-spec get_luma_config_map(record() | doc()) -> map().
get_luma_config_map(#storage_config{luma_config = undefined}) ->
    #{enabled => false};
get_luma_config_map(#storage_config{luma_config = #luma_config{url = URL}}) ->
    #{
        enabled => true,
        url => URL
    };
get_luma_config_map(#document{value = Storage}) ->
    get_luma_config_map(Storage).


%%--------------------------------------------------------------------
%% @doc
%% Selects storage helper by its name form the list of configured storage helpers.
%% @end
%%--------------------------------------------------------------------
%% @formatter:off
-spec select_helper
    (record() | doc() | od_storage:id(), [helper:name()]) ->
        {ok, [helper()]} | {error, Reason :: term()};
    (record() | doc() | od_storage:id(), helper:name()) ->
        {ok, helper()} | {error, Reason :: term()}.
%% @formatter:on
select_helper(Storage, HelperNames) when is_list(HelperNames) ->
    Helpers = lists:filter(fun(Helper) ->
        lists:member(helper:get_name(Helper), HelperNames)
    end, get_helpers(Storage)),
    case Helpers of
        [] -> {error, not_found};
        _ -> {ok, Helpers}
    end;

select_helper(Storage, HelperName) ->
    case select_helper(Storage, [HelperName]) of
        {ok, [Helper]} -> {ok, Helper};
        Error -> Error
    end.


-spec update_name(StorageId :: od_storage:id(), NewName :: name()) -> ok.
update_name(StorageId, NewName) ->
    UpdateFun = fun(Storage) -> {ok, Storage#storage_config{name = NewName}} end,
    case update(StorageId, UpdateFun) of
        {ok, _} -> rtransfer_put_storage(StorageId);
        Error -> Error
    end.


%%--------------------------------------------------------------------
%% @doc
%% Updates storage helper arguments.
%% @end
%%--------------------------------------------------------------------
-spec update_helper_args(od_storage:id(), helper:name(), helper:args()) ->
    ok | {error, term()}.
update_helper_args(StorageId, HelperName, Changes) when is_map(Changes) ->
    UpdateFun = fun(Helper) -> helper:update_args(Helper, Changes) end,
    update_helper(StorageId, HelperName, UpdateFun).


%%--------------------------------------------------------------------
%% @doc
%% Updates storage helper admin ctx.
%% @end
%%--------------------------------------------------------------------
-spec update_admin_ctx(od_storage:id(), helper:name(), helper:user_ctx()) ->
    ok | {error, term()}.
update_admin_ctx(StorageId, HelperName, Changes) when is_map(Changes) ->
    UpdateFun = fun(Helper) -> helper:update_admin_ctx(Helper, Changes) end,
    update_helper(StorageId, HelperName, UpdateFun).


%%--------------------------------------------------------------------
%% @doc
%% Updates LUMA configuration of the storage.
%% LUMA cannot be enabled or disabled, only its parameters may be changed.
%% @end
%%--------------------------------------------------------------------
-spec update_luma_config(od_storage:id(), Changes) -> ok | {error, term()}
    when Changes :: #{url => luma_config:url(), api_key => luma_config:api_key()}.
update_luma_config(StorageId, Changes) when is_map(Changes) ->
    ?extract_ok(update(StorageId, fun
        (#storage_config{luma_config = undefined}) ->
            {error, luma_disabled};
        (#storage_config{luma_config = #luma_config{url = Url, api_key = ApiKey}} = Storage) ->
            NewConfig = luma_config:new(
                maps:get(url, Changes, Url),
                maps:get(api_key, Changes, ApiKey)
            ),
            {ok, Storage#storage_config{luma_config = NewConfig}}
    end)).


%%--------------------------------------------------------------------
%% @doc
%% Updates LUMA configuration of the storage.
%% LUMA cannot be enabled or disabled, only its parameters may be changed.
%% @end
%%--------------------------------------------------------------------
-spec set_luma_config(od_storage:id(), LumaConfig :: luma_config:config() | undefined) ->
    ok | {error, term()}.
set_luma_config(StorageId, LumaConfig) ->
    ?extract_ok(update(StorageId, fun(#storage_config{} = Storage) ->
        {ok, Storage#storage_config{luma_config = LumaConfig}}
    end)).


%%--------------------------------------------------------------------
%% @doc
%% Updates storage helper 'insecure' setting.
%% @end
%%--------------------------------------------------------------------
-spec set_insecure(od_storage:id(), helper:name(), Insecure :: boolean()) ->
    ok | {error, term()}.
set_insecure(StorageId, HelperName, Insecure) when is_boolean(Insecure) ->
    UpdateFun = fun(Helper) -> helper:update_insecure(Helper, Insecure) end,
    update_helper(StorageId, HelperName, UpdateFun).


%%--------------------------------------------------------------------
%% @doc
%% Updates storage's 'readonly' setting.
%% @end
%%--------------------------------------------------------------------
-spec set_readonly(StorageId :: od_storage:id(), Readonly :: boolean()) ->
    ok | {error, term()}.
set_readonly(StorageId, Readonly) when is_boolean(Readonly) ->
    ?extract_ok(update(StorageId, fun(#storage_config{} = Storage) ->
        {ok, Storage#storage_config{readonly = Readonly}}
    end)).


%%--------------------------------------------------------------------
%% @doc
%% Sets storage as mount in root.
%% @end
%%--------------------------------------------------------------------
-spec set_mount_in_root(od_storage:id()) -> ok.
set_mount_in_root(StorageId) ->
    ?extract_ok(update(StorageId, fun(#storage_config{} = Storage) ->
        {ok, Storage#storage_config{mount_in_root = true}}
    end)).


%%--------------------------------------------------------------------
%% @doc
%% Removes storage. Fails with an error if the storage supports
%% any space.
%% @end
%%--------------------------------------------------------------------
-spec safe_remove(od_storage:id()) -> ok | {error, storage_in_use | term()}.
safe_remove(StorageId) ->
    critical_section:run({storage_to_space, StorageId}, fun() ->
        case supports_any_space(StorageId) of
            true ->
                {error, storage_in_use};
            false ->
                % TODO VFS-5124 Remove from rtransfer
                storage_logic:delete(StorageId)
        end
    end).


%%--------------------------------------------------------------------
%% @doc
%% Checks if given storage supports any space.
%% @end
%%--------------------------------------------------------------------
-spec supports_any_space(StorageId :: od_storage:id()) -> boolean().
supports_any_space(StorageId) ->
    case provider_logic:get_spaces() of
        {ok, Spaces} ->
            lists:any(fun(SpaceId) ->
                {ok, StorageIds} = space_logic:get_storage_ids(SpaceId),
                lists:member(StorageId, StorageIds)
            end, Spaces);
        ?ERROR_UNREGISTERED_ONEPROVIDER ->
            false
    end.


%%--------------------------------------------------------------------
%% @doc
%% Selects storage by its name from the list of configured storages.
%% @end
%%--------------------------------------------------------------------
-spec select(name()) -> {ok, doc()} | {error, term()}.
select(Name) ->
    case storage_config:list() of
        {ok, Docs} ->
            Docs2 = lists:filter(fun(Doc) ->
                get_name(Doc) =:= Name
            end, Docs),
            case Docs2 of
                [] -> {error, not_found};
                [Doc | _] -> {ok, Doc}
            end
    end.


%%-------------------------------------------------------------------
%% @doc
%% Returns luma_config field for given storage.
%% @end
%%-------------------------------------------------------------------
-spec get_luma_config(record() | doc()) -> undefined | luma_config:config().
get_luma_config(#storage_config{luma_config = LumaConfig}) ->
    LumaConfig;
get_luma_config(#document{value = Storage = #storage_config{}}) ->
    get_luma_config(Storage).


%%-------------------------------------------------------------------
%% @doc
%% Checks whether luma is enabled for given storage.
%% @end
%%-------------------------------------------------------------------
-spec is_luma_enabled(record() | doc()) -> boolean().
is_luma_enabled(#storage_config{luma_config = undefined}) ->
    false;
is_luma_enabled(#storage_config{luma_config = #luma_config{}}) ->
    true;
is_luma_enabled(#document{value = #storage_config{} = Storage}) ->
    is_luma_enabled(Storage).


%%-------------------------------------------------------------------
%% @doc
%% Returns map describing the storage. The data is redacted to
%% remove sensitive information.
%% @end
%%-------------------------------------------------------------------
-spec describe(od_storage:id()) ->
    {ok, #{binary() := binary() | boolean() | undefined}} | {error, term()}.
describe(StorageId) ->
    case ?MODULE:get(StorageId) of
        {ok, #document{value = Storage}} ->
            [Helper | _] = get_helpers(Storage),
            AdminCtx = helper:get_redacted_admin_ctx(Helper),
            HelperArgs = helper:get_args(Helper),
            LumaConfigMap = get_luma_config_map(Storage),
            Base = maps:merge(HelperArgs, AdminCtx),
            {ok, Base#{
                <<"id">> => StorageId,
                <<"name">> => get_name(Storage),
                <<"type">> => helper:get_name(Helper),
                <<"readonly">> => is_readonly(Storage),
                <<"insecure">> => helper:is_insecure(Helper),
                <<"storagePathType">> => helper:get_storage_path_type(Helper),
                <<"lumaEnabled">> => maps:get(enabled, LumaConfigMap, false),
                <<"lumaUrl">> => maps:get(url, LumaConfigMap, undefined)
            }};
        {error, _} = Error -> Error
    end.

-spec on_storage_created(od_storage:id()) -> ok.
on_storage_created(StorageId) ->
    {ok, Doc} = ?MODULE:get(StorageId),
    ok = rtransfer_put_storage(Doc).

-spec delete_all() -> ok.
delete_all() ->
    {ok, Storages} = list(),
    lists:foreach(fun(#document{key = Id, value = #storage_config{}}) ->
        delete(Id)
    end, Storages).

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
    6.


%%--------------------------------------------------------------------
%% @doc
%% Returns model's record structure in provided version.
%% @end
%%--------------------------------------------------------------------
-spec get_record_struct(datastore_model:record_version()) ->
    datastore_model:record_struct().
get_record_struct(1) ->
    {record, [
        {name, binary},
        {helpers, [{record, [
            {name, string},
            {args, #{string => string}}
        ]}]}
    ]};
get_record_struct(2) ->
    {record, [
        {name, string},
        {helpers, [{record, [
            {name, string},
            {args, #{string => string}},
            {admin_ctx, #{string => string}},
            {insecure, boolean}
        ]}]},
        {readonly, boolean}
    ]};
get_record_struct(3) ->
    {record, [
        {name, string},
        {helpers, [{record, [
            {name, string},
            {args, #{string => string}},
            {admin_ctx, #{string => string}},
            {insecure, boolean}
        ]}]},
        {readonly, boolean},
        {luma_config, {record, [
            {url, string},
            {cache_timeout, integer},
            {api_key, string}
        ]}}
    ]};
get_record_struct(4) ->
    {record, [
        {name, string},
        {helpers, [{record, [
            {name, string},
            {args, #{string => string}},
            {admin_ctx, #{string => string}},
            {insecure, boolean},
            {extended_direct_io, boolean}
        ]}]},
        {readonly, boolean},
        {luma_config, {record, [
            {url, string},
            {cache_timeout, integer},
            {api_key, string}
        ]}}
    ]};
get_record_struct(5) ->
    {record, [
        {name, string},
        {helpers, [{record, [
            {name, string},
            {args, #{string => string}},
            {admin_ctx, #{string => string}},
            {insecure, boolean},
            {extended_direct_io, boolean},
            {storage_path_type, string}
        ]}]},
        {readonly, boolean},
        {luma_config, {record, [
            {url, string},
            {api_key, string}
        ]}}
    ]};
get_record_struct(6) ->
    {record, [
        {name, string},
        {helpers, [{record, [
            {name, string},
            {args, #{string => string}},
            {admin_ctx, #{string => string}},
            {insecure, boolean},
            {extended_direct_io, boolean},
            {storage_path_type, string}
        ]}]},
        {readonly, boolean},
        {luma_config, {record, [
            {url, string},
            {api_key, string}
        ]}},
        {mount_in_root, boolean} % new field
    ]}.

%%--------------------------------------------------------------------
%% @doc
%% Upgrades model's record from provided version to the next one.
%% @end
%%--------------------------------------------------------------------
-spec upgrade_record(datastore_model:record_version(), datastore_model:record()) ->
    {datastore_model:record_version(), datastore_model:record()}.
upgrade_record(1, {?MODULE, Name, Helpers}) ->
    {2, #storage_config{
        name = Name,
        helpers = [{
            helper,
            helper:translate_name(HelperName),
            maps:fold(fun(K, V, Args) ->
                maps:put(helper:translate_arg_name(K), V, Args)
            end, #{}, HelperArgs),
            #{},
            false
        } || {_, HelperName, HelperArgs} <- Helpers]
    }};
upgrade_record(2, {?MODULE, Name, Helpers, Readonly}) ->
    {3, #storage_config{
        name = Name,
        helpers = Helpers,
        readonly = Readonly,
        luma_config = undefined
    }};
upgrade_record(3, {?MODULE, Name, Helpers, Readonly, LumaConfig}) ->
    {4, #storage_config{
        name = Name,
        helpers = [{
            helper,
            HelperName,
            HelperArgs,
            AdminCtx,
            Insecure,
            false
        } || {_, HelperName, HelperArgs, AdminCtx, Insecure} <- Helpers],
        readonly = Readonly,
        luma_config = LumaConfig
    }};
upgrade_record(4, {?MODULE, Name, Helpers, Readonly, LumaConfig}) ->
    {5, #storage_config{
        name = Name,
        helpers = [
            #helper{
                name = HelperName,
                args = HelperArgs,
                admin_ctx = AdminCtx,
                insecure = Insecure,
                extended_direct_io = ExtendedDirectIO,
                storage_path_type = ?CANONICAL_STORAGE_PATH
            } || {_, HelperName, HelperArgs, AdminCtx, Insecure,
                ExtendedDirectIO} <- Helpers
        ],
        readonly = Readonly,
        luma_config = LumaConfig
    }};
%% @TODO VFS-5854 Implement upgrade procedure using cluster upgrade
upgrade_record(5, {?MODULE, Name, Helpers, Readonly, LumaConfig}) ->
    {6, #storage_config{
        name = Name,
        helpers = [
            #helper{
                name = HelperName,
                args = HelperArgs,
                admin_ctx = AdminCtx,
                insecure = Insecure,
                extended_direct_io = ExtendedDirectIO,
                storage_path_type = ?CANONICAL_STORAGE_PATH
            } || {_, HelperName, HelperArgs, AdminCtx, Insecure,
                ExtendedDirectIO} <- Helpers
        ],
        readonly = Readonly,
        luma_config = LumaConfig,
        mount_in_root = false
    }}.


%%%===================================================================
%%% Internal functions
%%%===================================================================

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Updates selected storage helper in the datastore.
%% @end
%%--------------------------------------------------------------------
-spec update_helper(StorageId :: od_storage:id(), helper:name(), DiffFun) ->
    ok | {error, term()} when
    DiffFun :: fun((helper()) -> {ok, helper()} | {error, term()}).
update_helper(StorageId, HelperName, DiffFun) ->
    UpdateFun = fun(Storage) ->
        case select_helper(Storage, HelperName) of
            {ok, Helper} ->
                case DiffFun(Helper) of
                    {ok, Helper} ->
                        {error, no_changes};
                    {ok, NewHelper} ->
                        {ok, replace_helper(Storage, HelperName, NewHelper)};
                    {error, _} = Error ->
                        Error
                end;
            {error, _} = Error ->
                Error
        end
    end,
    case update(StorageId, UpdateFun) of
        {ok, _} -> on_helper_changed(StorageId);
        {error, no_changes} -> ok;
        Error -> Error
    end.


%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handles actions necessary after helper params have been changed.
%% @end
%%--------------------------------------------------------------------
-spec on_helper_changed(StorageId :: od_storage:id()) -> ok.
on_helper_changed(StorageId) ->
    {ok, Nodes} = node_manager:get_cluster_nodes(),
    fslogic_event_emitter:emit_helper_params_changed(StorageId),
    rtransfer_put_storage(StorageId),
    rpc:multicall(Nodes, rtransfer_config, restart_link, []),
    helpers_reload:refresh_helpers_by_storage(StorageId).


%%--------------------------------------------------------------------
%% @private
%% @doc
%% Replaces storage helper with given name.
%% @end
%%--------------------------------------------------------------------
-spec replace_helper(record(), helper:name(), NewHelper :: helpers:helper()) ->
    record().
replace_helper(#storage_config{helpers = OldHelpers} = Storage, HelperName, NewHelper) ->
    NewHelpers = lists:keyreplace(HelperName, #helper.name, OldHelpers, NewHelper),
    Storage#storage_config{helpers = NewHelpers}.


%%--------------------------------------------------------------------
%% @private
%% @doc
%% Adds or updates storage in rtransfer config.
%% @end
%%--------------------------------------------------------------------
-spec rtransfer_put_storage(doc() | od_storage:id()) -> ok.
rtransfer_put_storage(#document{} = Doc) ->
    rtransfer_config:add_storage(Doc),
    ok;

rtransfer_put_storage(StorageId) ->
    {ok, Doc} = ?MODULE:get(StorageId),
    rtransfer_put_storage(Doc).
