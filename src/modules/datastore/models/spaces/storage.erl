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
-module(storage).
-author("Rafal Slota").

-include("modules/datastore/datastore_models.hrl").
-include("modules/datastore/datastore_runner.hrl").
-include("modules/storage_file_manager/helpers/helpers.hrl").

%% API
-export([new/2, new/4]).
-export([get_id/1, get_name/1, is_readonly/1, get_helpers/1, get_luma_config_map/1]).
-export([select_helper/2, select/1]).
-export([get/1, exists/1, delete/1, update/2, create/1, list/0]).
-export([supports_any_space/1]).

%% Exports for onepanel RPC
-export([update_name/2, update_helper_args/3, update_admin_ctx/3,
    update_luma_config/2, set_luma_config/2, set_insecure/3,
    set_readonly/2, safe_remove/1]).
-export([get_luma_config/1, is_luma_enabled/1]).

%% datastore_model callbacks
-export([get_ctx/0]).
-export([get_record_version/0, get_record_struct/1, upgrade_record/2]).

-type id() :: datastore:key().
-type record() :: #storage{}.
-type doc() :: datastore_doc:doc(record()).
-type diff() :: datastore_doc:diff(record()).
-type name() :: binary().
-type helper() :: helpers:helper().

-export_type([id/0, record/0, doc/0, name/0, helper/0]).

-define(CTX, #{
    model => ?MODULE,
    fold_enabled => true
}).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Updates storage.
%% @end
%%--------------------------------------------------------------------
-spec update(id(), diff()) -> {ok, doc()} | {error, term()}.
update(Key, Diff) ->
    datastore_model:update(?CTX, Key, Diff).

%%--------------------------------------------------------------------
%% @doc
%% Creates storage.
%% @end
%%--------------------------------------------------------------------
-spec create(doc()) -> {ok, id()} | {error, term()}.
create(#document{value = #storage{}} = Doc) ->
    case ?extract_key(datastore_model:create(?CTX, Doc)) of
        {ok, StorageId} ->
            rtransfer_put_storage(Doc#document{key = StorageId}),
            {ok, StorageId};
        Other ->
            Other
    end.

%%--------------------------------------------------------------------
%% @doc
%% Returns storage.
%% @end
%%--------------------------------------------------------------------
-spec get(id()) -> {ok, doc()} | {error, term()}.
get(Key) ->
    datastore_model:get(?CTX, Key).

%%--------------------------------------------------------------------
%% @doc
%% Deletes storage.
%% @end
%%--------------------------------------------------------------------
-spec delete(id()) -> ok | {error, term()}.
delete(Key) ->
    datastore_model:delete(?CTX, Key).

%%--------------------------------------------------------------------
%% @doc
%% Checks whether storage exists.
%% @end
%%--------------------------------------------------------------------
-spec exists(id()) -> boolean().
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
    #document{value = #storage{
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
-spec get_id(id() | doc()) -> id().
get_id(<<_/binary>> = StorageId) ->
    StorageId;
get_id(#document{key = StorageId, value = #storage{}}) ->
    StorageId.


%%--------------------------------------------------------------------
%% @doc
%% Returns storage name.
%% @end
%%--------------------------------------------------------------------
-spec get_name(record() | doc()) -> name().
get_name(#storage{name = Name}) ->
    Name;
get_name(#document{value = #storage{} = Value}) ->
    get_name(Value).


%%--------------------------------------------------------------------
%% @doc
%% Checks whether storage is readonly.
%% @end
%%--------------------------------------------------------------------
-spec is_readonly(record() | doc()) -> boolean().
is_readonly(#storage{readonly = ReadOnly}) ->
    ReadOnly;
is_readonly(#document{value = #storage{} = Value}) ->
    is_readonly(Value).


%%--------------------------------------------------------------------
%% @doc
%% Returns list of storage helpers.
%% @end
%%--------------------------------------------------------------------
-spec get_helpers(record() | doc() | id()) -> [helper()].
get_helpers(#storage{helpers = Helpers}) ->
    Helpers;
get_helpers(#document{value = #storage{} = Value}) ->
    get_helpers(Value);
get_helpers(StorageId) ->
    {ok, StorageDoc} = ?MODULE:get(StorageId),
    get_helpers(StorageDoc).


%%-------------------------------------------------------------------
%% @doc
%% Returns map describing luma configuration
%% @end
%%-------------------------------------------------------------------
-spec get_luma_config_map(record() | doc()) -> maps:map().
get_luma_config_map(#storage{luma_config = undefined}) ->
    #{enabled => false};
get_luma_config_map(#storage{luma_config = #luma_config{url = URL}}) ->
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
-spec select_helper(record() | doc(), helpers:name() | [helpers:name()]) ->
    {ok, helper() | [helper()]} | {error, Reason :: term()}.
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


%%--------------------------------------------------------------------
%% @doc
%% Replaces storage with a new one of the same name.
%% @end
%%--------------------------------------------------------------------
-spec replace_helper(record(), helper:name(), NewHelper :: helpers:helper()) ->
    record().
replace_helper(#storage{helpers = OldHelpers} = Storage, HelperName, NewHelper) ->
    NewHelpers = lists:keyreplace(HelperName, #helper.name, OldHelpers, NewHelper),
    Storage#storage{helpers = NewHelpers}.


-spec update_name(StorageId :: id(), NewName :: name()) -> ok.
update_name(StorageId, NewName) ->
    Result = update(StorageId, fun(#storage{} = Storage) ->
        {ok, Storage#storage{name = NewName}}
    end),
    case Result of
        {ok, _} -> rtransfer_put_storage(StorageId);
        Error -> Error
    end.


%%--------------------------------------------------------------------
%% @doc
%% Updates storage helper arguments.
%% @end
%%--------------------------------------------------------------------
-spec update_helper_args(storage:id(), helper:name(), helper:args()) ->
    ok | {error, term()}.
update_helper_args(StorageId, HelperName, Changes) when is_map(Changes) ->
    Updater = fun(Helper) -> helper:update_args(Helper, Changes) end,
    case update_helper(StorageId, HelperName, Updater) of
        ok -> rtransfer_put_storage(StorageId);
        Error -> Error
    end.


%%--------------------------------------------------------------------
%% @doc
%% Updates storage helper admin ctx.
%% @end
%%--------------------------------------------------------------------
-spec update_admin_ctx(storage:id(), helper:name(), helper:user_ctx()) ->
    ok | {error, term()}.
update_admin_ctx(StorageId, HelperName, Changes) when is_map(Changes) ->
    Updater = fun(Helper) -> helper:update_admin_ctx(Helper, Changes) end,
    update_helper(StorageId, HelperName, Updater).


%%--------------------------------------------------------------------
%% @doc
%% Updates LUMA configuration of the storage.
%% LUMA cannot be enabled or disabled, only its parameters may be changed.
%% @end
%%--------------------------------------------------------------------
-spec update_luma_config(storage:id(), Changes) -> ok | {error, term()}
    when Changes :: #{url => luma_config:url(), api_key => luma_config:api_key()}.
update_luma_config(StorageId, Changes) when is_map(Changes) ->
    ?extract_ok(update(StorageId, fun
        (#storage{luma_config = undefined}) ->
            {error, luma_disabled};
        (#storage{luma_config = #luma_config{url = Url, api_key = ApiKey}} = Storage) ->
            NewConfig = luma_config:new(
                maps:get(url, Changes, Url),
                maps:get(api_key, Changes, ApiKey)
            ),
            {ok, Storage#storage{luma_config = NewConfig}}
    end)).


%%--------------------------------------------------------------------
%% @doc
%% Updates LUMA configuration of the storage.
%% LUMA cannot be enabled or disabled, only its parameters may be changed.
%% @end
%%--------------------------------------------------------------------
-spec set_luma_config(storage:id(), LumaConfig :: luma_config:config() | undefined) ->
    ok | {error, term()}.
set_luma_config(StorageId, LumaConfig) ->
    ?extract_ok(update(StorageId, fun(#storage{} = Storage) ->
        {ok, Storage#storage{luma_config = LumaConfig}}
    end)).


%%--------------------------------------------------------------------
%% @doc
%% Updates storage helper 'insecure' setting.
%% @end
%%--------------------------------------------------------------------
-spec set_insecure(storage:id(), helper:name(), Insecure :: boolean()) ->
    ok | {error, term()}.
set_insecure(StorageId, HelperName, Insecure) when is_boolean(Insecure) ->
    update_helper(StorageId, HelperName, fun(Helper) ->
        helper:update_insecure(Helper, Insecure)
    end).


%%--------------------------------------------------------------------
%% @doc
%% Updates storage's 'readonly' setting.
%% @end
%%--------------------------------------------------------------------
-spec set_readonly(StorageId :: storage:id(), Readonly :: boolean()) ->
    ok | {error, term()}.
set_readonly(StorageId, Readonly) when is_boolean(Readonly) ->
    ?extract_ok(update(StorageId, fun(#storage{} = Storage) ->
        {ok, Storage#storage{readonly = Readonly}}
    end)).


%%--------------------------------------------------------------------
%% @doc
%% Removes storage. Fails with an error if the storage supports
%% any space.
%% @end
%%--------------------------------------------------------------------
-spec safe_remove(id()) -> ok | {error, storage_in_use | term()}.
safe_remove(StorageId) ->
    critical_section:run({storage_to_space, StorageId}, fun() ->
        case supports_any_space(StorageId) of
            true ->
                {error, storage_in_use};
            false ->
                % TODO VFS-5124 Remove from rtransfer
                delete(StorageId)
        end
    end).


%%--------------------------------------------------------------------
%% @doc
%% Checks if given storage supports any space.
%% @end
%%--------------------------------------------------------------------
-spec supports_any_space(StorageId :: id()) -> boolean().
supports_any_space(StorageId) ->
    {ok, Spaces} = provider_logic:get_spaces(),
    lists:any(fun(SpaceId) ->
        lists:member(StorageId, space_storage:get_storage_ids(SpaceId))
    end, Spaces).


%%--------------------------------------------------------------------
%% @doc
%% Selects storage by its name from the list of configured storages.
%% @end
%%--------------------------------------------------------------------
-spec select(name()) -> {ok, doc()} | {error, term()}.
select(Name) ->
    case storage:list() of
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
get_luma_config(#storage{luma_config = LumaConfig}) ->
    LumaConfig;
get_luma_config(#document{value = Storage = #storage{}}) ->
    get_luma_config(Storage).


%%-------------------------------------------------------------------
%% @doc
%% Checks whether luma is enabled for given storage.
%% @end
%%-------------------------------------------------------------------
-spec is_luma_enabled(record() | doc()) -> boolean().
is_luma_enabled(#storage{luma_config = undefined}) ->
    false;
is_luma_enabled(#storage{luma_config = #luma_config{}}) ->
    true;
is_luma_enabled(#document{value = #storage{} = Storage}) ->
    is_luma_enabled(Storage);
is_luma_enabled(StorageId) when is_binary(StorageId) ->
    {ok, #document{value = #storage{} = Storage}} = ?MODULE:get(StorageId),
    is_luma_enabled(Storage).


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
    5.


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
    ]}.

%%--------------------------------------------------------------------
%% @doc
%% Upgrades model's record from provided version to the next one.
%% @end
%%--------------------------------------------------------------------
-spec upgrade_record(datastore_model:record_version(), datastore_model:record()) ->
    {datastore_model:record_version(), datastore_model:record()}.
upgrade_record(1, {?MODULE, Name, Helpers}) ->
    {2, #storage{
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
    {3, #storage{
        name = Name,
        helpers = Helpers,
        readonly = Readonly,
        luma_config = undefined
    }};
upgrade_record(3, {?MODULE, Name, Helpers, Readonly, LumaConfig}) ->
    {4, #storage{
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
    {5, #storage{
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
    }}.


%%%===================================================================
%%% Internal functons
%%%===================================================================

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Updates selected storage helper in the datastore.
%% @end
%%--------------------------------------------------------------------
-spec update_helper(StorageId :: storage:id(), helper:name(), DiffFun) ->
    ok | {error, term()} when
    DiffFun :: fun((helper()) -> {ok, helper()} | {error, term()}).
update_helper(StorageId, HelperName, DiffFun) ->
    ?extract_ok(update(StorageId, fun(Storage) ->
        case select_helper(Storage, HelperName) of
            {ok, Helper} ->
                HelperName = helper:get_name(Helper),
                case DiffFun(Helper) of
                    {ok, NewHelper} ->
                        {ok, replace_helper(Storage, HelperName, NewHelper)};
                    {error, _} = Error ->
                        Error
                end;
            {error, _} = Error -> Error
        end
    end)).


%%--------------------------------------------------------------------
%% @private
%% @doc
%% Adds or updates storage in rtransfer config.
%% @end
%%--------------------------------------------------------------------
-spec rtransfer_put_storage(doc() | id()) -> ok.
rtransfer_put_storage(#document{} = Doc) ->
    rtransfer_config:add_storage(Doc),
    ok;

rtransfer_put_storage(StorageId) ->
    {ok, Doc} = ?MODULE:get(StorageId),
    rtransfer_put_storage(Doc).
