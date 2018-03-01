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
-export([get_id/1, get_name/1, is_readonly/1, get_helpers/1, is_luma_enabled/1,
    get_luma_config/1, get_luma_config_map/1]).
-export([select_helper/2, update_helper/3, select/1]).
-export([save/1, get/1, exists/1, delete/1, update/2, create/1, list/0]).

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
%% Saves storage.
%% @end
%%--------------------------------------------------------------------
-spec save(doc()) -> {ok, id()} | {error, term()}.
save(Doc) ->
    ?extract_key(datastore_model:save(?CTX, Doc)).

%%--------------------------------------------------------------------
%% @doc
%% Updates storage.
%% @end
%%--------------------------------------------------------------------
-spec update(id(), diff()) -> {ok, id()} | {error, term()}.
update(Key, Diff) ->
    ?extract_key(datastore_model:update(?CTX, Key, Diff)).

%%--------------------------------------------------------------------
%% @doc
%% Creates storage.
%% @end
%%--------------------------------------------------------------------
-spec create(doc()) -> {ok, id()} | {error, term()}.
create(#document{value = #storage{}} = Doc) ->
    ?extract_key(datastore_model:create(?CTX, Doc)).

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
-spec get_helpers(record() | doc()) -> [helper()].
get_helpers(#storage{helpers = Helpers}) ->
    Helpers;
get_helpers(#document{value = #storage{} = Value}) ->
    get_helpers(Value).

%%-------------------------------------------------------------------
%% @doc
%% Returns map describing luma configuration
%% @end
%%-------------------------------------------------------------------
-spec get_luma_config_map(record() | doc()) -> maps:map().
get_luma_config_map(#storage{luma_config = undefined}) ->
    #{enabled => false};
get_luma_config_map(#storage{
    luma_config = #luma_config{
        url = URL,
        cache_timeout = CacheTimeout
}}) ->
    #{
        enabled => true,
        url => URL,
        cache_timeout => CacheTimeout div 60000
    };
get_luma_config_map(#document{value = Storage}) ->
    get_luma_config_map(Storage).

%%--------------------------------------------------------------------
%% @doc
%% Selects storage helper by its name form the list of configured storage helpers.
%% @end
%%--------------------------------------------------------------------
-spec select_helper(record() | doc(), helpers:name()) ->
    {ok, helper()} | {error, Reason :: term()}.
select_helper(Storage, HelperName) ->
    Helpers = lists:filter(fun(Helper) ->
        helper:get_name(Helper) =:= HelperName
    end, get_helpers(Storage)),
    case Helpers of
        [] -> {error, not_found};
        [Helper] -> {ok, Helper}
    end.

%%--------------------------------------------------------------------
%% @doc
%% Updates storage helper arguments.
%% @end
%%--------------------------------------------------------------------
-spec update_helper(storage:id(), helper:name(), helpers:args()) ->
    {ok, id()} | {error, term()}.
update_helper(StorageId, HelperName, NewArgs) ->
    update(StorageId, fun(#storage{helpers = Helpers} = Storage) ->
        case select_helper(Storage, HelperName) of
            {ok, #helper{args = Args} = Helper} ->
                Helper2 = Helper#helper{args = maps:merge(Args, NewArgs)},
                Helpers2 = lists:keyreplace(HelperName, 2, Helpers, Helper2),
                {ok, Storage#storage{helpers = Helpers2}};
            {error, Reason} ->
                {error, Reason}
        end
    end).

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
                [Doc] -> {ok, Doc}
            end
    end.

%%-------------------------------------------------------------------
%% @private
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
%% @private
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
            {cache_timeout, integer},
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
        helpers = [
            #helper{
                name = helper:translate_name(HelperName),
                args = maps:fold(fun(K, V, Args) ->
                    maps:put(helper:translate_arg_name(K), V, Args)
                end, #{}, HelperArgs)
            } || {_, HelperName, HelperArgs} <- Helpers
        ]
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
        helpers = [
            #helper{
                name = HelperName,
                args = HelperArgs,
                admin_ctx = AdminCtx,
                insecure = Insecure,
                extended_direct_io = false
            } || {_, HelperName, HelperArgs, AdminCtx, Insecure} <- Helpers
        ],
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
                extended_direct_io = false,
                storage_path_type = ?CANONICAL_STORAGE_PATH
            } || {_, HelperName, HelperArgs, AdminCtx, Insecure} <- Helpers
        ],
        readonly = Readonly,
        luma_config = LumaConfig
    }}.
