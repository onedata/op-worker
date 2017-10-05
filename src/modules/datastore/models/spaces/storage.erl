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
-behaviour(model_behaviour).

-include("modules/storage_file_manager/helpers/helpers.hrl").
-include("modules/datastore/datastore_specific_models_def.hrl").
-include_lib("cluster_worker/include/modules/datastore/datastore_model.hrl").

%% ID of root storage which links to all registered storage to simplify list/1 operation
-define(ROOT_STORAGE, <<"root_storage">>).

%% Resource ID used to sync all operations on this model
-define(STORAGE_LOCK_ID, <<"storage_res_id">>).

%% API
-export([new/2, new/4]).
-export([get_id/1, get_name/1, is_readonly/1, get_helpers/1, is_luma_enabled/1,
    get_luma_config/1, get_luma_config_map/1]).
-export([select_helper/2, update_helper/3, select/1]).

%% model_behaviour callbacks
-export([save/1, get/1, exists/1, delete/1, update/2, create/1, model_init/0,
    'after'/5, before/4, list/0]).
-export([record_struct/1, record_upgrade/2]).

-type id() :: datastore:key().
-type model() :: #storage{}.
-type doc() :: #document{value :: model()}.
-type name() :: binary().
-type helper() :: helpers:helper().

-export_type([id/0, model/0, doc/0, name/0, helper/0]).

%%--------------------------------------------------------------------
%% @doc
%% Returns structure of the record in specified version.
%% @end
%%--------------------------------------------------------------------
-spec record_struct(datastore_json:record_version()) ->
    datastore_json:record_struct().
record_struct(1) ->
    {record, [
        {name, binary},
        {helpers, [{record, [
            {name, string},
            {args, #{string => string}}
        ]}]}
    ]};
record_struct(2) ->
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
record_struct(3) ->
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
    ]}.

%%--------------------------------------------------------------------
%% @doc
%% Upgrades record from specified version.
%% @end
%%--------------------------------------------------------------------
-spec record_upgrade(datastore_json:record_version(), tuple()) ->
    {datastore_json:record_version(), tuple()}.
record_upgrade(1, {?MODEL_NAME, Name, Helpers}) ->
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
record_upgrade(2, {?MODEL_NAME, Name, Helpers, Readonly}) ->
    {3, #storage{
        name = Name,
        helpers = Helpers,
        readonly = Readonly,
        luma_config = undefined
    }}.


%%%===================================================================
%%% model_behaviour callbacks
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% {@link model_behaviour} callback save/1.
%% @end
%%--------------------------------------------------------------------
-spec save(datastore:document()) ->
    {ok, datastore:key()} | datastore:generic_error().
save(Document) ->
    model:execute_with_default_context(?MODULE, save, [Document]).

%%--------------------------------------------------------------------
%% @doc
%% {@link model_behaviour} callback update/2.
%% @end
%%--------------------------------------------------------------------
-spec update(datastore:key(), Diff :: datastore:document_diff()) ->
    {ok, datastore:key()} | datastore:update_error().
update(Key, Diff) ->
    model:execute_with_default_context(?MODULE, update, [Key, Diff]).

%%--------------------------------------------------------------------
%% @doc
%% {@link model_behaviour} callback create/1.
%% @end
%%--------------------------------------------------------------------
-spec create(datastore:document()) ->
    {ok, datastore:key()} | datastore:create_error().
create(#document{value = #storage{name = Name}} = Document) ->
    critical_section:run([?MODEL_NAME, ?STORAGE_LOCK_ID], fun() ->
        case model:execute_with_default_context(?MODULE, fetch_link, [?ROOT_STORAGE, Name]) of
            {ok, _} ->
                {error, already_exists};
            {error, link_not_found} ->
                model:execute_with_default_context(?MODULE, create,
                    [#document{
                        key = ?ROOT_STORAGE, value = #storage{name = ?ROOT_STORAGE}
                    }]),
                case model:execute_with_default_context(?MODULE, create, [Document]) of
                    {error, Reason} ->
                        {error, Reason};
                    {ok, Key} ->
                        ok = model:execute_with_default_context(?MODULE,
                            add_links, [?ROOT_STORAGE, {Name, {Key, ?MODEL_NAME}}]),
                        {ok, Key}
                end
        end
    end).

%%--------------------------------------------------------------------
%% @doc
%% {@link model_behaviour} callback get/1.
%% @end
%%--------------------------------------------------------------------
-spec get(datastore:key()) -> {ok, datastore:document()} | datastore:get_error().
get(Key) ->
    model:execute_with_default_context(?MODULE, get, [Key]).

%%--------------------------------------------------------------------
%% @doc
%% {@link model_behaviour} callback delete/1.
%% @end
%%--------------------------------------------------------------------
-spec delete(datastore:key()) -> ok | datastore:generic_error().
delete(Key) ->
    model:execute_with_default_context(?MODULE, delete, [Key]).

%%--------------------------------------------------------------------
%% @doc
%% {@link model_behaviour} callback exists/1.
%% @end
%%--------------------------------------------------------------------
-spec exists(datastore:key()) -> datastore:exists_return().
exists(Key) ->
    ?RESPONSE(model:execute_with_default_context(?MODULE, exists, [Key])).

%%--------------------------------------------------------------------
%% @doc
%% {@link model_behaviour} callback model_init/0.
%% @end
%%--------------------------------------------------------------------
-spec model_init() -> model_behaviour:model_config().
model_init() ->
    Config = ?MODEL_CONFIG(system_config_bucket, [], ?GLOBALLY_CACHED_LEVEL),
    Config#model_config{version = 3}.

%%--------------------------------------------------------------------
%% @doc
%% {@link model_behaviour} callback 'after'/5.
%% @end
%%--------------------------------------------------------------------
-spec 'after'(model_behaviour:model_type(), model_behaviour:model_action(),
    datastore:store_level(), Context :: term(), ReturnValue :: term()) -> ok.
'after'(_ModelName, _Method, _Level, _Context, _ReturnValue) ->
    ok.

%%--------------------------------------------------------------------
%% @doc
%% {@link model_behaviour} callback before/4.
%% @end
%%--------------------------------------------------------------------
-spec before(model_behaviour:model_type(), model_behaviour:model_action(),
    datastore:store_level(), Context :: term()) -> ok | datastore:generic_error().
before(_ModelName, _Method, _Level, _Context) ->
    ok.

%%--------------------------------------------------------------------
%% @doc
%% Returns list of all records.
%% @end
%%--------------------------------------------------------------------
-spec list() -> {ok, [datastore:document()]} | datastore:generic_error() | no_return().
list() ->
    model:execute_with_default_context(?MODULE, foreach_link, [?ROOT_STORAGE,
        fun(_LinkName, {_V, [{_, _, Key, storage}]}, AccIn) ->
            {ok, Doc} = storage:get(Key),
            [Doc | AccIn]
        end, []]).

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
-spec get_name(model() | doc()) -> name().
get_name(#storage{name = Name}) ->
    Name;
get_name(#document{value = #storage{} = Value}) ->
    get_name(Value).

%%--------------------------------------------------------------------
%% @doc
%% Checks whether storage is readonly.
%% @end
%%--------------------------------------------------------------------
-spec is_readonly(model() | doc()) -> boolean().
is_readonly(#storage{readonly = ReadOnly}) ->
    ReadOnly;
is_readonly(#document{value = #storage{} = Value}) ->
    is_readonly(Value).

%%--------------------------------------------------------------------
%% @doc
%% Returns list of storage helpers.
%% @end
%%--------------------------------------------------------------------
-spec get_helpers(model() | doc()) -> [helper()].
get_helpers(#storage{helpers = Helpers}) ->
    Helpers;
get_helpers(#document{value = #storage{} = Value}) ->
    get_helpers(Value).

%%-------------------------------------------------------------------
%% @doc
%% Returns map describing luma configuration
%% @end
%%-------------------------------------------------------------------
-spec get_luma_config_map(model() | doc()) -> maps:map().
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
-spec select_helper(model() | doc(), helpers:name()) ->
    {ok, helper()} | {error, Reason :: term()}.
select_helper(Storage, HelperName) ->
    Helpers = lists:filter(fun(Helper) ->
        helper:get_name(Helper) =:= HelperName
    end, get_helpers(Storage)),
    case Helpers of
        [] -> {error, {not_found, helper}};
        [Helper] -> {ok, Helper}
    end.

%%--------------------------------------------------------------------
%% @doc
%% Updates storage helper arguments.
%% @end
%%--------------------------------------------------------------------
-spec update_helper(storage:id(), helper:name(), helpers:args()) ->
    ok | datastore:update_error().
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
%% Selects storage by its name form the list of configured storages.
%% @end
%%--------------------------------------------------------------------
-spec select(name()) -> {ok, doc()} | datastore:get_error().
select(Name) ->
    case storage:list() of
        {ok, Docs} ->
            Docs2 = lists:filter(fun(Doc) ->
                get_name(Doc) =:= Name
            end, Docs),
            case Docs2 of
                [] -> {error, {not_found, ?MODULE}};
                [Doc] -> {ok, Doc}
            end
    end.

%%-------------------------------------------------------------------
%% @private
%% @doc
%% Returns luma_config field for given storage.
%% @end
%%-------------------------------------------------------------------
-spec get_luma_config(model() | doc()) -> undefined | luma_config:config().
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
-spec is_luma_enabled(model() | doc()) -> boolean().
is_luma_enabled(#storage{luma_config = undefined}) ->
    false;
is_luma_enabled(#storage{luma_config = #luma_config{}}) ->
    true;
is_luma_enabled(#document{value = #storage{} = Storage})->
    is_luma_enabled(Storage).