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
-export([new/2, get_id/1, get_name/1, get_helpers/1, select_helper/2,
    update_helper/3, select/1, new/3]).

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
        {helpers, [{record, 1, [
            {name, string},
            {args, #{string => string}}
        ]}]}
    ]};
record_struct(2) ->
    {record, [
        {name, string},
        {helpers, [{record, 1, [
            {name, string},
            {args, #{string => string}},
            {admin_ctx, #{string => string}},
            {insecure, boolean}
        ]}]},
        {readonly, boolean}
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
            #helper{name = Name, args = Args} || {_, Name, Args} <- Helpers
        ]
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
    datastore:save(?STORE_LEVEL, Document).

%%--------------------------------------------------------------------
%% @doc
%% {@link model_behaviour} callback update/2.
%% @end
%%--------------------------------------------------------------------
-spec update(datastore:key(), Diff :: datastore:document_diff()) ->
    {ok, datastore:key()} | datastore:update_error().
update(Key, Diff) ->
    datastore:update(?STORE_LEVEL, ?MODULE, Key, Diff).

%%--------------------------------------------------------------------
%% @doc
%% {@link model_behaviour} callback create/1.
%% @end
%%--------------------------------------------------------------------
-spec create(datastore:document()) ->
    {ok, datastore:key()} | datastore:create_error().
create(#document{value = #storage{name = Name}} = Document) ->
    critical_section:run_on_mnesia([?MODEL_NAME, ?STORAGE_LOCK_ID], fun() ->
        case datastore:fetch_link(?LINK_STORE_LEVEL, ?ROOT_STORAGE, ?MODEL_NAME, Name) of
            {ok, _} ->
                {error, already_exists};
            {error, link_not_found} ->
                datastore:run_transaction(fun() ->
                    datastore:create(?STORE_LEVEL, #document{
                        key = ?ROOT_STORAGE, value = #storage{name = ?ROOT_STORAGE}
                    }),
                    case datastore:create(?STORE_LEVEL, Document) of
                        {error, Reason} ->
                            {error, Reason};
                        {ok, Key} ->
                            ok = datastore:add_links(?LINK_STORE_LEVEL,
                                ?ROOT_STORAGE, ?MODEL_NAME, {Name, {Key, ?MODEL_NAME}}),
                            {ok, Key}
                    end
                end)
        end
    end).

%%--------------------------------------------------------------------
%% @doc
%% {@link model_behaviour} callback get/1.
%% @end
%%--------------------------------------------------------------------
-spec get(datastore:key()) -> {ok, datastore:document()} | datastore:get_error().
get(Key) ->
    datastore:get(?STORE_LEVEL, ?MODULE, Key).

%%--------------------------------------------------------------------
%% @doc
%% {@link model_behaviour} callback delete/1.
%% @end
%%--------------------------------------------------------------------
-spec delete(datastore:key()) -> ok | datastore:generic_error().
delete(Key) ->
    datastore:delete(?STORE_LEVEL, ?MODULE, Key).

%%--------------------------------------------------------------------
%% @doc
%% {@link model_behaviour} callback exists/1.
%% @end
%%--------------------------------------------------------------------
-spec exists(datastore:key()) -> datastore:exists_return().
exists(Key) ->
    ?RESPONSE(datastore:exists(?STORE_LEVEL, ?MODULE, Key)).

%%--------------------------------------------------------------------
%% @doc
%% {@link model_behaviour} callback model_init/0.
%% @end
%%--------------------------------------------------------------------
-spec model_init() -> model_behaviour:model_config().
model_init() ->
    Config = ?MODEL_CONFIG(system_config_bucket, [], ?GLOBALLY_CACHED_LEVEL),
    Config#model_config{version = 2}.

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
    datastore:foreach_link(?LINK_STORE_LEVEL, ?ROOT_STORAGE, ?MODEL_NAME,
        fun(_LinkName, {_V, [{_, _, Key, storage}]}, AccIn) ->
            {ok, Doc} = storage:get(Key),
            [Doc | AccIn]
        end, []).

%%%===================================================================
%%% API functions
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Constructs storage record.
%% @end
%%--------------------------------------------------------------------
-spec new(name(), [helper()]) -> doc().
new(Name, Helpers) ->
    #document{value = #storage{name = Name, helpers = Helpers}}.

%%--------------------------------------------------------------------
%% @doc
%% Constructs storage record.
%% @end
%%--------------------------------------------------------------------
-spec new(name(), [helper()], boolean()) -> doc().
new(Name, Helpers, ReadOnly) ->
    #document{value = #storage{name = Name, helpers = Helpers, readonly=ReadOnly}}.


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
%% Returns list of storage helpers.
%% @end
%%--------------------------------------------------------------------
-spec get_helpers(model() | doc()) -> [helper()].
get_helpers(#storage{helpers = Helpers}) ->
    Helpers;
get_helpers(#document{value = #storage{} = Value}) ->
    get_helpers(Value).

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