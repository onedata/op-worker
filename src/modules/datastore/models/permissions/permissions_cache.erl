%%%-------------------------------------------------------------------
%%% @author Michal Wrzeszcz
%%% @copyright (C) 2016 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Model used for caching permissions to files (used by check_permissions module).
%%% @end
%%%-------------------------------------------------------------------
-module(permissions_cache).
-author("Michal Wrzeszcz").
-behaviour(model_behaviour).

-include("modules/datastore/datastore_specific_models_def.hrl").
-include_lib("ctool/include/logging.hrl").
-include_lib("cluster_worker/include/modules/datastore/datastore_model.hrl").
-include_lib("cluster_worker/include/elements/task_manager/task_manager.hrl").

%% model_behaviour callbacks
-export([save/1, get/1, exists/1, delete/1, update/2, create/1, create_or_update/2,
    list/0, model_init/0, 'after'/5, before/4]).

%% API
-export([check_permission/1, cache_permission/2, invalidate_permissions_cache/0, invalidate_permissions_cache/2,
    check_remote_invalitation/4]).

%% Key of document that keeps information about whole cache status.
-define(STATUS_UUID, <<"status">>).

%%%===================================================================
%%% model_behaviour callbacks
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% {@link model_behaviour} callback save/1.
%% @end
%%--------------------------------------------------------------------
-spec save(datastore:document()) -> {ok, datastore:ext_key()} | datastore:generic_error().
save(Document) ->
    datastore:save(?STORE_LEVEL, Document).

%%--------------------------------------------------------------------
%% @doc
%% {@link model_behaviour} callback update/2.
%% @end
%%--------------------------------------------------------------------
-spec update(datastore:ext_key(), Diff :: datastore:document_diff()) ->
    {ok, datastore:ext_key()} | datastore:update_error().
update(Key, Diff) ->
    datastore:update(?STORE_LEVEL, ?MODULE, Key, Diff).

%%--------------------------------------------------------------------
%% @doc
%% {@link model_behaviour} callback create/1.
%% @end
%%--------------------------------------------------------------------
-spec create(datastore:document()) -> {ok, datastore:ext_key()} | datastore:create_error().
create(Document) ->
    datastore:create(?STORE_LEVEL, Document).

%%--------------------------------------------------------------------
%% @doc
%% Updates document with using ID from document. If such object does not exist,
%% it initialises the object with the document.
%% @end
%%--------------------------------------------------------------------
-spec create_or_update(datastore:document(), Diff :: datastore:document_diff()) ->
    {ok, datastore:ext_key()} | datastore:update_error().
create_or_update(Doc, Diff) ->
    datastore:create_or_update(?STORE_LEVEL, Doc, Diff).

%%--------------------------------------------------------------------
%% @doc
%% {@link model_behaviour} callback get/1.
%% @end
%%--------------------------------------------------------------------
-spec get(datastore:ext_key()) -> {ok, datastore:document()} | datastore:get_error().
get(Key) ->
    datastore:get(?STORE_LEVEL, ?MODULE, Key).

%%--------------------------------------------------------------------
%% @doc
%% {@link model_behaviour} callback delete/1.
%% @end
%%--------------------------------------------------------------------
-spec delete(datastore:ext_key()) -> ok | datastore:generic_error().
delete(Key) ->
    datastore:delete(?STORE_LEVEL, ?MODULE, Key).

%%--------------------------------------------------------------------
%% @doc
%% {@link model_behaviour} callback exists/1.
%% @end
%%--------------------------------------------------------------------
-spec exists(datastore:ext_key()) -> datastore:exists_return().
exists(Key) ->
    ?RESPONSE(datastore:exists(?STORE_LEVEL, ?MODULE, Key)).

%%--------------------------------------------------------------------
%% @doc
%% Returns list of all records.
%% @end
%%--------------------------------------------------------------------
-spec list() -> {ok, [datastore:ext_key()]} | datastore:generic_error() | no_return().
list() ->
    Filter = fun
                ('$end_of_table', Acc) ->
                    {abort, Acc};
                (#document{key = ?STATUS_UUID}, Acc) ->
                     {next, Acc};
                (#document{key = Uuid}, Acc) ->
                    {next, [Uuid | Acc]}
            end,
    datastore:list(?STORE_LEVEL, ?MODULE, Filter, []).

%%--------------------------------------------------------------------
%% @doc
%% {@link model_behaviour} callback model_init/0.
%% @end
%%--------------------------------------------------------------------
-spec model_init() -> model_behaviour:model_config().
model_init() ->
    ?MODEL_CONFIG(permissions_cache_bucket, [], ?GLOBAL_ONLY_LEVEL).

%%--------------------------------------------------------------------
%% @doc
%% {@link model_behaviour} callback 'after'/5.
%% @end
%%--------------------------------------------------------------------
-spec 'after'(ModelName :: model_behaviour:model_type(), Method :: model_behaviour:model_action(),
    Level :: datastore:store_level(), Context :: term(),
    ReturnValue :: term()) -> ok.
'after'(_ModelName, _Method, _Level, _Context, _ReturnValue) ->
    ok.

%%--------------------------------------------------------------------
%% @doc
%% {@link model_behaviour} callback before/4.
%% @end
%%--------------------------------------------------------------------
-spec before(ModelName :: model_behaviour:model_type(), Method :: model_behaviour:model_action(),
    Level :: datastore:store_level(), Context :: term()) -> ok | datastore:generic_error().
before(_ModelName, _Method, _Level, _Context) ->
    ok.

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Checks permission in cache.
%% @end
%%--------------------------------------------------------------------
-spec check_permission(Rule :: term()) -> {ok, term()} | calculate | no_return().
check_permission(Rule) ->
    case get(?STATUS_UUID) of
        {ok, #document{value = #permissions_cache{value = {clearing, _}}}} ->
            calculate;
        {ok, #document{value = #permissions_cache{value = {Model, _}}}} ->
            get_rule(Model, Rule);
        {error, {not_found, _}} ->
            get_rule(?MODULE, Rule)
    end.

%%--------------------------------------------------------------------
%% @doc
%% Saves permission in cache.
%% @end
%%--------------------------------------------------------------------
-spec cache_permission(Rule :: term(), Value :: term()) ->
    ok | {ok, datastore:ext_key()} | datastore:generic_error().
cache_permission(Rule, Value) ->
    CurrentModel = case get(?STATUS_UUID) of
                       {ok, #document{value = #permissions_cache{value = {Model, _}}}} ->
                           Model;
                       {error, {not_found, _}} ->
                           ?MODULE
                   end,

    case CurrentModel of
        clearing ->
            ok;
        permissions_cache ->
            save(#document{key = get_uuid(Rule), value =
            #permissions_cache{value = Value}});
        permissions_cache_helper ->
            permissions_cache_helper:save(#document{key = get_uuid(Rule), value =
            #permissions_cache_helper{value = Value}})
    end.

invalidate_permissions_cache(Model, Key) ->
    invalidate_permissions_cache(),

    MC = Model:model_init(),
    Driver = datastore:driver_to_module(datastore:level_to_driver(?DISK_ONLY_LEVEL)),
    {Rev, Document} = case MC#model_config.store_level of
        ?DISK_ONLY_LEVEL ->
            {ok, Doc} = erlang:apply(Driver, get, [MC, Key]),
            {rev_to_number(Doc#document.rev), Doc};
        _ ->
            A1 = erlang:apply(Driver, get, [MC, Key]),
            Driver2 = datastore:driver_to_module(datastore:level_to_driver(
                caches_controller:cache_to_datastore_level(Model))),
            A2 = erlang:apply(Driver2, get, [MC, Key]),
            case {A1, A2} of
                {{ok, Doc}, {ok, Doc2}} ->
                    case Doc2#document.value =:= Doc#document.value of
                        true ->
                            {rev_to_number(Doc#document.rev), Doc};
                        _ ->
                            {rev_to_number(Doc#document.rev) + 1, Doc}
                    end;
                {{ok, Doc}, _} ->
                    {rev_to_number(Doc#document.rev), Doc};
                {_, {ok, Doc}} ->
                    {0, Doc}
            end
    end,

    case dbsync_worker:has_sync_context(Document) of
        true ->
            SpaceId = dbsync_worker:get_space_id(Document),
            {ok, _} = change_propagation_controller:save_info(Model, Key, Rev, SpaceId, ?MODEL_NAME, check_remote_invalitation),
            ok;
        _ ->
            ok
    end.

check_remote_invalitation(Model, Key, Rev, SpaceId) ->
    ok = file_consistency:wait(Key, SpaceId, [{rev, Model, Rev}],
        {?MODULE, check_remote_invalitation, [Model, Key, Rev, SpaceId]}),
    invalidate_permissions_cache().

% TODO - move to driver
%%--------------------------------------------------------------------
%% @private
%% @doc
%%
%% @end
%%--------------------------------------------------------------------
-spec rev_to_number(binary()) -> non_neg_integer().
rev_to_number(Rev) ->
    [Num, _ID] = binary:split(Rev, <<"-">>),
    binary_to_integer(Num).

%%--------------------------------------------------------------------
%% @doc
%% Clears all permissions from cache.
%% @end
%%--------------------------------------------------------------------
-spec invalidate_permissions_cache() -> ok.
invalidate_permissions_cache() ->
%%    dla dokumentow cachowanych
%%    pobrac z dysku aktualna rewizje dokumentu i jego wartosc (w sekcji krytycznej obejmujacej zrzut na dysk w cache controllerze)
%%    jesli wartosc na dysku zgadza sie z wartoscia w pamieci to uzywac numeru rewizji z dysku, jesli nie o 1 wiekszego
%%    dla dokumentow disk only - pobrac rewizje z dysku i jej uzywac
%%    zapisac w dokumencie ze cache uniewazniamy dla takiej rewizji i takiego klucza oraz info ze ten provider juz uniewaznil
%%    jak przyjdzie taki doc z dbsync to zapisac sie file_confistency na dany dokument - jak przyjdzie to dopisujemy sie do tych co uniewaznili
%%    kazde dopisanie (w tym pierwsze) skutkuje sprawdzeniem ilu jest providerow w space - jesli tylu ilu uniewaznilo to kasujemy dokument
%%    (jesli jest tylko 1 to nigdy nie zapisujemy)


    CurrentModel = case get(?STATUS_UUID) of
        {ok, #document{value = #permissions_cache{value = {Model, _}}}} ->
            Model;
        {error, {not_found, _}} ->
            ?MODULE
    end,

    case CurrentModel of
        clearing ->
            ok;
        _ ->
            case start_clearing(CurrentModel) of
                {ok, _} ->
                    task_manager:start_task(fun() ->
                        {ok, Uuids} = erlang:apply(CurrentModel, list, []),
                        lists:foreach(fun(Uuid) ->
                            ok = erlang:apply(CurrentModel, delete, [Uuid])
                        end, Uuids),
                        ok = stop_clearing(CurrentModel),
                        ok
                    end, ?CLUSTER_LEVEL);
                {error, parallel_cleaning} ->
                    ok
            end
    end.


%%%===================================================================
%%% Internal functions
%%%===================================================================

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Gets rule's uuid.
%% @end
%%--------------------------------------------------------------------
-spec get_uuid(Rule :: term()) -> binary().
get_uuid(Rule) ->
    base64:encode(term_to_binary(Rule)).

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Gets rule value from cache.
%% @end
%%--------------------------------------------------------------------
-spec get_rule(Model :: atom(), Rule :: term()) -> {ok, term()} | calculate | no_return().
get_rule(Model, Rule) ->
    case erlang:apply(Model, get, [get_uuid(Rule)]) of
        {ok, #document{value = #permissions_cache{value = V}}} ->
            {ok, V};
        {ok, #document{value = #permissions_cache_helper{value = V}}} ->
            {ok, V};
        {error, {not_found, _}} ->
            calculate
    end.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Saves information about clearing start.
%% @end
%%--------------------------------------------------------------------
-spec start_clearing(CurrentModel :: atom()) -> {ok, datastore:ext_key()} | datastore:update_error().
start_clearing(CurrentModel) ->
    NewDoc = #document{key = ?STATUS_UUID, value = #permissions_cache{value = {permissions_cache_helper, clearing}}},
    UpdateFun = fun
                    (#permissions_cache{value = {S1, clearing}}) ->
                        case S1 of
                            CurrentModel ->
                                {ok, #permissions_cache{value = {clearing, clearing}}};
                            _ ->
                                {error, parallel_cleaning}
                        end;
                    (#permissions_cache{value = {S1, Helper}}) ->
                        case S1 of
                            CurrentModel ->
                                {ok, #permissions_cache{value = {Helper, clearing}}};
                            _ ->
                                {error, parallel_cleaning}
                        end
                end,

    create_or_update(NewDoc, UpdateFun).

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Saves information about clearing stop.
%% @end
%%--------------------------------------------------------------------
-spec stop_clearing(CurrentModel :: atom()) -> ok | no_return().
stop_clearing(CurrentModel) ->
    UpdateFun = fun
                    (#permissions_cache{value = {clearing, clearing}}) ->
                        {ok, #permissions_cache{value = {CurrentModel, clearing}}};
                    (#permissions_cache{value = {S1, clearing}}) ->
                        case S1 of
                            CurrentModel ->
                                {error, already_cleared};
                            _ ->
                                {ok, #permissions_cache{value = {S1, CurrentModel}}}
                        end;
                    (#permissions_cache{value = {clearing, S2}}) ->
                        case S2 of
                            CurrentModel ->
                                {ok, #permissions_cache{value = {CurrentModel, clearing}}};
                            _ ->
                                {ok, #permissions_cache{value = {CurrentModel, S2}}}
                        end;
                    (_) ->
                        {error, already_cleared}
                end,
    case update(?STATUS_UUID, UpdateFun) of
        {ok, _} -> ok;
        {error, already_cleared} -> ok
    end.