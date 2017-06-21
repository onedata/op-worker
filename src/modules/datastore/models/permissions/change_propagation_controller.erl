%%%-------------------------------------------------------------------
%%% @author Michal Wrzeszcz
%%% @copyright (C) 2016 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Model used to control propagation of changes that require action after
%%% particular revision is replicated.
%%% @end
%%%-------------------------------------------------------------------
-module(change_propagation_controller).
-author("Michal Wrzeszcz").
-behaviour(model_behaviour).

-include("modules/datastore/datastore_specific_models_def.hrl").
-include_lib("cluster_worker/include/modules/datastore/datastore_model.hrl").

%% model_behaviour callbacks
-export([save/1, get/1, exists/1, delete/1, update/2, create/1, create_or_update/2,
    list/0, model_init/0, 'after'/5, before/4]).
-export([record_struct/1]).
%% export API
-export([save_change/6, mark_change_propagated/1, verify_propagation/3]).

-define(SET_LINK_SCOPE(ScopeID), [{scope, ScopeID}]).

%%--------------------------------------------------------------------
%% @doc
%% Returns structure of the record in specified version.
%% @end
%%--------------------------------------------------------------------
-spec record_struct(datastore_json:record_version()) -> datastore_json:record_struct().
record_struct(1) ->
    {record, [
        {change_revision, integer},
        {space_id, string},
        {verify_module, atom},
        {verify_function, atom}
    ]}.

%%%===================================================================
%%% model_behaviour callbacks
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% {@link model_behaviour} callback save/1.
%% @end
%%--------------------------------------------------------------------
-spec save(datastore:document()) ->
    {ok, datastore:ext_key()} | datastore:generic_error().
save(Document) ->
    model:execute_with_default_context(?MODULE, save, [Document]).

%%--------------------------------------------------------------------
%% @doc
%% {@link model_behaviour} callback update/2.
%% @end
%%--------------------------------------------------------------------
-spec update(datastore:ext_key(), Diff :: datastore:document_diff()) ->
    {ok, datastore:ext_key()} | datastore:update_error().
update(Key, Diff) ->
    model:execute_with_default_context(?MODULE, update, [Key, Diff]).

%%--------------------------------------------------------------------
%% @doc
%% {@link model_behaviour} callback create/1.
%% @end
%%--------------------------------------------------------------------
-spec create(datastore:document()) ->
    {ok, datastore:ext_key()} | datastore:create_error().
create(Document) ->
    model:execute_with_default_context(?MODULE, create, [Document]).

%%--------------------------------------------------------------------
%% @doc
%% Updates document with using ID from document. If such object does not exist,
%% it initialises the object with the document.
%% @end
%%--------------------------------------------------------------------
-spec create_or_update(datastore:document(), Diff :: datastore:document_diff()) ->
    {ok, datastore:ext_key()} | datastore:generic_error().
create_or_update(Doc, Diff) ->
    model:execute_with_default_context(?MODULE, create_or_update, [Doc, Diff]).

%%--------------------------------------------------------------------
%% @doc
%% {@link model_behaviour} callback get/1.
%% @end
%%--------------------------------------------------------------------
-spec get(datastore:ext_key()) -> {ok, datastore:document()} | datastore:get_error().
get(Key) ->
    model:execute_with_default_context(?MODULE, get, [Key]).

%%--------------------------------------------------------------------
%% @doc
%% {@link model_behaviour} callback delete/1.
%% @end
%%--------------------------------------------------------------------
-spec delete(datastore:ext_key()) -> ok | datastore:generic_error().
delete(Key) ->
    model:execute_with_default_context(?MODULE, delete, [Key]).

%%--------------------------------------------------------------------
%% @doc
%% {@link model_behaviour} callback delete/1.
%% @end
%%--------------------------------------------------------------------
-spec delete(datastore:ext_key(), datastore:delete_predicate(), Options :: [datastore:option()]) ->
    ok | datastore:generic_error().
delete(Key, Pred, Opts) ->
    model:execute_with_default_context(?MODULE, delete, [Key, Pred, Opts]).

%%--------------------------------------------------------------------
%% @doc
%% {@link model_behaviour} callback exists/1.
%% @end
%%--------------------------------------------------------------------
-spec exists(datastore:ext_key()) -> datastore:exists_return().
exists(Key) ->
    ?RESPONSE(model:execute_with_default_context(?MODULE, exists, [Key])).

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
                 (#document{key = Uuid}, Acc) ->
                     {next, [Uuid | Acc]}
             end,
    model:execute_with_default_context(?MODULE, list, [Filter, []]).

%%--------------------------------------------------------------------
%% @doc
%% {@link model_behaviour} callback model_init/0.
%% @end
%%--------------------------------------------------------------------
-spec model_init() -> model_behaviour:model_config().
model_init() ->
    ?MODEL_CONFIG(change_propagation_controller_bucket, [], ?GLOBALLY_CACHED_LEVEL, ?GLOBALLY_CACHED_LEVEL,
        true, false, oneprovider:get_provider_id())#model_config{sync_enabled = true}.

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
%% Saves information about change to be propagated.
%% @end
%%--------------------------------------------------------------------
-spec save_change(Model :: model_behaviour:model_type(), Key :: datastore:ext_key(), Rev :: non_neg_integer(),
    SpaceId :: space_info:id(), VefifyModule :: atom(), VerifyFun :: atom()) -> ok | no_return().
save_change(Model, Key, Rev, SpaceId, VefifyModule, VerifyFun) ->
    Providers = dbsync_utils:get_providers_for_space(SpaceId),
    MyId = oneprovider:get_provider_id(),
    case Providers of
        [MyId] ->
            ok;
        _ ->
            Doc = #document{key = get_key(Model, Key),
                value = #change_propagation_controller{change_revision = Rev, space_id = SpaceId,
                    verify_module = VefifyModule, verify_function = VerifyFun}, scope = SpaceId},
            {ok, _Uuid} = save(Doc),
            ok = model:execute_with_default_context(?MODULE, add_links,
                [Doc, {MyId, Doc}], ?SET_LINK_SCOPE(SpaceId)),
            ok
    end.

%%--------------------------------------------------------------------
%% @doc
%% Saves information that change propagated and activates action connected with change.
%% @end
%%--------------------------------------------------------------------
-spec mark_change_propagated(datastore:document()) -> ok | no_return().
mark_change_propagated(#document{key = ControllerKey, value = #change_propagation_controller{space_id = SpaceId,
    change_revision = Rev, verify_module = VM, verify_function = VF}}) ->
    ok = verify_propagation(ControllerKey, SpaceId, true),
    {Model, Uuid} = decode_key(ControllerKey),
    ok = apply(VM, VF, [Model, Uuid, Rev, SpaceId]).

%%--------------------------------------------------------------------
%% @doc
%% Verifies if change was propagated to all providers.
%% @end
%%--------------------------------------------------------------------
-spec verify_propagation(ControllerKey :: datastore:ext_key(), SpaceId :: space_info:id(), AddLocal :: boolean()) ->
    ok | no_return().
verify_propagation(ControllerKey, SpaceId, AddLocal) ->
    MyId = oneprovider:get_provider_id(),
    ListFun = fun(LinkName, _LinkTarget, Acc) ->
        [LinkName | Acc]
    end,

    critical_section:run({?MODULE, verify_propagation}, fun() ->
        {ok, Links} = model:execute_with_default_context(?MODULE, foreach_link, [ControllerKey, ListFun, []]),
        LocalListed = lists:member(MyId, Links),
        Correction = case ((not LocalListed) and AddLocal) of
            true ->
                1;
            _ ->
                0
        end,

        Providers = dbsync_utils:get_providers_for_space(SpaceId),
        ToDel = (length(Links) + Correction) >= length(Providers),
        case {ToDel, LocalListed, AddLocal} of
            {true, true, _} ->
                ok = model:execute_with_default_context(?MODULE, delete_links,
                    [ControllerKey, Links], ?SET_LINK_SCOPE(SpaceId)),
                ok = delete(ControllerKey, ?PRED_ALWAYS, [ignore_links]);
            {true, _, _} ->
                ok = delete(ControllerKey, ?PRED_ALWAYS, [ignore_links]);
            {_, _, true} ->
                ok = model:execute_with_default_context(?MODULE, add_links,
                    [ControllerKey, {MyId, {ControllerKey, ?MODULE}}],
                    ?SET_LINK_SCOPE(SpaceId));
            {_, _, _} ->
                ok
        end
    end).

%%%===================================================================
%%% Internal functions
%%%===================================================================

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Gets key of document that describes change.
%% @end
%%--------------------------------------------------------------------
-spec get_key(Model :: model_behaviour:model_type(), Uuid :: datastore:ext_key()) -> binary().
get_key(Model, Uuid) ->
    base64:encode(term_to_binary({Model, Uuid})).

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Gets change description from key.
%% @end
%%--------------------------------------------------------------------
-spec decode_key(Key :: binary()) -> {Model :: model_behaviour:model_type(), Uuid :: datastore:ext_key()}.
decode_key(Key) ->
    binary_to_term(base64:decode(Key)).
